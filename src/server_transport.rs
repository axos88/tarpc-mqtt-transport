use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;
use std::time::Instant;
use tarpc::{context, trace, ClientMessage, Response};
use futures::{prelude::*};
use paho_mqtt::{AsyncReceiver, DeliveryToken, Message, MessageBuilder, Properties, PropertyCode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tarpc::context::{ExtractContext};
use crate::util::{ClientMessageMapper, ResponseMapper};

#[pin_project]
pub struct ServerTransport<Req> {
    #[pin]
    client: paho_mqtt::AsyncClient,
    #[pin]
    stream: AsyncReceiver<Option<Message>>,
    #[pin]
    delivery_token: Option<DeliveryToken>,

    request_topic: String,
    phantom: PhantomData<Req>
}

#[derive(Debug, Clone)]
pub struct MqttServerContext {
    pub deadline: Instant,
    pub trace_context: trace::Context,

    pub foo: String,
    pub response_topic: String,
    pub correlation: Vec<u8>,
}

impl ExtractContext<context::Context> for MqttServerContext {
    fn extract(&self) -> context::Context {
        context::Context {
            deadline: self.deadline,
            trace_context: self.trace_context.clone(),
        }
    }

    fn update(&mut self, other: context::Context) {
        self.deadline = other.deadline;
        self.trace_context = other.trace_context
    }
}


impl<Req> ServerTransport<Req> {
    pub async fn new<T: Into<String>>(mut client: paho_mqtt::AsyncClient, request_topic: T) -> ServerTransport<Req> {
        let request_topic = request_topic.into();
        let stream = client.get_stream(25);

        let rt = request_topic.clone();

        client.set_connected_callback(move |cli| {
            let cli = cli.clone();
            let rt2 = rt.clone();
            let start = std::time::Instant::now();

            tokio::spawn(async move {
                log::error!("Re-Subscribe to {} start", rt2);
                cli.subscribe(rt2.clone(), 1).map(|_| ()).await;
                log::warn!("Re-Subscribe to {} finished. Waited {} us", rt2, start.elapsed().as_micros());
            });
        });

        if client.is_connected() {
            log::warn!("Client already connected - subscribe to {}", request_topic);
            client.subscribe(request_topic.clone(), 1).await.unwrap();
        }

        ServerTransport { client, request_topic: request_topic, stream, delivery_token: None, phantom: PhantomData::default() }
    }
}



impl<Req, Res> Sink<Response<MqttServerContext, Res>> for ServerTransport<Req> where Res: Debug + Serialize {
    type Error = crate::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        let pinref = &mut self.delivery_token;

        match pinref {
            None => Poll::Ready(Ok(())),
            Some(f) => Pin::new(f).poll(cx).map_err(Into::into)
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Response<MqttServerContext, Res>) -> Result<(), Self::Error> {
        let response = item;

        log::info!("Sending response {:?}", response);

        let mut props = Properties::new();
        props.push_binary(PropertyCode::CorrelationData, response.context.correlation.clone())?;
        let msg = MessageBuilder::new().topic(&response.context.response_topic).qos(1).properties(props);

        let response: Response<context::Context, _> = response.map_context(|ctx| ctx.extract());
        log::info!("response context =  {:?}", response.context);

        let data = serde_json::to_vec(&response)?;
        let msg = msg.payload(data).finalize();
        let delivery_token = (&mut self.client).publish(msg);
        self.delivery_token = Some(delivery_token);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        let pinref = &mut self.delivery_token;

        match pinref {
            None => Poll::Ready(Ok(())),
            Some(f) => {
                match Pin::new(f).poll(cx) {
                    Poll::Ready(r) => {
                        self.delivery_token.take();
                        Poll::Ready(r).map_err(Into::into)
                    },
                    Poll::Pending => Poll::Pending
                }
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }
}

impl<Req> ServerTransport<Req> where Req: DeserializeOwned + Debug {
    fn decode_mqtt_message(msg: &Message) -> Result<ClientMessage<MqttServerContext, Req>, crate::Error> {
        let m: ClientMessage<context::Context, Req> = serde_json::from_slice(msg.payload()).map_err(|err| paho_mqtt::Error::GeneralString(format!("Malformed MQTT Message {:?}. Error: {:?}", String::from_utf8_lossy(msg.payload()), err)))?;
        let response_topic = msg.properties().get_string(PropertyCode::ResponseTopic).ok_or(paho_mqtt::Error::General("Response topic property not found"))?;
        let correlation = msg.properties().get_binary(PropertyCode::CorrelationData).ok_or(paho_mqtt::Error::General("CorrelationData property not found"))?;

        log::info!("Got Client Message {:?}", m);

        let mut m = m.map_context(|ctx| MqttServerContext {
            deadline: ctx.deadline,
            trace_context: ctx.trace_context,
            foo: "foo".to_string(),
            response_topic: response_topic.clone(),
            correlation: correlation.clone()
        });

        log::info!("Got Client Message {:?}", m);

        let request_id = match m {
            ClientMessage::Request(ref mut r) => &mut r.id,
            ClientMessage::Cancel {ref mut request_id, ..} => request_id,
            _ => unimplemented!()
        };
        *request_id = u64::from_str_radix(&sha256::digest(format!("{}/{}", response_topic, request_id))[0..16], 16).expect("Sha256 to return a hexadecimal string");

        log::info!("Transformed Client Message RequestId {:?}", m);

        Ok(m)
    }
}



impl<Req> Stream for ServerTransport<Req> where Req: DeserializeOwned + Debug {
    type Item = Result<ClientMessage<MqttServerContext, Req>, crate::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            let msg = match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(None) => break Poll::Ready(None),
                Poll::Ready(Some(None)) => continue, // Mqtt Disconnecting
                Poll::Ready(Some(Some(msg))) => msg,
                Poll::Pending => break Poll::Pending
            };

            match ServerTransport::decode_mqtt_message(&msg) {
                Ok(m) => break Poll::Ready(Some(Ok(m))),
                Err(e) => log::warn!("ServerTransport: Error decoding MQTT Message: {:?}", e)
            }
        }
    }
}
