use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;
use tarpc::{ClientMessage, Response};
use futures::{prelude::*};
use paho_mqtt::{AsyncReceiver, DeliveryToken, Message, MessageBuilder, Properties, PropertyCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use log::warn;
use tarpc::context::Context;

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

#[derive(Debug, Clone, Default)]
pub struct ServerContext {
    pub response_topic: String,
    pub correlation: Vec<u8>
}


impl<Req> ServerTransport<Req> {
    pub async fn new<T: Into<String>>(mut client: paho_mqtt::AsyncClient, request_topic: T) -> ServerTransport<Req> {
        let request_topic = request_topic.into();
        let stream = client.get_stream(25);

        let rt = request_topic.clone();
        client.set_connected_callback(move |cli| {
            let sys = actix::System::new();
            log::error!("Re-Subscribe to {}", rt);

            sys.block_on(async {
                actix::System::current().arbiter().spawn(cli.subscribe(rt.clone(), 1).map(|_| ()));
            });

            log::error!("Re-Subscribe to {} end", rt);
        });

        log::error!("Subscribe to {}", request_topic);
        client.subscribe(request_topic.clone(), 1).await.unwrap();

        ServerTransport { client, request_topic: request_topic, stream, delivery_token: None, phantom: PhantomData::default() }
    }
}



impl<Req, Res> Sink<Response<Res>> for ServerTransport<Req> where Res: Debug + Serialize {
    type Error = crate::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        let pinref = &mut self.delivery_token;

        match pinref {
            None => Poll::Ready(Ok(())),
            Some(f) => Pin::new(f).poll(cx).map_err(Into::into)
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Response<Res>) -> Result<(), Self::Error> {
        let response = item;
        let context = response.context.extensions.get::<MqttTransportExtension>();

        log::info!("Sending response {:?}", response);
        log::info!("response context =  {:?}", context);
        log::info!("context::current {:?}", Context::current());
        log::info!("context::current {:?}", Context::current().extensions.get::<MqttTransportExtension>());

        let context = context.unwrap();


        let data = serde_json::to_vec(&response)?;

        let mut props = Properties::new();
        props.push_binary(PropertyCode::CorrelationData, context.correlation_data.clone())?;

        let msg = MessageBuilder::new().payload(data).topic(&context.response_topic).qos(1).properties(props).finalize();

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

#[derive(Debug, Clone)]
pub struct MqttTransportExtension {
    response_topic: String,
    correlation_data: Vec<u8>
}


impl<Req> ServerTransport<Req> where Req: DeserializeOwned + Debug {
    fn decode_mqtt_message(msg: &Message) -> Result<ClientMessage<Req>, crate::Error> {
        let mut m: ClientMessage<Req> = serde_json::from_slice(msg.payload()).map_err(|err| paho_mqtt::Error::GeneralString(format!("Malformed MQTT Message {:?}. Error: {:?}", String::from_utf8_lossy(msg.payload()), err)))?;

        log::info!("Got Client Message {:?}", m);


        let response_topic = msg.properties().get_string(PropertyCode::ResponseTopic).ok_or(paho_mqtt::Error::General("Response topic property not found"))?;
        let correlation = msg.properties().get_binary(PropertyCode::CorrelationData).ok_or(paho_mqtt::Error::General("CorrelationData property not found"))?;

        let request_id = match m {
            ClientMessage::Request(ref mut r) => &mut r.request_id,
            ClientMessage::Cancel {ref mut request_id, ..} => request_id,
            _ => unimplemented!()
        };
        *request_id = u64::from_str_radix(&sha256::digest(format!("{}/{}", response_topic, request_id))[0..16], 16).expect("Sha256 to return a hexadecimal string");


        log::info!("Transformed Client Message RequestId {:?}", m);


        if let ClientMessage::Request(ref mut r) = m {
            r.context.extensions.insert(MqttTransportExtension { response_topic, correlation_data: correlation });
        }

        Ok(m)
    }
}



impl<Req> Stream for ServerTransport<Req> where Req: DeserializeOwned + Debug {
    type Item = Result<ClientMessage<Req>, crate::Error>;

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
                Err(e) => warn!("ServerTransport: Error decoding MQTT Message: {:?}", e)
            }
        }
    }
}
