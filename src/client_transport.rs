use std::fmt::{Debug};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, SystemTime};
use tarpc::{ClientMessage, Response};
use futures::{prelude::*};
use paho_mqtt::{AsyncReceiver, DeliveryToken, Message, MessageBuilder, Properties, PropertyCode, Token};
use serde::Serialize;
use serde::de::DeserializeOwned;
use log::warn;

#[pin_project]
pub struct ClientTransport<Res> {
    #[pin]
    client: paho_mqtt::AsyncClient,
    #[pin]
    stream: AsyncReceiver<Option<Message>>,
    #[pin]
    delivery_token: Option<DeliveryToken>,
    #[pin]
    disconnect_token: Option<Token>,

    request_topic: String,
    response_topic: String,

    phantom: PhantomData<Res>
}

impl<Res> ClientTransport<Res> {
    pub fn new<T: Into<String>, U: Into<String>>(mut client: paho_mqtt::AsyncClient, request_topic: T, response_topic: U) -> ClientTransport<Res> {
        let request_topic = request_topic.into();
        let response_topic = response_topic.into();
        let stream = client.get_stream(25);

        let rt = response_topic.clone();
        client.set_connected_callback(move |cli| {
            tokio::spawn(cli.subscribe(rt.clone(), 1));
        });

        client.subscribe(response_topic.clone(), 1);

        ClientTransport { client, stream, request_topic, response_topic, delivery_token: None, disconnect_token: None, phantom: PhantomData::default() }
    }
}



impl<Req, Res> Sink<ClientMessage<Req>> for ClientTransport<Res> where Req: Debug + Serialize {
    type Error = crate::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        let pinref = &mut self.delivery_token;

        match pinref {
            None => Poll::Ready(Ok(())),
            Some(f) => Pin::new(f).poll(cx).map_err(Into::into)
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: ClientMessage<Req>) -> Result<(), Self::Error> {
        let data = serde_json::to_vec(&item)?;

        let mut props = Properties::new();

        let rid = match item {
            ClientMessage::Request(ref r) => r.id,
            ClientMessage::Cancel { request_id, .. } => request_id,
            _ => unreachable!()
        };

        let deadline = match item {
            ClientMessage::Request(ref r) => Some(r.context.deadline),
            _ => None
        }.and_then(|deadline| deadline.duration_since(SystemTime::now()).ok()).as_ref().map(Duration::as_secs);

        props.push_binary(PropertyCode::CorrelationData, rid.to_le_bytes())?;
        props.push_string(PropertyCode::ResponseTopic, &self.response_topic)?;
        deadline.map_or(Ok(()), |d| props.push_int(PropertyCode::MessageExpiryInterval, (d+1) as i32))?;

        let msg = MessageBuilder::new().payload(data).topic(&self.request_topic).qos(1).properties(props).finalize();

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
                        Poll::Ready(r.map_err(Into::into))
                    },
                    Poll::Pending => Poll::Pending
                }
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        let disconnect_token = match self.disconnect_token.as_mut() {
            Some(token) => token,
            None => {
                let token = self.client.disconnect(None);
                self.disconnect_token.insert(token)
            }
        };

        Pin::new(disconnect_token).poll(cx).map_ok(|_| ()).map_err(Into::into)
    }
}

impl<Res> Stream for ClientTransport<Res> where Res: Debug + DeserializeOwned {
    type Item = Result<Response<Res>, crate::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            let e : Result<!, crate::Error> = match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(None) => break Poll::Ready(None),
                Poll::Ready(Some(None)) => continue, // Mqtt Disconnecting
                Poll::Ready(Some(Some(msg))) => try {
                    break Poll::Ready(Some(Ok(serde_json::from_slice(msg.payload())?)))
                },
                Poll::Pending => break Poll::Pending
            };

            warn!("ClientTransport: Dropping malformed MQTT Message {:?}", e);
        }


    }
}
