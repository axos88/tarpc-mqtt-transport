use std::io::Write;
use std::error::Error;
use std::string::ToString;
use std::time::{Duration, Instant};
use env_logger::Builder;
use futures::StreamExt;
use log::{info, warn};
use paho_mqtt::{Property, PropertyCode, SslOptions};
use tarpc::{client};
use tarpc::context::{DefaultContext};
use tarpc::server::{BaseChannel, Channel};
use crate::simple_context::ServerContext;
use crate::simple_service::{HelloServer, World, WorldClient};

mod simple_context;
mod simple_service;


const REQUEST_TOPIC: &str = "/tarpc-mqtt-example-requests";
const RESPONSE_TOPIC: &str = "/tarpc-mqtt-example-response";



async fn create_client() -> WorldClient::<DefaultContext> {
  let transport = tarpc_mqtt_transport::ClientTransport::new(build_mqtt_client().await, REQUEST_TOPIC, RESPONSE_TOPIC);
  let client = WorldClient::new(client::Config::default(), transport);

  client.spawn()
}

async fn start_server() {
  let transport = tarpc_mqtt_transport::ServerTransport::new(build_mqtt_client().await, REQUEST_TOPIC).await;

  tokio::spawn(
    BaseChannel::<_, _, _, _, ServerContext>::with_defaults(transport)
      .execute(HelloServer.serve())
      .for_each(|f|
        async { tokio::spawn(f).await.unwrap()}
      )
  );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  Builder::new()
    .format(|buf, record| {
      // Get a rough timestamp (seconds since UNIX epoch)
      let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
      let secs = ts.as_secs();
      let millis = ts.subsec_millis();

      // Use buf.write_str / write! from std::fmt
      Ok(writeln!(
        buf,
        "[{}.{:03}] [{}] {}:{} {}",
        secs,
        millis,
        record.level(),
        record.file().unwrap_or("?"),
        record.line().unwrap_or(0),
        record.args()
      ).unwrap())
    })
    .filter(None, log::LevelFilter::Warn)
    .init();

  start_server().await;
  let client = create_client().await;

  let mut context = DefaultContext::current();


  let resp = client.hello(&mut context, "abc".to_string()).await;
  warn!("Response = {:?}, deadline = {:?}", resp, context.deadline.duration_since(Instant::now()));
  let resp = client.hello(&mut context, "def".to_string()).await;
  warn!("Response = {:?}, deadline = {:?}", resp, context.deadline.duration_since(Instant::now()));
  let resp = client.hello(&mut context, "ghi".to_string()).await;
  warn!("Response = {:?}, deadline = {:?}", resp, context.deadline.duration_since(Instant::now()));

  Ok(())
}


async fn build_mqtt_client() -> paho_mqtt::AsyncClient {
  let host = "broker.hivemq.com";
  let nanos = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap()
    .as_nanos();

  let clientid = format!("tarpc-mqtt-transport-{:x}", nanos);

  let cli = paho_mqtt::CreateOptionsBuilder::new()
    .persistence(None)
    .allow_disconnected_send_at_anytime(true)
    .delete_oldest_messages(true)
    .send_while_disconnected(true)
    .client_id(clientid.as_str())
    .create_client()
    .expect("to be able to create a client");

  let mut conn_prop = paho_mqtt::Properties::new();
  conn_prop
    .push(Property::new(PropertyCode::SessionExpiryInterval, 20).unwrap())
    .unwrap();

  let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
    .keep_alive_interval(Duration::from_secs(15))
    .server_uris(&[format!("ssl://{}", host)])
    .ssl_options(SslOptions::new())
    .clean_start(true)
    .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(120))
    .properties(conn_prop)
    .finalize();

  cli.set_connected_callback(|cli| println!("MQTT client {} connected", cli.client_id()));
  cli.set_disconnected_callback(|cli, p, r| println!("MQTT client {} disconnected: {:?}, {:?}", cli.client_id(), p, r));
  cli.set_connection_lost_callback(|cli| println!("MQTT client {} lost connection", cli.client_id()));

  cli.connect(conn_opts).await.unwrap();

  cli
}
