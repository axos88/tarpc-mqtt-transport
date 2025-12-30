use std::time::Instant;
use tarpc::context::{DefaultContext, ExtractContext, SharedContext};
use tarpc::trace::Context;
use tarpc_mqtt_transport::MqttServerContextData;

#[derive(Debug, Clone)]
pub struct ServerContext {
  pub shared: DefaultContext,
  pub mqtt: MqttServerContextData,
}

impl SharedContext for ServerContext {
  fn deadline(&self) -> Instant {
    self.shared.deadline
  }

  fn trace_context(&self) -> Context {
    self.shared.trace_context
  }

  fn set_trace_context(&mut self, trace_context: Context) {
    self.shared.trace_context = trace_context;
  }
}

impl ExtractContext<DefaultContext> for ServerContext {
  fn extract(&self) -> DefaultContext {
    self.shared.clone()
  }
}

impl ExtractContext<MqttServerContextData> for ServerContext {
  fn extract(&self) -> MqttServerContextData {
    self.mqtt.clone()
  }
}

impl From<(DefaultContext, MqttServerContextData)> for ServerContext {
  fn from((shared, mqtt): (DefaultContext, MqttServerContextData)) -> Self {
    ServerContext { shared, mqtt }
  }
}
