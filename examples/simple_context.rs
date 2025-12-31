use std::time::Instant;
use tarpc::context::{DefaultContext, ExtractContext, SharedContext};
use tarpc::trace::Context;
use tarpc_mqtt_transport::MqttContext;

#[derive(Debug, Clone)]
pub struct ServerContext {
  pub shared: DefaultContext,
  pub mqtt: MqttContext,
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

impl ExtractContext<MqttContext> for ServerContext {
  fn extract(&self) -> MqttContext {
    self.mqtt.clone()
  }
}

impl From<(DefaultContext, MqttContext)> for ServerContext {
  fn from((shared, mqtt): (DefaultContext, MqttContext)) -> Self {
    ServerContext { shared, mqtt }
  }
}
