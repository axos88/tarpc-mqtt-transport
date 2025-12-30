use std::time::{Duration, Instant};
use crate::simple_context::ServerContext;

#[derive(Clone)]
pub struct HelloServer;

#[tarpc::service]
pub trait World {
  async fn hello(name: String) -> String;
}

impl World for HelloServer {
  type Context = ServerContext;
  async fn hello(self, ctx: &mut Self::Context, name: String) -> String {
    println!("Server sees deadline in... {:?}", ctx.shared.deadline.duration_since(Instant::now()));
    ctx.shared.deadline = ctx.shared.deadline.checked_add(Duration::from_secs(10)).unwrap();
    format!("Hello, {name}!")
  }
}
