use tarpc::{ClientMessage, Request, Response};

pub trait ResponseMapper<Ctx, T> {
  fn map_context<Ctx2, F>(self, f: F) -> Response<Ctx2, T>
  where
    F: FnOnce(Ctx) -> Ctx2;
}

impl<Ctx, T> ResponseMapper<Ctx, T> for Response<Ctx, T> {
  fn map_context<Ctx2, F>(self, f: F) -> Response<Ctx2, T>
  where
    F: FnOnce(Ctx) -> Ctx2,
  {
    Response {
      request_id: self.request_id,
      context: f(self.context),
      message: self.message,
    }
  }
}

pub trait ClientMessageMapper<Ctx, Req> {
  fn map_context<Ctx2, F>(self, f: F) -> ClientMessage<Ctx2, Req>
  where
    F: FnOnce(Ctx) -> Ctx2;
}

impl<Ctx, Req> ClientMessageMapper<Ctx, Req> for ClientMessage<Ctx, Req> {
  fn map_context<Ctx2, F>(self, f: F) -> ClientMessage<Ctx2, Req>
  where
    F: FnOnce(Ctx) -> Ctx2,
  {
    match self {
      ClientMessage::Request(Request {
                               context,
                               id,
                               message,
                             }) => ClientMessage::Request(Request {
        context: f(context),
        id,
        message,
      }),
      ClientMessage::Cancel {
        trace_context,
        request_id,
      } => ClientMessage::Cancel {
        trace_context,
        request_id,
      },
      _ => unimplemented!(),
    }
  }
}


