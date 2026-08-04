// Test-only helpers for the server API handler tests.
// `Full<Bytes>` bodies use `Infallible` as their error type, but the handlers
// are generic with `hyper::Error: From<B::Error>` (satisfied by the real
// `hyper::body::Incoming`). This wrapper presents a body with
// `Error = hyper::Error` so tests exercise the same path as the server.
#![cfg(test)]

use bytes::Bytes;
use http_body_util::Full;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct TestBody(pub Full<Bytes>);

impl TestBody {
    pub fn empty() -> Self {
        TestBody(Full::new(Bytes::new()))
    }

    pub fn from_bytes(bytes: Bytes) -> Self {
        TestBody(Full::new(bytes))
    }
}

impl hyper::body::Body for TestBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.get_mut().0)
            .poll_frame(cx)
            .map(|opt| opt.map(|res| res.map_err(|e: Infallible| match e {})))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.0.size_hint()
    }
}
