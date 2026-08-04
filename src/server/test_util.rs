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

/// Build a minimal Parquet file with columns `time` (required int64:
/// 1000/2000/3000) and `usage` (optional double: 1.5/2.5/3.5), 3 rows.
pub fn make_test_parquet() -> Vec<u8> {
    use parquet::data_type::{DoubleType, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    let schema = Arc::new(
        parse_message_type("message schema { required int64 time; optional double usage; }")
            .unwrap(),
    );
    let mut buf = Vec::new();
    let props = Arc::new(WriterProperties::new());
    let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
    let mut row_group = writer.next_row_group().unwrap();
    {
        let mut col = row_group.next_column().unwrap().unwrap();
        col.typed::<Int64Type>()
            .write_batch(&[1000i64, 2000, 3000], None, None)
            .unwrap();
        col.close().unwrap();
    }
    {
        let mut col = row_group.next_column().unwrap().unwrap();
        // optional double → max definition level 1, values require def levels
        col.typed::<DoubleType>()
            .write_batch(&[1.5, 2.5, 3.5], Some(&[1i16, 1, 1]), None)
            .unwrap();
        col.close().unwrap();
    }
    row_group.close().unwrap();
    writer.close().unwrap();
    buf
}

/// Write a test Parquet file (3 rows) to `path`.
pub fn write_test_parquet(path: &std::path::Path) {
    std::fs::write(path, make_test_parquet()).unwrap();
}
