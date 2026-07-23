use std::{
    convert::Infallible,
    marker::PhantomData,
    task::{Context, Poll},
};

use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
        logs_service_server::SERVICE_NAME as LOGS_SERVICE_NAME,
    },
    metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        metrics_service_server::SERVICE_NAME as METRICS_SERVICE_NAME,
    },
    trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::SERVICE_NAME as TRACE_SERVICE_NAME,
    },
};
#[cfg(test)]
use prost::bytes::BufMut;
use prost::{
    Message,
    bytes::{Buf, Bytes},
};
use tonic::{
    Request, Response, Status,
    codec::{Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder},
    codegen::{Body, BoxFuture, Service, StdError, http},
    server::{NamedService, UnaryService},
};

use crate::store::{AsyncWriteReceipt, MeasureIngest, PreparedIngest};

use super::prepare_raw_request;
use crate::ingest::{
    IngestState, policy::ValidateOtlp, preflight::PreflightOtlp, store_status, wait_for_write,
};

pub(crate) trait Signal: Send + Sync + 'static {
    type Request: Message + Default + MeasureIngest + ValidateOtlp + PreflightOtlp;
    type Response: Message + Default + Send + 'static;

    const NAME: &'static str;
    const PATH: &'static str;

    fn ingest(
        state: &IngestState,
        request: PreparedIngest<Self::Request>,
    ) -> anyhow::Result<AsyncWriteReceipt<usize>>;
}

pub(crate) struct Traces;
pub(crate) struct Logs;
pub(crate) struct Metrics;

impl Signal for Traces {
    type Request = ExportTraceServiceRequest;
    type Response = ExportTraceServiceResponse;
    const NAME: &'static str = TRACE_SERVICE_NAME;
    const PATH: &'static str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

    fn ingest(
        state: &IngestState,
        request: PreparedIngest<Self::Request>,
    ) -> anyhow::Result<AsyncWriteReceipt<usize>> {
        state.store.try_ingest_traces(request)
    }
}

impl Signal for Logs {
    type Request = ExportLogsServiceRequest;
    type Response = ExportLogsServiceResponse;
    const NAME: &'static str = LOGS_SERVICE_NAME;
    const PATH: &'static str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";

    fn ingest(
        state: &IngestState,
        request: PreparedIngest<Self::Request>,
    ) -> anyhow::Result<AsyncWriteReceipt<usize>> {
        state.store.try_ingest_logs(request)
    }
}

impl Signal for Metrics {
    type Request = ExportMetricsServiceRequest;
    type Response = ExportMetricsServiceResponse;
    const NAME: &'static str = METRICS_SERVICE_NAME;
    const PATH: &'static str = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";

    fn ingest(
        state: &IngestState,
        request: PreparedIngest<Self::Request>,
    ) -> anyhow::Result<AsyncWriteReceipt<usize>> {
        state.store.try_ingest_metrics(request)
    }
}

pub(crate) struct OtlpService<S> {
    state: IngestState,
    max_decoding_message_size: usize,
    signal: PhantomData<S>,
}

impl<S> OtlpService<S> {
    pub(crate) fn new(state: IngestState, max_decoding_message_size: usize) -> Self {
        Self {
            state,
            max_decoding_message_size,
            signal: PhantomData,
        }
    }
}

impl<S> Clone for OtlpService<S> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            max_decoding_message_size: self.max_decoding_message_size,
            signal: PhantomData,
        }
    }
}

impl<S, B> Service<http::Request<B>> for OtlpService<S>
where
    S: Signal,
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path() != S::PATH {
            return Box::pin(async move { Ok(unimplemented_response()) });
        }

        let method = ExportService::<S> {
            state: self.state.clone(),
            signal: PhantomData,
        };
        let max_decoding_message_size = self.max_decoding_message_size;
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(RawRequestCodec::<S::Response>::default())
                .accept_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(max_decoding_message_size);
            Ok(grpc.unary(method, request).await)
        })
    }
}

impl<S: Signal> NamedService for OtlpService<S> {
    const NAME: &'static str = S::NAME;
}

struct ExportService<S> {
    state: IngestState,
    signal: PhantomData<S>,
}

impl<S: Signal> UnaryService<Bytes> for ExportService<S> {
    type Response = S::Response;
    type Future = BoxFuture<Response<Self::Response>, Status>;

    fn call(&mut self, request: Request<Bytes>) -> Self::Future {
        let state = self.state.clone();
        Box::pin(async move {
            let (request, permit) =
                prepare_raw_request::<S::Request>(request, state.limits.clone()).await?;
            let receipt = S::ingest(&state, request.into_inner()).map_err(store_status)?;
            wait_for_write(receipt, permit)
                .await
                .map_err(store_status)?;
            Ok(Response::new(S::Response::default()))
        })
    }
}

struct RawRequestCodec<T>(PhantomData<T>);

impl<T> Default for RawRequestCodec<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T> Codec for RawRequestCodec<T>
where
    T: Message + Send + 'static,
{
    type Encode = T;
    type Decode = Bytes;
    type Encoder = ProstResponseEncoder<T>;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        ProstResponseEncoder(PhantomData)
    }
    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder::default()
    }
}

struct ProstResponseEncoder<T>(PhantomData<T>);

impl<T: Message> Encoder for ProstResponseEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, destination: &mut EncodeBuf<'_>) -> Result<(), Status> {
        item.encode(destination)
            .expect("EncodeBuf grows to fit a Prost message");
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct RawDecoder {
    decoded: bool,
}

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Status> {
        if self.decoded {
            return Err(Status::invalid_argument(
                "unary OTLP request contains more than one message",
            ));
        }
        self.decoded = true;
        // Tonic has already enforced framing, compression, and size limits. Transfer the
        // decompressed allocation so policy scanning and Prost decoding can run off-runtime.
        let remaining = source.remaining();
        Ok(Some(source.copy_to_bytes(remaining)))
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) struct RawClientCodec;

#[cfg(test)]
impl Codec for RawClientCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = RawBytesEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawBytesEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder::default()
    }
}

#[cfg(test)]
pub(crate) struct RawBytesEncoder;

#[cfg(test)]
impl Encoder for RawBytesEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, destination: &mut EncodeBuf<'_>) -> Result<(), Status> {
        destination.put_slice(&item);
        Ok(())
    }
}

fn unimplemented_response() -> http::Response<tonic::body::Body> {
    let mut response = http::Response::new(tonic::body::Body::default());
    response.headers_mut().insert(
        Status::GRPC_STATUS,
        (tonic::Code::Unimplemented as i32).into(),
    );
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        tonic::metadata::GRPC_CONTENT_TYPE,
    );
    response
}
