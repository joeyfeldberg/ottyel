use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use axum::http;
use prost::{Message, bytes::Bytes};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Code, Request, Status, service::Interceptor};
use tower::Service;

use super::{
    IngestLimits,
    policy::ValidateOtlp,
    preflight::{PreflightError, PreflightOtlp},
};

pub(super) mod server;

#[derive(Clone)]
struct AdmissionPermit(Arc<OwnedSemaphorePermit>);

pub(super) fn admission_interceptor(admission: Arc<Semaphore>) -> impl Interceptor + Clone {
    move |mut request: Request<()>| {
        let permit = admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::unavailable("ingest at capacity"))?;
        request
            .extensions_mut()
            .insert(AdmissionPermit(Arc::new(permit)));
        Ok(request)
    }
}

pub(super) async fn prepare_raw_request<T>(
    request: Request<Bytes>,
    limits: Arc<IngestLimits>,
) -> Result<(Request<T>, OwnedSemaphorePermit), Status>
where
    T: Message + Default + ValidateOtlp + PreflightOtlp,
{
    let (metadata, mut extensions, message) = request.into_parts();
    let permit = extensions
        .remove::<AdmissionPermit>()
        .ok_or_else(|| Status::internal("missing ingest admission permit"))?;
    let permit = Arc::try_unwrap(permit.0)
        .map_err(|_| Status::internal("ingest admission permit was unexpectedly cloned"))?;
    tokio::task::spawn_blocking(move || {
        T::preflight(message.as_ref(), &limits).map_err(|error| match error {
            PreflightError::Malformed(error) => Status::invalid_argument(error.to_string()),
            PreflightError::Budget(error) => Status::resource_exhausted(error.to_string()),
        })?;
        let message = T::decode(message)
            .map_err(|_| Status::invalid_argument("request body is not valid OTLP protobuf"))?;
        message
            .validate(&limits)
            .map_err(|err| Status::resource_exhausted(err.to_string()))?;
        Ok((Request::from_parts(metadata, extensions, message), permit))
    })
    .await
    .map_err(|_| Status::internal("request decoder task failed"))?
}

#[derive(Clone)]
pub(super) struct NormalizeTonicSizeError<S> {
    inner: S,
}

impl<S> NormalizeTonicSizeError<S> {
    pub(super) fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for NormalizeTonicSizeError<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Send + Clone + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let future = self.inner.call(request);
        Box::pin(async move {
            let mut response = future.await?;
            normalize_status(&mut response);
            Ok(response)
        })
    }
}

impl<S> tonic::server::NamedService for NormalizeTonicSizeError<S>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

fn normalize_status<B>(response: &mut http::Response<B>) {
    let replacement = response.extensions().get::<Status>().and_then(|status| {
        if status.code() == Code::OutOfRange
            && status
                .message()
                .starts_with("Error, decoded message length too large:")
        {
            Some(Status::resource_exhausted(status.message().to_owned()))
        } else if status.code() == Code::Internal
            && status.message().starts_with("Error decompressing:")
        {
            Some(Status::invalid_argument(status.message().to_owned()))
        } else {
            None
        }
    });
    if let Some(status) = replacement {
        response.headers_mut().insert(
            Status::GRPC_STATUS,
            http::HeaderValue::from_str(&(status.code() as i32).to_string()).unwrap(),
        );
        response.extensions_mut().insert(status);
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_status;
    use axum::http;
    use tonic::{Code, Status};

    #[test]
    fn only_known_tonic_transport_failures_are_normalized() {
        let mut response: http::Response<()> = Status::out_of_range(
            "Error, decoded message length too large: found 11 bytes, the limit is: 10 bytes",
        )
        .into_http();
        normalize_status(&mut response);
        assert_eq!(
            response.extensions().get::<Status>().unwrap().code(),
            Code::ResourceExhausted
        );
        assert_eq!(response.headers()[Status::GRPC_STATUS], "8");

        let mut application: http::Response<()> =
            Status::out_of_range("application error").into_http();
        normalize_status(&mut application);
        assert_eq!(
            application.extensions().get::<Status>().unwrap().code(),
            Code::OutOfRange
        );

        let mut corrupt: http::Response<()> =
            Status::internal("Error decompressing: corrupt gzip stream").into_http();
        normalize_status(&mut corrupt);
        assert_eq!(
            corrupt.extensions().get::<Status>().unwrap().code(),
            Code::InvalidArgument
        );

        let mut internal: http::Response<()> = Status::internal("sqlite failed").into_http();
        normalize_status(&mut internal);
        assert_eq!(
            internal.extensions().get::<Status>().unwrap().code(),
            Code::Internal
        );
    }
}
