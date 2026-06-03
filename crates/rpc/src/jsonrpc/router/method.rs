//! Implementations for RPC non-subscription method handlers.

use std::future::Future;
use std::marker::PhantomData;

use async_trait::async_trait;
use pathfinder_serde::AsBoundedVec;
use serde::de::DeserializeSeed as _;
use serde_json::value::RawValue;
use tracing::Instrument;

use super::{
    run_concurrently,
    IntoRpcEndpoint,
    RpcEndpoint,
    RpcRequestError,
    RpcResponses,
    RpcRouter,
};
use crate::context::RpcContext;
use crate::dto::{DeserializeForVersion, SerializeForVersion, Serializer};
use crate::jsonrpc::request::RawParams;
use crate::jsonrpc::response::RpcResult;
use crate::jsonrpc::router::RpcEndpointInner;
use crate::jsonrpc::{RpcError, RpcResponse};
use crate::RpcVersion;

#[async_trait]
pub(super) trait RpcMethodEndpoint: Send + Sync {
    async fn invoke<'a>(
        &self,
        state: RpcContext,
        input: RawParams<'a>,
        version: RpcVersion,
    ) -> RpcResult;
}

/// Helper to scope the responses so we can set the content-type afterwards
/// instead of dealing with branches / early exits.
pub async fn handle_json_rpc_body(
    state: &RpcRouter,
    body: &[u8],
) -> Result<RpcResponses, RpcRequestError> {
    let body = std::str::from_utf8(body).map_err(|e| RpcRequestError::ParseError(e.to_string()))?;
    let body = body.trim_start();
    // Unfortunately due to this https://github.com/serde-rs/json/issues/497
    // we cannot use an enum with borrowed raw values inside to do a single
    // deserialization for us. Instead we have to distinguish manually
    // between a single request and a batch request which we do by checking
    // the first byte.
    if !body.starts_with('[') {
        // Not a batch request.
        let request = match serde_json::from_str::<&RawValue>(body) {
            Ok(request) => request,
            Err(e) => {
                return Err(RpcRequestError::ParseError(e.to_string()));
            }
        };

        match state.run_request(request.get()).await {
            Some(response) => Ok(RpcResponses::Single(response)),
            None => Ok(RpcResponses::Empty),
        }
    } else {
        // Batch request.
        if state.context.config.disable_batch_requests {
            return Err(RpcRequestError::InvalidRequest(
                "Batch request denied".to_owned(),
            ));
        }

        let as_bounded_vec =
            AsBoundedVec::<&RawValue>::new(state.context.config.batch_size_limit.get());

        let requests =
            match as_bounded_vec.deserialize(&mut serde_json::Deserializer::from_str(body)) {
                Ok(requests) => requests,
                Err(e) => {
                    return Err(RpcRequestError::ParseError(e.to_string()));
                }
            };

        if requests.is_empty() {
            return Err(RpcRequestError::InvalidRequest(
                "A batch request must contain at least one request".to_owned(),
            ));
        }

        let responses = run_concurrently(
            state.context.config.batch_concurrency_limit,
            requests.into_iter().enumerate(),
            |(idx, request)| {
                state
                    .run_request(request.get())
                    .instrument(tracing::debug_span!("batch", idx))
            },
        )
        .await
        .flatten()
        .collect::<Vec<RpcResponse>>();

        // All requests were notifications.
        if responses.is_empty() {
            return Ok(RpcResponses::Empty);
        }

        Ok(RpcResponses::Multiple(responses))
    }
}

/// ```ignore
/// async fn example(RpcContext, impl DeserializeForVersion, RpcVersion) -> Result<Output, Into<RpcError>>
/// ```
impl<F, Input, Output, Error, Fut>
    IntoRpcEndpoint<((), (), Input), ((), (), Output), ((), (), RpcContext)> for F
where
    F: Fn(RpcContext, Input, RpcVersion) -> Fut + Sync + Send + 'static,
    Input: DeserializeForVersion + Send + Sync + 'static,
    Output: SerializeForVersion + Send + Sync + 'static,
    Error: Into<RpcError> + Send + Sync + 'static,
    Fut: Future<Output = Result<Output, Error>> + Send,
{
    fn into_endpoint(self) -> RpcEndpoint {
        struct Helper<F, Input, Output, Error> {
            f: F,
            _marker: PhantomData<(Input, Output, Error)>,
        }

        #[async_trait]
        impl<F, Input, Output, Error, Fut> RpcMethodEndpoint for Helper<F, Input, Output, Error>
        where
            F: Fn(RpcContext, Input, RpcVersion) -> Fut + Sync + Send,
            Input: DeserializeForVersion + Send + Sync,
            Output: SerializeForVersion + Send + Sync,
            Error: Into<RpcError> + Send + Sync,
            Fut: Future<Output = Result<Output, Error>> + Send,
        {
            async fn invoke<'a>(
                &self,
                state: RpcContext,
                input: RawParams<'a>,
                version: RpcVersion,
            ) -> RpcResult {
                let input = input.deserialize_for_version(version)?;
                (self.f)(state, input, version)
                    .await
                    .map_err(Into::into)?
                    .serialize(Serializer::new(version))
                    .map_err(|e| RpcError::InternalError(e.into()))
            }
        }

        RpcEndpoint(RpcEndpointInner::Method(Box::new(Helper {
            f: self,
            _marker: Default::default(),
        })))
    }
}

/// ```ignore
/// async fn example(RpcContext, impl Deserialize) -> Result<Output, Into<RpcError>>
/// ```
impl<F, Input, Output, Error, Fut> IntoRpcEndpoint<((), Input), ((), Output), ((), RpcContext)>
    for F
where
    F: Fn(RpcContext, Input) -> Fut + Sync + Send + 'static,
    Input: DeserializeForVersion + Send + Sync + 'static,
    Output: SerializeForVersion + Send + Sync + 'static,
    Error: Into<RpcError> + Send + Sync + 'static,
    Fut: Future<Output = Result<Output, Error>> + Send,
{
    fn into_endpoint(self) -> RpcEndpoint {
        struct Helper<F, Input, Output, Error> {
            f: F,
            _marker: PhantomData<(Input, Output, Error)>,
        }

        #[async_trait]
        impl<F, Input, Output, Error, Fut> RpcMethodEndpoint for Helper<F, Input, Output, Error>
        where
            F: Fn(RpcContext, Input) -> Fut + Sync + Send,
            Input: DeserializeForVersion + Send + Sync,
            Output: SerializeForVersion + Send + Sync,
            Error: Into<RpcError> + Send + Sync,
            Fut: Future<Output = Result<Output, Error>> + Send,
        {
            async fn invoke<'a>(
                &self,
                state: RpcContext,
                input: RawParams<'a>,
                version: RpcVersion,
            ) -> RpcResult {
                let input = input.deserialize_for_version(version)?;
                (self.f)(state, input)
                    .await
                    .map_err(Into::into)?
                    .serialize(Serializer::new(version))
                    .map_err(|e| RpcError::InternalError(e.into()))
            }
        }

        RpcEndpoint(RpcEndpointInner::Method(Box::new(Helper {
            f: self,
            _marker: Default::default(),
        })))
    }
}

/// ```ignore
/// async fn example(impl Deserialize) -> Result<Output, Into<RpcError>>
/// ```
#[async_trait]
impl<F, Input, Output, Error, Fut> IntoRpcEndpoint<((), Input), ((), Output), ()> for F
where
    F: Fn(Input) -> Fut + Sync + Send + 'static,
    Input: DeserializeForVersion + Sync + Send + 'static,
    Output: SerializeForVersion + Sync + Send + 'static,
    Error: Into<RpcError> + Sync + Send + 'static,
    Fut: Future<Output = Result<Output, Error>> + Send,
{
    fn into_endpoint(self) -> RpcEndpoint {
        struct Helper<F, Input, Output, Error> {
            f: F,
            _marker: PhantomData<(Input, Output, Error)>,
        }

        #[async_trait]
        impl<F, Input, Output, Error, Fut> RpcMethodEndpoint for Helper<F, Input, Output, Error>
        where
            F: Fn(Input) -> Fut + Sync + Send,
            Input: DeserializeForVersion + Send + Sync,
            Output: SerializeForVersion + Send + Sync,
            Error: Into<RpcError> + Send + Sync,
            Fut: Future<Output = Result<Output, Error>> + Send,
        {
            async fn invoke<'a>(
                &self,
                _state: RpcContext,
                input: RawParams<'a>,
                version: RpcVersion,
            ) -> RpcResult {
                let input = input.deserialize_for_version(version)?;
                (self.f)(input)
                    .await
                    .map_err(Into::into)?
                    .serialize(Serializer::new(version))
                    .map_err(|e| RpcError::InternalError(e.into()))
            }
        }

        RpcEndpoint(RpcEndpointInner::Method(Box::new(Helper {
            f: self,
            _marker: Default::default(),
        })))
    }
}

/// ```ignore
/// async fn example(RpcContext) -> Result<Output, Into<RpcError>>
/// ```
#[async_trait]
impl<F, Output, Error, Fut> IntoRpcEndpoint<(), ((), Output), ((), RpcContext)> for F
where
    F: Fn(RpcContext) -> Fut + Sync + Send + 'static,
    Output: SerializeForVersion + Sync + Send + 'static,
    Error: Into<RpcError> + Send + Sync + 'static,
    Fut: Future<Output = Result<Output, Error>> + Send,
{
    fn into_endpoint(self) -> RpcEndpoint {
        struct Helper<F, Output, Error> {
            f: F,
            _marker: PhantomData<(Output, Error)>,
        }

        #[async_trait]
        impl<F, Output, Error, Fut> RpcMethodEndpoint for Helper<F, Output, Error>
        where
            F: Fn(RpcContext) -> Fut + Sync + Send,
            Output: SerializeForVersion + Send + Sync,
            Error: Into<RpcError> + Send + Sync,
            Fut: Future<Output = Result<Output, Error>> + Send,
        {
            async fn invoke<'a>(
                &self,
                state: RpcContext,
                input: RawParams<'a>,
                version: RpcVersion,
            ) -> RpcResult {
                if !input.is_empty() {
                    return Err(RpcError::InvalidParams(
                        "This method takes no inputs".to_owned(),
                    ));
                }
                (self.f)(state)
                    .await
                    .map_err(Into::into)?
                    .serialize(Serializer::new(version))
                    .map_err(|e| RpcError::InternalError(e.into()))
            }
        }

        RpcEndpoint(RpcEndpointInner::Method(Box::new(Helper {
            f: self,
            _marker: Default::default(),
        })))
    }
}

/// ```ignore
/// async fn example() -> Result<Output, Into<RpcError>>
/// ```
#[async_trait]
impl<F, Output, Error, Fut> IntoRpcEndpoint<(), (), ((), Output)> for F
where
    F: Fn() -> Fut + Sync + Send + 'static,
    Output: SerializeForVersion + Sync + Send + 'static,
    Error: Into<RpcError> + Sync + Send + 'static,
    Fut: Future<Output = Result<Output, Error>> + Send,
{
    fn into_endpoint(self) -> RpcEndpoint {
        struct Helper<F, Output, Error> {
            f: F,
            _marker: PhantomData<(Output, Error)>,
        }

        #[async_trait]
        impl<F, Output, Error, Fut> RpcMethodEndpoint for Helper<F, Output, Error>
        where
            F: Fn() -> Fut + Sync + Send,
            Output: SerializeForVersion + Send + Sync,
            Error: Into<RpcError> + Send + Sync,
            Fut: Future<Output = Result<Output, Error>> + Send,
        {
            async fn invoke<'a>(
                &self,
                _state: RpcContext,
                input: RawParams<'a>,
                version: RpcVersion,
            ) -> RpcResult {
                if !input.is_empty() {
                    return Err(RpcError::InvalidParams(
                        "This method takes no inputs".to_owned(),
                    ));
                }
                (self.f)()
                    .await
                    .map_err(Into::into)?
                    .serialize(Serializer::new(version))
                    .map_err(|e| RpcError::InternalError(e.into()))
            }
        }

        RpcEndpoint(RpcEndpointInner::Method(Box::new(Helper {
            f: self,
            _marker: Default::default(),
        })))
    }
}

/// ```ignore
/// fn example() -> &'static str
/// ```
#[async_trait]
impl<F> IntoRpcEndpoint<(), (), ((), (), &'static str)> for F
where
    F: Fn() -> &'static str + Sync + Send + 'static,
{
    fn into_endpoint(self) -> RpcEndpoint {
        struct Helper<F> {
            f: F,
        }

        #[async_trait]
        impl<F> RpcMethodEndpoint for Helper<F>
        where
            F: Fn() -> &'static str + Sync + Send,
        {
            async fn invoke<'a>(
                &self,
                _state: RpcContext,
                input: RawParams<'a>,
                version: RpcVersion,
            ) -> RpcResult {
                if !input.is_empty() {
                    return Err(RpcError::InvalidParams(
                        "This method takes no inputs".to_owned(),
                    ));
                }
                (self.f)()
                    .serialize(Serializer::new(version))
                    .map_err(|e| RpcError::InternalError(e.into()))
            }
        }
        RpcEndpoint(RpcEndpointInner::Method(Box::new(Helper { f: self })))
    }
}
