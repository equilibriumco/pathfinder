use std::collections::HashMap;
use std::num::NonZeroUsize;

use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::{State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures::{Future, FutureExt, StreamExt};
use http::HeaderValue;
use method::RpcMethodEndpoint;
pub use subscription::{handle_json_rpc_socket, CatchUp, RpcSubscriptionFlow, SubscriptionMessage};
use subscription::{split_ws, RpcSubscriptionEndpoint};
use tracing::Instrument;

use crate::context::RpcContext;
use crate::jsonrpc::error::RpcError;
use crate::jsonrpc::request::RpcRequest;
use crate::jsonrpc::response::RpcResponse;
use crate::RpcVersion;

mod method;
mod subscription;

pub use method::handle_json_rpc_body;

#[derive(Clone)]
pub struct RpcRouter {
    pub context: RpcContext,
    method_endpoints: &'static HashMap<&'static str, Box<dyn RpcMethodEndpoint>>,
    subscription_endpoints: &'static HashMap<&'static str, Box<dyn RpcSubscriptionEndpoint>>,
    pub version: RpcVersion,
}

pub struct RpcRouterBuilder {
    method_endpoints: HashMap<&'static str, Box<dyn RpcMethodEndpoint>>,
    subscription_endpoints: HashMap<&'static str, Box<dyn RpcSubscriptionEndpoint>>,
    version: RpcVersion,
}

impl RpcRouterBuilder {
    /// Registers an RPC method.
    ///
    /// Panics if the method was already registered.
    pub fn register<I, O, S, M: IntoRpcEndpoint<I, O, S>>(
        mut self,
        method_name: &'static str,
        method: M,
    ) -> Self {
        match IntoRpcEndpoint::into_endpoint(method).0 {
            RpcEndpointInner::Method(method) => {
                if self.method_endpoints.insert(method_name, method).is_some() {
                    panic!("'{method_name}' is already registered");
                }
                if self.subscription_endpoints.contains_key(method_name) {
                    panic!("'{method_name}' is already registered as a subscription");
                }
            }
            RpcEndpointInner::Subscription(subscription) => {
                if self
                    .subscription_endpoints
                    .insert(method_name, subscription)
                    .is_some()
                {
                    panic!("'{method_name}' is already registered");
                }
                if self.method_endpoints.contains_key(method_name) {
                    panic!("'{method_name}' is already registered as a method");
                }
            }
        }
        self
    }

    pub fn build(self, context: RpcContext) -> RpcRouter {
        // Intentionally leak the hashmaps to give them a static lifetime.
        // Since the router is expected to be long lived, this shouldn't be an issue.
        let methods = Box::new(self.method_endpoints);
        let methods = Box::leak(methods);
        let subscriptions = Box::new(self.subscription_endpoints);
        let subscriptions = Box::leak(subscriptions);
        RpcRouter {
            context,
            method_endpoints: methods,
            subscription_endpoints: subscriptions,
            version: self.version,
        }
    }

    fn new(version: RpcVersion) -> Self {
        RpcRouterBuilder {
            method_endpoints: Default::default(),
            subscription_endpoints: Default::default(),
            version,
        }
    }
}

impl RpcRouter {
    pub fn builder(version: RpcVersion) -> RpcRouterBuilder {
        RpcRouterBuilder::new(version)
    }

    /// Parses and executes a request. Returns [None] if its a notification.
    async fn run_request(&self, request: &str) -> Option<RpcResponse> {
        tracing::trace!(%request, "Running request");

        let request = match serde_json::from_str::<RpcRequest<'_>>(request) {
            Ok(request) => request,
            Err(e) => {
                return Some(RpcResponse::invalid_request(e.to_string(), self.version));
            }
        };

        // Ignore notification requests.
        if request.id.is_notification() {
            return None;
        }

        // Also grab the method_name as it is a static str, which is required by the
        // metrics.
        let Some((&method_name, method)) =
            self.method_endpoints.get_key_value(request.method.as_ref())
        else {
            return Some(RpcResponse::method_not_found(request.id, self.version));
        };

        metrics::counter!("rpc_method_calls_total", "method" => method_name, "version" => self.version.to_str()).increment(1);

        let start = std::time::Instant::now();

        let method = method
            .invoke(self.context.clone(), request.params, self.version)
            .instrument(tracing::debug_span!("rpc_call", method=%method_name));
        let result = std::panic::AssertUnwindSafe(method).catch_unwind().await;

        let duration = start.elapsed();
        metrics::histogram!("rpc_method_calls_duration_milliseconds", "method" => method_name, "version" => self.version.to_str()).record(duration.as_millis() as f64);

        let output = match result {
            Ok(output) => output,
            Err(e) => {
                tracing::warn!(method=%request.method, backtrace=?e, "RPC method panic'd");
                Err(RpcError::InternalError(anyhow::anyhow!(
                    "RPC method panic'd"
                )))
            }
        };

        if output.is_err() {
            metrics::counter!("rpc_method_calls_failed_total", "method" => method_name, "version" => self.version.to_str()).increment(1);
        }

        Some(RpcResponse {
            output,
            id: request.id,
            version: self.version,
        })
    }
}

// A slight variation on the axum json extractor.
fn is_utf8_encoded_json(headers: http::HeaderMap) -> bool {
    let Some(content_type) = headers.get(http::header::CONTENT_TYPE) else {
        return false;
    };

    let Ok(content_type) = content_type.to_str() else {
        return false;
    };

    let mime = if let Ok(mime) = content_type.parse::<mime::Mime>() {
        mime
    } else {
        return false;
    };

    // Only accept utf8 encoding, which is the default if it missing.
    let valid_charset = mime
        .get_param(mime::CHARSET)
        .map(|x| x == "utf-8")
        .unwrap_or(true);

    // `application/json` or `XXX+json` are allowed.
    let is_json = (mime.type_() == "application" && mime.subtype() == "json")
        || mime.suffix().is_some_and(|name| name == "json");

    is_json && valid_charset
}

#[axum::debug_handler]
pub async fn rpc_handler(
    State(state): State<RpcRouter>,
    headers: http::HeaderMap,
    method: http::Method,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    body: axum::body::Bytes,
) -> impl axum::response::IntoResponse {
    match ws {
        Ok(ws) => {
            if state.context.websocket.is_none() {
                return StatusCode::FORBIDDEN.into_response();
            }

            ws.max_message_size(crate::REQUEST_MAX_SIZE)
                .on_upgrade(|ws| async move {
                    let (ws_tx, ws_rx) = split_ws(ws, state.version);
                    handle_json_rpc_socket(state, ws_tx, ws_rx);
                })
        }
        Err(_) => {
            if method != http::Method::POST {
                return StatusCode::METHOD_NOT_ALLOWED.into_response();
            }

            // Only utf8 json content allowed.
            if !is_utf8_encoded_json(headers) {
                return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
            }

            let mut response = match handle_json_rpc_body(&state, body.as_ref()).await {
                Ok(responses) => match responses {
                    RpcResponses::Empty => ().into_response(),
                    RpcResponses::Single(response) => response.into_response(),
                    RpcResponses::Multiple(responses) => {
                        use crate::dto::SerializeForVersion;
                        let values = responses
                            .into_iter()
                            .map(|response| {
                                response
                                    .serialize(crate::dto::Serializer::new(state.version))
                                    .unwrap()
                            })
                            .collect::<Vec<_>>();
                        serde_json::to_string(&values).unwrap().into_response()
                    }
                },
                Err(RpcRequestError::ParseError(e)) => {
                    RpcResponse::parse_error(e, state.version).into_response()
                }
                Err(RpcRequestError::InvalidRequest(e)) => {
                    RpcResponse::invalid_request(e, state.version).into_response()
                }
            };

            use http::header::CONTENT_TYPE;
            static APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");
            response
                .headers_mut()
                .insert(CONTENT_TYPE, APPLICATION_JSON.clone());
            response
        }
    }
}

pub(super) enum RpcRequestError {
    ParseError(String),
    InvalidRequest(String),
}

#[derive(Debug)]
pub(super) enum RpcResponses {
    Empty,
    Single(RpcResponse),
    Multiple(Vec<RpcResponse>),
}

impl crate::dto::SerializeForVersion for RpcResponses {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self {
            Self::Empty => serializer.serialize_unit(),
            Self::Single(response) => serializer.serialize(response),
            Self::Multiple(responses) => {
                serializer.serialize_iter(responses.len(), &mut responses.iter())
            }
        }
    }
}

pub struct RpcEndpoint(RpcEndpointInner);

enum RpcEndpointInner {
    Method(Box<dyn RpcMethodEndpoint>),
    Subscription(Box<dyn RpcSubscriptionEndpoint>),
}

/// This trait is implemented for all types that implemented
/// [`subscription::RpcSubscriptionEndpoint`], or for async functions. You can
/// find the particular list in the [`method`] module.
///
/// (The generic parameters are there to allow multiple different blanket
/// implementations of the trait - this allows the trait to be implemented for
/// different `Fn` signatures. It's a Rust hack.)
pub trait IntoRpcEndpoint<I, O, S> {
    fn into_endpoint(self) -> RpcEndpoint;
}

impl IntoRpcEndpoint<(), (), ()> for RpcEndpoint {
    fn into_endpoint(self) -> RpcEndpoint {
        self
    }
}

impl<T> IntoRpcEndpoint<(), (), T> for T
where
    T: RpcMethodEndpoint + 'static,
{
    fn into_endpoint(self) -> RpcEndpoint {
        RpcEndpoint(RpcEndpointInner::Method(Box::new(self)))
    }
}

impl<T> IntoRpcEndpoint<(), T, ()> for T
where
    T: RpcSubscriptionEndpoint + 'static,
{
    fn into_endpoint(self) -> RpcEndpoint {
        RpcEndpoint(RpcEndpointInner::Subscription(Box::new(self)))
    }
}

/// Performs asynchronous work concurrently on an input iterator, returning an
/// `Iterator` with the output of each piece of work.
///
/// ⚠ Execution will be performed out of order. Results are
/// eventually re-ordered.
///
/// Usage example:
/// ```ignore
/// let results = run_concurrently(
///     NonZeroUsize::new(10).unwrap(),
///     0..iterations,
///     |i| async move {
///         sleep(Duration::from_millis(i) as u64).await;
///         i
///     },
/// )
/// .await
/// .collect::<Vec<i>>();
/// ```
async fn run_concurrently<O, I, F, W, V>(
    concurrency_limit: NonZeroUsize,
    input_iter: V,
    work: W,
) -> impl Iterator<Item = O>
where
    V: Iterator<Item = I> + ExactSizeIterator,
    W: Fn(I) -> F,
    F: Future<Output = O> + Sized,
{
    let capacity = input_iter.len();
    let (result_sender, mut result_receiver) = tokio::sync::mpsc::channel(capacity);

    futures::stream::iter(input_iter)
        .enumerate()
        .for_each_concurrent(Some(concurrency_limit.get()), |(index, input)| {
            let result_sender = result_sender.clone();
            let future = work(input);

            async move {
                let result = future.await;

                // No reason for this to fail as:
                //  * channel capacity is sized according to the input size,
                //  * a sender is kept alive until completion
                result_sender.send((index, result)).await.expect(
                    "This channel is expected to be open and to not go over capacity. This is a \
                     bug.",
                );
            }
        })
        .await;

    // Necessary to break the receive loop eventually.
    drop(result_sender);

    // All results should be available immediately at this point.
    let mut indexed_results = Vec::new();
    indexed_results.reserve_exact(capacity);
    while let Some(indexed_result) = result_receiver.recv().await {
        indexed_results.push(indexed_result)
    }

    indexed_results.sort_by_key(|(index, _)| *index);

    indexed_results.into_iter().map(|(_index, result)| result)
}

#[cfg(test)]
mod tests {
    use futures::SinkExt;
    use serde::{Deserialize, Serialize};
    use serde_json::{json, Value};

    use super::*;
    use crate::jsonrpc::response::RpcResult;

    async fn spawn_server(router: RpcRouter) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://127.0.0.1:{}", addr.port());

        tokio::spawn(async {
            let router = axum::Router::new()
                .route("/", axum::routing::post(rpc_handler).get(rpc_handler))
                .with_state(router);
            axum::serve(listener, router.into_make_service()).await
        });

        url
    }

    /// Spawns an RPC server with the given router and queries it with the given
    /// request over HTTP.
    async fn serve_and_query(router: RpcRouter, request: Value) -> Value {
        let url = spawn_server(router).await;
        let client = reqwest::Client::new();
        client
            .post(url.clone())
            .json(&request)
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    }

    /// Spawns an RPC server with the given router and queries it with the given
    /// request over a WS connection.
    async fn serve_and_query_ws(router: RpcRouter, request: Value) -> Value {
        let url = spawn_server(router).await.replace("http", "ws");
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            request.to_string().into(),
        ))
        .await
        .unwrap();
        let tokio_tungstenite::tungstenite::Message::Text(response) =
            ws.next().await.unwrap().unwrap()
        else {
            panic!("Expected a text response");
        };
        serde_json::from_str(&response).unwrap()
    }

    mod specification_tests {
        //! Test cases lifted directly from the [RPC specification](https://www.jsonrpc.org/specification).
        use pretty_assertions_sorted::assert_eq;
        use rstest::rstest;
        use serde_json::json;

        use super::*;
        use crate::dto::DeserializeForVersion;
        use crate::jsonrpc::websocket::{WebsocketContext, WebsocketHistory};

        fn spec_router() -> RpcRouter {
            crate::error::generate_rpc_error_subset!(ExampleError:);

            #[derive(Debug, Serialize)]
            struct SubtractInput {
                minuend: i32,
                subtrahend: i32,
            }

            impl DeserializeForVersion for SubtractInput {
                fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
                    value.deserialize_map(|value| {
                        Ok(Self {
                            minuend: value.deserialize("minuend")?,
                            subtrahend: value.deserialize("subtrahend")?,
                        })
                    })
                }
            }

            async fn subtract(input: SubtractInput) -> Result<Value, ExampleError> {
                Ok(Value::Number((input.minuend - input.subtrahend).into()))
            }

            #[derive(Debug, Serialize)]
            struct SumInput(Vec<i32>);

            impl DeserializeForVersion for SumInput {
                fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
                    Ok(Self(value.deserialize_array(|value| value.deserialize())?))
                }
            }

            async fn sum(input: SumInput) -> Result<Value, ExampleError> {
                Ok(Value::Number((input.0.iter().sum::<i32>()).into()))
            }

            #[derive(Debug, Deserialize, Serialize)]
            struct GetDataOutput(Vec<Value>);
            async fn get_data() -> Result<GetDataOutput, ExampleError> {
                Ok(GetDataOutput(vec![
                    Value::String("hello".to_owned()),
                    Value::Number(5.into()),
                ]))
            }

            impl crate::dto::SerializeForVersion for GetDataOutput {
                fn serialize(
                    &self,
                    serializer: crate::dto::Serializer,
                ) -> Result<crate::dto::Ok, crate::dto::Error> {
                    let value = serde_json::to_value(&self.0).unwrap();
                    serializer.serialize(&value)
                }
            }

            let ws_ctx = WebsocketContext::new(WebsocketHistory::Unlimited);
            RpcRouter::builder(RpcVersion::default())
                .register("subtract", subtract)
                .register("sum", sum)
                .register("get_data", get_data)
                .build(RpcContext::for_tests().with_websockets(ws_ctx))
        }

        #[rstest]
        #[case::with_positional_params(
            json!({"jsonrpc": "2.0", "method": "subtract", "params": [42, 23], "id": 2}),
            json!({"jsonrpc": "2.0", "result": 19, "id": 2}),
        )]
        #[case::with_positional_params_switched(
            json!({"jsonrpc": "2.0", "method": "subtract", "params": [23, 42], "id": 2}),
            json!({"jsonrpc": "2.0", "result": -19, "id": 2}),
        )]
        #[case::with_named_params(
            json!({"jsonrpc": "2.0", "method": "subtract", "params": {"subtrahend": 23, "minuend": 42}, "id": 3}),
            json!({"jsonrpc": "2.0", "result": 19, "id": 3}),
        )]
        #[case::with_named_params_switched(
            json!({"jsonrpc": "2.0", "method": "subtract", "params": {"minuend": 42, "subtrahend": 23}, "id": 4}),
            json!({"jsonrpc": "2.0", "result": 19, "id": 4}),
        )]
        #[case::non_existent_method(
            json!({"jsonrpc": "2.0", "method": "foobar", "id": "1"}),
            json!({"jsonrpc": "2.0", "error": {"code": -32601, "message": "Method not found"}, "id": "1"}),
        )]
        #[case::invalid_request_object(
            json!({"jsonrpc": "2.0", "method": 1, "params": "bar"}),
            json!({"jsonrpc": "2.0", "id": null,
                "error": {"code": -32600, "message": "Invalid request", "data": {
                    "reason": "invalid type: integer `1`, expected a string at line 1 column 27"
                }}}),
        )]
        #[case::empty_batch(
            json!([]),
            json!({"jsonrpc": "2.0", "id": null,
                "error": {"code": -32600, "message": "Invalid request", "data": {
                    "reason": "A batch request must contain at least one request"
                }}}),
        )]
        #[case::invalid_batch_single(
            json!([1]),
            json!([{"jsonrpc": "2.0", "id": null,
                "error": {"code": -32600, "message": "Invalid request", "data": {
                    "reason": "invalid type: integer `1`, expected struct Helper at line 1 column 1"
                }}}
            ]),
        )]
        #[case::invalid_batch_multiple(
            json!([1, 2, 3]),
            json!([
                {"jsonrpc": "2.0", "id": null,
                    "error": {"code": -32600, "message": "Invalid request", "data": {
                        "reason": "invalid type: integer `1`, expected struct Helper at line 1 column 1"
                    }}},
                {"jsonrpc": "2.0", "id": null,
                    "error": {"code": -32600, "message": "Invalid request", "data": {
                        "reason": "invalid type: integer `2`, expected struct Helper at line 1 column 1"
                    }}},
                {"jsonrpc": "2.0", "id": null,
                    "error": {"code": -32600, "message": "Invalid request", "data": {
                        "reason": "invalid type: integer `3`, expected struct Helper at line 1 column 1"
                    }}},
            ]),
        )]
        #[case::batch(
            json!([
                {"jsonrpc": "2.0", "method": "sum", "params": [1,2,4], "id": "1"},
                {"jsonrpc": "2.0", "method": "notify_hello", "params": [7]},
                {"jsonrpc": "2.0", "method": "subtract", "params": [42,23], "id": "2"},
                {"foo": "boo"},
                {"jsonrpc": "2.0", "method": "foo.get", "params": {"name": "myself"}, "id": "5"},
                {"jsonrpc": "2.0", "method": "get_data", "id": "9"}
            ]),
            json!([
                {"jsonrpc": "2.0", "result": 7, "id": "1"},
                {"jsonrpc": "2.0", "result": 19, "id": "2"},
                {"jsonrpc": "2.0", "id": null, "error":
                    {"code": -32600, "message": "Invalid request", "data": {
                        "reason": "missing field `jsonrpc` at line 1 column 13"
                    }}},
                {"jsonrpc": "2.0", "error": {"code": -32601, "message": "Method not found"}, "id": "5"},
                {"jsonrpc": "2.0", "result": ["hello", 5], "id": "9"}
            ]),
        )]
        #[tokio::test]
        async fn specification_test(#[case] request: Value, #[case] expected: Value) {
            let response = serve_and_query(spec_router(), request.clone()).await;
            assert_eq!(response, expected);
            let response = serve_and_query_ws(spec_router(), request).await;
            assert_eq!(response, expected);
        }

        #[rstest]
        #[case::with_params(json!({"jsonrpc": "2.0", "method": "update", "params": [1,2,3,4,5]}))]
        #[case::without_params(json!({"jsonrpc": "2.0", "method": "foobar"}))]
        #[case::batch(json!([
            {"jsonrpc": "2.0", "method": "notify_sum", "params": [1,2,4]},
            {"jsonrpc": "2.0", "method": "notify_hello", "params": [7]}
        ]))]
        #[tokio::test]
        async fn notifications(#[case] request: Value) {
            let url = spawn_server(spec_router()).await;

            let client = reqwest::Client::new();
            let res = client
                .post(url.clone())
                .json(&request)
                .send()
                .await
                .unwrap();

            assert_eq!(res.content_length(), Some(0));
        }

        #[rstest]
        #[case::single(
            r#"{"jsonrpc": "2.0", "method": "foobar, "params": "bar", "baz]"#,
            "expected `,` or `}` at line 1 column 40"
        )]
        #[case::batch(
            r#"[
            {"jsonrpc": "2.0", "method": "sum", "params": [1,2,4], "id": "1"},
            {"jsonrpc": "2.0", "method"
         ]"#,
            "expected `:` at line 4 column 10"
        )]
        #[tokio::test]
        async fn invalid_json(#[case] request: &'static str, #[case] reason: &'static str) {
            let url = spawn_server(spec_router()).await;

            let client = reqwest::Client::new();
            let res = client
                .post(url.clone())
                .body(request)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();

            let expected = serde_json::json!({"jsonrpc": "2.0", "error": {"code": -32700, "data": {"reason": reason}, "message": "Parse error"}, "id": null});
            assert_eq!(res, expected);

            let url = spawn_server(spec_router()).await;
            let (mut ws, _) = tokio_tungstenite::connect_async(url.replace("http", "ws"))
                .await
                .unwrap();
            ws.send(tokio_tungstenite::tungstenite::Message::Text(
                request.to_string().into(),
            ))
            .await
            .unwrap();
            let tokio_tungstenite::tungstenite::Message::Text(response) =
                ws.next().await.unwrap().unwrap()
            else {
                panic!("Expected a text response");
            };
            let res: serde_json::Value = serde_json::from_str(&response).unwrap();
            assert_eq!(res, expected);
        }
    }

    mod panic_handling {
        use super::*;
        use crate::jsonrpc::websocket::{WebsocketContext, WebsocketHistory};

        fn panic_router() -> RpcRouter {
            fn always_panic() -> &'static str {
                panic!("Oh no!");
            }

            fn always_success() -> &'static str {
                "Success"
            }

            let ws_ctx = WebsocketContext::new(WebsocketHistory::Unlimited);
            RpcRouter::builder(Default::default())
                .register("panic", always_panic)
                .register("success", always_success)
                .build(RpcContext::for_tests().with_websockets(ws_ctx))
        }

        #[tokio::test]
        async fn panic_is_internal_error() {
            let response = serve_and_query(
                panic_router(),
                json!(
                    {"jsonrpc": "2.0", "method": "panic", "id": 1}
                ),
            )
            .await;
            let expected = serde_json::json!({"jsonrpc": "2.0", "error": {"code": -32603, "message": "Internal error"}, "id": 1});
            assert_eq!(response, expected);

            let response = serve_and_query_ws(
                panic_router(),
                json!(
                    {"jsonrpc": "2.0", "method": "panic", "id": 1}
                ),
            )
            .await;
            let expected = serde_json::json!({"jsonrpc": "2.0", "error": {"code": -32603, "message": "Internal error"}, "id": 1});
            assert_eq!(response, expected);
        }

        #[tokio::test]
        async fn panic_in_batch_is_isolated() {
            let response = serve_and_query(
                panic_router(),
                json!([
                    {"jsonrpc": "2.0", "method": "panic", "id": 1},
                    {"jsonrpc": "2.0", "method": "success", "id": 2},
                ]),
            )
            .await;
            let expected = serde_json::json!([
                {"jsonrpc": "2.0", "error": {"code": -32603, "message": "Internal error"}, "id": 1},
                {"jsonrpc": "2.0", "result": "Success", "id": 2},
            ]);
            assert_eq!(response, expected);

            let response = serve_and_query_ws(
                panic_router(),
                json!([
                    {"jsonrpc": "2.0", "method": "panic", "id": 1},
                    {"jsonrpc": "2.0", "method": "success", "id": 2},
                ]),
            )
            .await;
            let expected = serde_json::json!([
                {"jsonrpc": "2.0", "error": {"code": -32603, "message": "Internal error"}, "id": 1},
                {"jsonrpc": "2.0", "result": "Success", "id": 2},
            ]);
            assert_eq!(response, expected);
        }
    }

    #[tokio::test]
    async fn rejects_non_json_content_header() {
        async fn always_success(_ctx: RpcContext) -> RpcResult {
            Ok(json!("Success"))
        }

        let router = RpcRouter::builder(Default::default())
            .register("success", always_success)
            .build(RpcContext::for_tests());

        let url = spawn_server(router).await;

        let client = reqwest::Client::new();
        let res = client
            .post(url.clone())
            .body(
                json!(
                    {"jsonrpc": "2.0", "method": "any", "id": 1}
                )
                .to_string(),
            )
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .send()
            .await
            .unwrap()
            .status();

        assert_eq!(res, reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn accepts_json_with_charset_utf8() {
        async fn always_success(_ctx: RpcContext) -> RpcResult {
            Ok(json!("Success"))
        }

        let router = RpcRouter::builder(Default::default())
            .register("success", always_success)
            .build(RpcContext::for_tests());

        let url = spawn_server(router).await;

        let expected = json!({"jsonrpc": "2.0", "result": "Success", "id": 1});

        let client = reqwest::Client::new();
        let res = client
            .post(url.clone())
            .body(
                json!(
                    {"jsonrpc": "2.0", "method": "success", "id": 1}
                )
                .to_string(),
            )
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/json; charset=utf-8",
            )
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();

        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn rejects_json_with_charset_utf16() {
        async fn always_success(_ctx: RpcContext) -> RpcResult {
            Ok(json!("Success"))
        }

        let router = RpcRouter::builder(Default::default())
            .register("success", always_success)
            .build(RpcContext::for_tests());

        let url = spawn_server(router).await;

        let client = reqwest::Client::new();
        let res = client
            .post(url.clone())
            .body(
                json!(
                    {"jsonrpc": "2.0", "method": "success", "id": 1}
                )
                .to_string(),
            )
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/json; charset=utf-16",
            )
            .send()
            .await
            .unwrap()
            .status();

        assert_eq!(res, reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn with_no_params() {
        fn always_success() -> &'static str {
            "Success"
        }

        let router = RpcRouter::builder(Default::default())
            .register("success", always_success)
            .build(RpcContext::for_tests());

        let url = spawn_server(router).await;

        let expected = json!({"jsonrpc": "2.0", "result": "Success", "id": 1});

        let client = reqwest::Client::new();
        let res = client
            .post(url.clone())
            .json(&json!(
                {"jsonrpc": "2.0", "method": "success", "id": 1}
            ))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();

        assert_eq!(res, expected);

        let res = client
            .post(url.clone())
            .json(&json!(
                {"jsonrpc": "2.0", "method": "success", "id": 1, "params": []}
            ))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();

        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn response_hash_content_type_json() {
        fn always_success() -> &'static str {
            "Success"
        }

        let router = RpcRouter::builder(Default::default())
            .register("success", always_success)
            .build(RpcContext::for_tests());

        let url = spawn_server(router).await;

        let client = reqwest::Client::new();
        let res = client
            .post(url.clone())
            .json(&json!(
                {"jsonrpc": "2.0", "method": "success", "id": 1}
            ))
            .send()
            .await
            .unwrap();

        use reqwest::header::CONTENT_TYPE;
        let content_type = res
            .headers()
            .get(CONTENT_TYPE)
            .expect("content-type header should be set");

        assert_eq!(content_type, "application/json");
    }

    mod concurrent_futures {
        use std::cmp::max;
        use std::sync::Arc;
        use std::time::Duration;

        use tokio::sync::Notify;
        use tokio::time::timeout;

        use super::*;

        pub enum TaskEvent {
            Start,
            End(usize),
        }

        #[tokio::test]
        async fn concurrent_futures() {
            let iterations = 100;
            let concurrency_limit = iterations / 2;
            let events =
                concurrent_count(iterations, NonZeroUsize::new(concurrency_limit).unwrap()).await;
            assert_eq!(max_concurrency_level(&events), concurrency_limit);

            // The test should have messed up with the execution order, which is important
            // to assess that the results are ordered according to the input
            // order and not the execution order.
            let order_difference = events
                .into_iter()
                .filter_map(|event| {
                    if let TaskEvent::End(index) = event {
                        Some(index)
                    } else {
                        None
                    }
                })
                .enumerate()
                .any(|(execution_index, task_index)| execution_index != task_index);
            assert!(order_difference);
        }

        #[tokio::test]
        async fn sequential_futures() {
            let iterations = 100;
            let concurrency_limit = 1;
            let events =
                concurrent_count(iterations, NonZeroUsize::new(concurrency_limit).unwrap()).await;
            assert_eq!(max_concurrency_level(&events), concurrency_limit);

            // Make sure there isn't a change in the execution order so there is no change
            // in behavior with the introduction of this feature.
            let order_match = events
                .into_iter()
                .filter_map(|event| {
                    if let TaskEvent::End(index) = event {
                        Some(index)
                    } else {
                        None
                    }
                })
                .enumerate()
                .all(|(execution_index, task_index)| execution_index == task_index);
            assert!(order_match);
        }

        async fn concurrent_count(
            iterations: usize,
            concurrency_limit: NonZeroUsize,
        ) -> Vec<TaskEvent> {
            #[derive(Clone)]
            struct State {
                index: usize,
                notify: Arc<Notify>,
            }

            let task_states = (0..iterations)
                .map(|index| State {
                    index,
                    notify: Arc::new(Notify::new()),
                })
                .collect::<Vec<State>>();

            let (event_sender, mut event_receiver) = tokio::sync::mpsc::channel(iterations * 2);
            let (result_sender, result_receiver) = tokio::sync::oneshot::channel();

            // Run the tasks in the background
            tokio::spawn({
                let task_states = task_states.clone();
                async move {
                    let results =
                        run_concurrently(concurrency_limit, task_states.into_iter(), |state| {
                            let event_sender = event_sender.clone();
                            async move {
                                event_sender.send(TaskEvent::Start).await.unwrap();
                                // Wait until allowed to continue.
                                let _start = state.notify.notified().await;
                                event_sender
                                    .send(TaskEvent::End(state.index))
                                    .await
                                    .unwrap();
                                state.index
                            }
                        })
                        .await
                        .collect::<Vec<usize>>();

                    result_sender.send(results).unwrap();
                }
            });

            // N tasks should already have started, N being the `concurrency_limit`.
            let mut events = vec![];
            for _i in 0..concurrency_limit.get() {
                let event = timeout(Duration::from_millis(100), event_receiver.recv())
                    .await
                    .expect("Timeout reached, there's something wrong with the concurrency limit")
                    .expect("The event channel closed early");
                events.push(event);
            }

            // Allow all tasks to continue, descending order to mess up with completion
            // order.
            for i in (0..iterations).rev() {
                task_states[i].notify.notify_one();
            }

            let results = result_receiver.await.unwrap();

            // Make sure the results are complete.
            assert_eq!(results.len(), iterations);
            // Make sure the results are ordered consistently with the input.
            results
                .into_iter()
                .enumerate()
                .for_each(|(expected, result)| {
                    assert_eq!(result, expected);
                });

            // Now retrieve the rest of the events
            while let Some(event) = event_receiver.recv().await {
                events.push(event)
            }

            events
        }

        fn max_concurrency_level(events: &[TaskEvent]) -> usize {
            let mut started_task = 0;
            let mut max_simultaneous = 0;

            for event in events {
                match event {
                    TaskEvent::Start => {
                        started_task += 1;
                        max_simultaneous = max(max_simultaneous, started_task);
                    }
                    TaskEvent::End(_) => {
                        started_task -= 1;
                    }
                }
            }

            max_simultaneous
        }
    }
}
