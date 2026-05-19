use std::future::Future;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::ws::{close_code, CloseFrame, Message, Utf8Bytes, WebSocket};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use pathfinder_common::BlockNumber;
use serde_json::value::RawValue;
use tokio::sync::{mpsc, RwLock};
use tracing::Instrument;

use super::{run_concurrently, RpcRouter};
use crate::context::RpcContext;
use crate::dto::{DeserializeForVersion, SerializeForVersion};
use crate::error::ApplicationError;
use crate::jsonrpc::websocket::WebsocketHistory;
use crate::jsonrpc::{RpcError, RpcRequest, RpcResponse};
use crate::types::request::SubscriptionBlockId;
use crate::{RpcVersion, SubscriptionId};

/// See [`RpcSubscriptionFlow`].
#[async_trait]
pub(super) trait RpcSubscriptionEndpoint: Send + Sync {
    // Start the subscription.
    async fn invoke(&self, params: InvokeParams) -> Result<tokio::task::JoinHandle<()>, RpcError>;
}

pub(super) struct InvokeParams {
    router: RpcRouter,
    input: serde_json::Value,
    subscription_id: SubscriptionId,
    subscriptions: Arc<Subscriptions>,
    ws_tx: mpsc::Sender<Result<Message, RpcResponse>>,
    lock: Arc<RwLock<()>>,
}

#[derive(Default, Debug)]
pub struct Subscriptions {
    subscriptions: DashMap<SubscriptionId, tokio::task::JoinHandle<()>>,
    next_id: AtomicU32,
}

impl Subscriptions {
    pub fn remove(
        &self,
        subscription_id: &SubscriptionId,
    ) -> Option<(SubscriptionId, tokio::task::JoinHandle<()>)> {
        self.subscriptions.remove(subscription_id)
    }

    pub fn contains_key(&self, subscription_id: &SubscriptionId) -> bool {
        self.subscriptions.contains_key(subscription_id)
    }

    pub fn insert(
        &self,
        subscription_id: SubscriptionId,
        handle: tokio::task::JoinHandle<()>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        self.subscriptions.insert(subscription_id, handle)
    }

    pub fn next_id(&self) -> SubscriptionId {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SubscriptionId(id)
    }
}

/// This trait is the main entry point for subscription endpoint
/// implementations.
///
/// Many subscription endpoints allow for historical data to be streamed before
/// starting to stream active updates. This is done by having the subscription
/// request pass a `block` parameter indicating the block to start from. This
/// trait is designed to make it easy to implement this behavior, and difficult
/// to make mistakes (e.g. race conditions or accidentally dropping messages).
///
/// The `catch_up` method is used to stream historical data, while the
/// `subscribe` method is used to subscribe to active updates. The
/// `starting_block` method extracts the first block to start streaming from.
/// This will probably always just be the `block` field of the request.
///
/// If a subscription endpoint does not need to stream historical data, it
/// should always return an empty vec from `catch_up`.
///
/// The flow is implemented as follows:
/// - Catch up from the starting block to the latest block known to pathfinder,
///   in batches. Call that block K.
/// - Subscribe to active updates. Fetch the first update, along with the block
///   number that it applies to.
/// - Catch up from block K to the block just before the first active update.
///   This is done to ensure that no blocks are missed between the previous
///   catch-up and the subscription.
/// - Stream the first active update, and then keep streaming the rest.
pub trait RpcSubscriptionFlow: Send + Sync {
    /// `params` field of the subscription request.
    type Params: crate::dto::DeserializeForVersion + Clone + Send + Sync + 'static;
    /// The notification type to be sent to the client.
    type Notification: crate::dto::SerializeForVersion + std::fmt::Debug + Send + Sync + 'static;
    /// The maximum number of blocks to catch up to in a single batch.
    const CATCH_UP_BATCH_SIZE: u64 = 64;

    /// Validate the subscription parameters. If the parameters are invalid,
    /// return an error.
    fn validate_params(_params: &Self::Params) -> Result<(), RpcError> {
        Ok(())
    }

    /// The block to start streaming from. If the subscription endpoint does not
    /// support catching up, leave this method unimplemented.
    fn starting_block(_params: &Self::Params) -> SubscriptionBlockId {
        SubscriptionBlockId::Latest
    }

    /// Fetch historical data from the `from` block to the `to` block. The
    /// range is inclusive on both ends. If there is no historical data in the
    /// range, return an empty vec. If the subscription endpoint does not
    /// support catching up, leave this method unimplemented.
    fn catch_up(
        _state: &RpcContext,
        _params: &Self::Params,
        _from: BlockNumber,
        _to: BlockNumber,
    ) -> impl Future<Output = Result<CatchUp<Self::Notification>, RpcError>> + Send {
        async { Ok(Default::default()) }
    }

    /// Subscribe to active updates.
    fn subscribe(
        state: RpcContext,
        version: RpcVersion,
        params: Self::Params,
        tx: mpsc::Sender<SubscriptionMessage<Self::Notification>>,
    ) -> impl Future<Output = Result<(), RpcError>> + Send;
}

pub struct CatchUp<T> {
    pub messages: Vec<SubscriptionMessage<T>>,
    /// [`SubscriptionMessage`] already contains a `block_number` field, but
    /// `messages` can be empty (e.g. due to some filtering logic), so the last
    /// block caught up to must be sent separately.
    ///
    /// If there are no blocks in the block range given to
    /// [`RpcSubscriptionFlow::catch_up`], this field should be [`None`].
    pub last_block: Option<BlockNumber>,
}

impl<T> Default for CatchUp<T> {
    fn default() -> Self {
        Self {
            messages: Default::default(),
            last_block: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct SubscriptionMessage<T> {
    /// [`RpcSubscriptionFlow::Notification`] to be sent to the client.
    pub notification: T,
    /// The block number of the notification. If the notification does not have
    /// a block number, this value does not matter.
    pub block_number: BlockNumber,
    /// The value for the `method` field of the subscription notification sent
    /// to the client.
    pub subscription_name: &'static str,
}

#[async_trait]
impl<T> RpcSubscriptionEndpoint for T
where
    T: RpcSubscriptionFlow + 'static,
{
    async fn invoke(
        &self,
        InvokeParams {
            router,
            input,
            subscription_id,
            subscriptions,
            ws_tx,
            lock,
        }: InvokeParams,
    ) -> Result<tokio::task::JoinHandle<()>, RpcError> {
        let params = T::Params::deserialize(crate::dto::Value::new(input, router.version))
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        T::validate_params(&params)?;

        let tx = SubscriptionSender {
            subscription_id,
            subscriptions: Arc::clone(&subscriptions),
            tx: ws_tx,
            version: router.version,
            _phantom: Default::default(),
        };

        let max_history = router
            .context
            .websocket
            .as_ref()
            .map(|ws_ctx| ws_ctx.max_history)
            .expect("Subscription methods should not be called when websockets are disabled");
        let storage = router.context.storage.clone();
        let starting_block = T::starting_block(&params);

        let mut current_block = util::task::spawn_blocking(move |_| -> Result<_, RpcError> {
            let mut conn = storage.connection()?;
            let db = conn.transaction()?;

            let starting_block = match starting_block {
                SubscriptionBlockId::Number(starting_block_number)
                    if db.blockchain_pruning_enabled() =>
                {
                    let earliest = db
                        .earliest_block_number()
                        .map_err(RpcError::InternalError)?
                        .unwrap_or(BlockNumber::GENESIS);
                    let starting_block = std::cmp::max(earliest, starting_block_number);
                    SubscriptionBlockId::Number(starting_block)
                }
                _ => starting_block,
            };
            let starting_block = pathfinder_common::BlockId::from(starting_block);

            db.block_number(starting_block)
                .map_err(RpcError::InternalError)?
                .map(|starting_block| -> Result<_, RpcError> {
                    match max_history {
                        WebsocketHistory::Limited(limit) => {
                            let latest = db
                                .block_number(pathfinder_common::BlockId::Latest)
                                .map_err(RpcError::InternalError)?
                                .unwrap_or(BlockNumber::GENESIS);
                            // + 1 because `starting_block` also counts as one block.
                            let requested_history = (latest + 1)
                                .checked_sub(starting_block.get())
                                .map(|requested| requested.get())
                                .expect("Starting block should be behind latest");

                            if requested_history > limit {
                                let requested_history = usize::try_from(requested_history)
                                    .expect("Requested history conversion error");
                                let limit =
                                    usize::try_from(limit).expect("Max history conversion error");
                                return Err(ApplicationError::TooManyBlocksBack {
                                    limit,
                                    requested: requested_history,
                                }
                                .into());
                            }

                            Ok(starting_block)
                        }
                        WebsocketHistory::Unlimited => Ok(starting_block),
                    }
                })
                .transpose()?
                .ok_or_else(|| ApplicationError::BlockNotFound.into())
        })
        .await
        .map_err(|e| RpcError::InternalError(e.into()))??;

        Ok(util::task::spawn(async move {
            let _subscription_guard = SubscriptionsGuard {
                subscription_id,
                subscriptions,
            };
            // This lock ensures that the streaming of subscriptions doesn't start before
            // the caller sends the success response for the subscription request.
            let _lock_guard = lock.read().await;

            // Catch up to the latest block in batches of BATCH_SIZE.
            tracing::trace!(%current_block, "Catching up");
            loop {
                // -1 because the end is inclusive, otherwise we get batches of
                // `CATCH_UP_BATCH_SIZE + 1` which probably doesn't really
                // matter, but it's misleading.
                let end = current_block + Self::CATCH_UP_BATCH_SIZE - 1;
                let catch_up = match T::catch_up(&router.context, &params, current_block, end).await
                {
                    Ok(messages) => messages,
                    Err(e) => {
                        tx.send_err(e)
                            .await
                            // Could error if the subscription is closing.
                            .ok();
                        return;
                    }
                };
                let last_block = match catch_up.last_block {
                    Some(last_block) => last_block,
                    None => {
                        // `None` means that there were no messages for the given block range.
                        break;
                    }
                };
                for msg in catch_up.messages {
                    if tx
                        .send(msg.notification, msg.subscription_name)
                        .await
                        .is_err()
                    {
                        // Subscription closing.
                        return;
                    }
                }
                // Increment by 1 because the catch_up range is inclusive.
                current_block = last_block + 1;
                if last_block < end {
                    // This was the last batch.
                    break;
                }
            }

            // Subscribe to new blocks. Receive the first subscription message.
            let (tx1, mut rx1) = mpsc::channel::<SubscriptionMessage<T::Notification>>(1024);
            util::task::spawn({
                let params = params.clone();
                let context = router.context.clone();
                let rpc_version = router.version;
                let tx = tx.clone();
                async move {
                    if let Err(e) = T::subscribe(context, rpc_version, params, tx1).await {
                        tx.send_err(e).await.ok();
                    }
                    tracing::trace!("Subscription task exited");
                }
            }.instrument(tracing::debug_span!("subscribe_task")));
            let first_msg = match rx1.recv().await {
                Some(msg) => msg,
                None => {
                    // Subscription closing.
                    return;
                }
            };

            // Catch up from the latest block that we already caught up to, to the first
            // block that will be streamed from the subscription. This way we don't miss any
            // blocks. Because the catch_up range is inclusive, we need to subtract 1 from
            // the block number (i.e. take its parent).
            let end = first_msg.block_number.parent();
            match end {
                Some(end) if current_block <= end => {
                    tracing::trace!(%current_block, %end, "Catching up to the first subscription message");
                    let catch_up =
                        match T::catch_up(&router.context, &params, current_block, end).await {
                            Ok(messages) => messages,
                            Err(e) => {
                                tx.send_err(e)
                                    .await
                                    // Could error if the subscription is closing.
                                    .ok();
                                return;
                            }
                        };
                    for msg in catch_up.messages {
                        if tx
                            .send(msg.notification, msg.subscription_name)
                            .await
                            .is_err()
                        {
                            // Subscription closing.
                            return;
                        }
                    }
                }
                _ => {
                    // The range is empty.
                }
            }

            // Send the first subscription message and then forward the rest.
            if let Err(e) = tx
                .send(first_msg.notification, first_msg.subscription_name)
                .await
            {
                // Subscription closing.
                tracing::trace!(error=?e, "Error sending first subscription message, closing subscription");
                return;
            }
            while let Some(msg) = rx1.recv().await {
                tracing::trace!(block_number=%msg.block_number, notification=?msg.notification, "Sending subscription notification");
                if let Err(e) = tx
                    .send(msg.notification, msg.subscription_name)
                    .await
                {
                    // Subscription closing.
                    tracing::trace!(error=?e, "Error sending subscription message, closing subscription");
                    break;
                }
            }

            rx1.close();
            tracing::trace!("Subscription closed");
        }.instrument(tracing::debug_span!("subscription", subscription_id=%subscription_id.0))))
    }
}

/// A guard to ensure that the subscription handle is removed when the
/// subscription task corresponding to that handle returns.
struct SubscriptionsGuard {
    subscription_id: SubscriptionId,
    subscriptions: Arc<Subscriptions>,
}

impl Drop for SubscriptionsGuard {
    fn drop(&mut self) {
        self.subscriptions.remove(&self.subscription_id);
    }
}

type WsSender = mpsc::Sender<Result<Message, RpcResponse>>;
type WsReceiver = mpsc::Receiver<Result<Message, axum::Error>>;

/// Split a websocket into an MPSC sender and receiver.
/// These two are later passed to [`handle_json_rpc_socket`]. This separation
/// serves to allow easier testing. The sender sends `Result<_, RpcResponse>`
/// purely for convenience, and the [`RpcResponse`] will be encoded into a
/// [`Message::Text`].
pub fn split_ws(ws: WebSocket, version: RpcVersion) -> (WsSender, WsReceiver) {
    let (mut ws_sender, mut ws_receiver) = ws.split();
    // Send messages to the websocket using an MPSC channel.
    let (sender_tx, mut sender_rx) = mpsc::channel::<Result<Message, RpcResponse>>(1024);
    util::task::spawn_with_cancel(move |cancellation_token| {
        async move {
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        // Ignore the error since we're shutting down anyway.
                        let _ = ws_sender.send(Message::Close(Some(CloseFrame {
                            code: close_code::NORMAL,
                            reason:  Utf8Bytes::from_static("Server shutdown"),
                        }))).await.ok();
                        break;
                    }
                    msg = sender_rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        match msg {
                            Ok(msg) => {
                                if let Err(e) = ws_sender.send(msg).await {
                                    tracing::debug!(error=?e, "Error sending websocket message");
                                    break;
                                }
                            }
                            Err(e) => {
                                let data = serde_json::to_string(&e.serialize(crate::dto::Serializer::new(version)).unwrap()).unwrap();
                                if let Err(e) = ws_sender
                                    .send(Message::Text(data.into()))
                                    .await
                                {
                                    tracing::debug!(error=?e, "Error sending websocket error message");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    });
    // Receive messages from the websocket using an MPSC channel.
    let (receiver_tx, receiver_rx) = mpsc::channel::<Result<Message, axum::Error>>(1024);
    util::task::spawn(async move {
        while let Some(msg) = ws_receiver.next().await {
            if let Err(e) = receiver_tx.send(msg).await {
                tracing::debug!(error=?e, "Error sending incoming websocket over channel");
                break;
            }
        }
    });
    (sender_tx, receiver_rx)
}

pub fn handle_json_rpc_socket(
    state: RpcRouter,
    ws_tx: mpsc::Sender<Result<Message, RpcResponse>>,
    mut ws_rx: mpsc::Receiver<Result<Message, axum::Error>>,
) {
    let subscriptions = Arc::new(Subscriptions::default());
    // Read and handle messages from the websocket.
    util::task::spawn(async move {
        loop {
            let request = match ws_rx.recv().await {
                Some(Ok(Message::Text(msg))) => msg.as_str().to_string(),
                Some(Ok(Message::Binary(bytes))) => match String::from_utf8(bytes.to_vec()) {
                    Ok(msg) => msg,
                    Err(e) => {
                        if let Err(e) = ws_tx
                            .send(Err(RpcResponse::parse_error(e.to_string(), state.version)))
                            .await
                        {
                            // Connection is closing.
                            tracing::debug!(error=?e, "Error sending websocket decoding error message");
                            break;
                        }
                        continue;
                    }
                },
                Some(Ok(Message::Pong(_) | Message::Ping(_))) => {
                    // Ping and pong messages are handled automatically by axum.
                    continue;
                }
                Some(Ok(Message::Close(_))) | None => {
                    // Websocket closed.
                    return;
                }
                Some(Err(e)) => {
                    tracing::trace!(error = ?e, "Error receiving websocket message");
                    return;
                }
            };

            // This lock ensures that the streaming of subscriptions doesn't start before we
            // send the success response for the subscription request. Once this write guard
            // is dropped, all of the read guards can proceed.
            let lock = Arc::new(RwLock::new(()));
            let _guard = lock.write().await;

            // Unfortunately due to this https://github.com/serde-rs/json/issues/497
            // we cannot use an enum with borrowed raw values inside to do a single
            // deserialization for us. Instead we have to distinguish manually
            // between a single request and a batch request which we do by checking
            // the first byte.
            let request = request.trim_start();
            if !request.starts_with('[') {
                let raw_value: &RawValue = match serde_json::from_str(request) {
                    Ok(raw_value) => raw_value,
                    Err(e) => {
                        if let Err(e) = ws_tx
                            .send(Err(RpcResponse::parse_error(e.to_string(), state.version)))
                            .await
                        {
                            // Connection is closing.
                            tracing::debug!(error=?e, "Error sending websocket parse error message");
                            break;
                        }
                        continue;
                    }
                };
                match handle_request(
                    &state,
                    raw_value,
                    Arc::clone(&subscriptions),
                    ws_tx.clone(),
                    lock.clone(),
                )
                .await
                {
                    Ok(Some(response)) | Err(response) => {
                        let data = serde_json::to_string(
                            &response
                                .serialize(crate::dto::Serializer::new(state.version))
                                .unwrap(),
                        ).unwrap();
                        if let Err(e) = ws_tx
                            .send(Ok(Message::Text(
                                data.into()
                            )))
                            .await
                        {
                            // Connection is closing.
                            tracing::debug!(error=?e, "Error sending websocket response message");
                            break;
                        }
                    }
                    Ok(None) => {
                        // No response.
                        continue;
                    }
                }
            } else {
                // Batch request.
                let requests = match serde_json::from_str::<Vec<&RawValue>>(request) {
                    Ok(requests) => requests,
                    Err(e) => {
                        if let Err(e) = ws_tx
                            .send(Err(RpcResponse::parse_error(e.to_string(), state.version)))
                            .await
                        {
                            // Connection is closing.
                            tracing::debug!(error=?e, "Error sending websocket parse error message");
                            break;
                        }
                        continue;
                    }
                };

                if requests.is_empty() {
                    // According to the JSON-RPC spec, a batch request cannot be empty.
                    if let Err(e) = ws_tx
                        .send(Err(RpcResponse::invalid_request(
                            "A batch request must contain at least one request".to_owned(),
                            state.version,
                        )))
                        .await
                    {
                        // Connection is closing.
                        tracing::debug!(error=?e, "Error sending websocket invalid request message");
                        break;
                    }
                }

                let responses = run_concurrently(
                    state.context.config.batch_concurrency_limit,
                    requests.into_iter().enumerate(),
                    {
                        |(idx, request)| {
                            let state = &state;
                            let ws_tx = ws_tx.clone();
                            let subscriptions = subscriptions.clone();
                            let lock = lock.clone();
                            async move {
                                match handle_request(state, request, subscriptions, ws_tx, lock)
                                    .instrument(tracing::debug_span!("ws batch", idx))
                                    .await
                                {
                                    Ok(Some(response)) | Err(response) => Some(response),
                                    Ok(None) => None,
                                }
                            }
                        }
                    },
                )
                .await
                .flatten()
                .collect::<Vec<RpcResponse>>();

                // All requests were notifications, no response needed.
                if responses.is_empty() {
                    continue;
                }

                let values = responses
                    .into_iter()
                    .map(|response| {
                        response
                            .serialize(crate::dto::Serializer::new(state.version))
                            .unwrap()
                    })
                    .collect::<Vec<_>>();

                let data = serde_json::to_string(&values).unwrap();
                if let Err(e) = ws_tx
                    .send(Ok(Message::Text(data.into())))
                    .await
                {
                    // Connection is closing.
                    tracing::debug!(error=?e, "Error sending websocket response message");
                    break;
                }
            }
        }
    }.in_current_span());
}

/// Handle a single request. Returns `Result` for convenience, so that the `?`
/// operator could be used in the body of the function. Returns `Ok(None)` if
/// the request was a notification (i.e. no response is needed).
async fn handle_request(
    state: &RpcRouter,
    raw_request: &RawValue,
    subscriptions: Arc<Subscriptions>,
    ws_tx: mpsc::Sender<Result<Message, RpcResponse>>,
    lock: Arc<RwLock<()>>,
) -> Result<Option<RpcResponse>, RpcResponse> {
    let rpc_request = serde_json::from_str::<RpcRequest<'_>>(raw_request.get())
        .map_err(|e| RpcResponse::invalid_request(e.to_string(), state.version))?;
    let req_id = rpc_request.id;

    // Ignore notification requests.
    if req_id.is_notification() {
        return Ok(None);
    }

    // Handle JSON-RPC non-subscription methods.
    if state
        .method_endpoints
        .contains_key(rpc_request.method.as_ref())
    {
        return Ok(state.run_request(raw_request.get()).await);
    }

    // Handle starknet_unsubscribe.
    if rpc_request.method == "starknet_unsubscribe" {
        // End the subscription.
        let params = rpc_request.params.0.ok_or_else(|| {
            RpcResponse::invalid_params(
                req_id.clone(),
                "Missing params for starknet_unsubscribe".to_string(),
                state.version,
            )
        })?;
        let params = crate::dto::Value::from_str(params.get(), state.version).map_err(|e| {
            RpcResponse::invalid_params(req_id.clone(), e.to_string(), state.version)
        })?;
        let params = StarknetUnsubscribeParams::deserialize(params).map_err(|e| {
            RpcResponse::invalid_params(req_id.clone(), e.to_string(), state.version)
        })?;
        let (_, handle) = subscriptions
            .remove(&params.subscription_id)
            .ok_or_else(|| RpcResponse {
                output: Err(RpcError::ApplicationError(
                    ApplicationError::InvalidSubscriptionID,
                )),
                id: req_id.clone(),
                version: state.version,
            })?;
        handle.abort();
        metrics::counter!("rpc_method_calls_total", "method" => "starknet_unsubscribe", "version" => state.version.to_str()).increment(1);
        return Ok(Some(RpcResponse {
            output: Ok(true.into()),
            id: req_id,
            version: state.version,
        }));
    }

    let (&method_name, endpoint) = state
        .subscription_endpoints
        .get_key_value(rpc_request.method.as_ref())
        .ok_or_else(|| RpcResponse::method_not_found(req_id.clone(), state.version))?;
    metrics::counter!("rpc_method_calls_total", "method" => method_name, "version" => state.version.to_str()).increment(1);

    let params = serde_json::to_value(rpc_request.params)
        .map_err(|e| RpcResponse::invalid_params(req_id.clone(), e.to_string(), state.version))?;

    // Start the subscription.
    let router = state.clone();
    let subscription_id = subscriptions.next_id();
    let ws_tx = ws_tx.clone();
    match endpoint
        .invoke(InvokeParams {
            router,
            input: params,
            subscription_id,
            subscriptions: Arc::clone(&subscriptions),
            ws_tx: ws_tx.clone(),
            lock,
        })
        .await
    {
        Ok(handle) => {
            if subscriptions.insert(subscription_id, handle).is_some() {
                panic!("subscription id overflow");
            }
            Ok(Some(RpcResponse {
                output: Ok(subscription_id
                    .serialize(crate::dto::Serializer::new(state.version))
                    .unwrap()),
                id: req_id,
                version: state.version,
            }))
        }
        Err(e) => Err(RpcResponse {
            output: Err(e),
            id: req_id,
            version: state.version,
        }),
    }
}

#[derive(Debug)]
struct StarknetUnsubscribeParams {
    subscription_id: SubscriptionId,
}

impl DeserializeForVersion for StarknetUnsubscribeParams {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                subscription_id: value.deserialize("subscription_id")?,
            })
        })
    }
}

#[derive(Debug)]
pub struct SubscriptionSender<T> {
    pub subscription_id: SubscriptionId,
    pub subscriptions: Arc<Subscriptions>,
    pub tx: mpsc::Sender<Result<Message, RpcResponse>>,
    pub version: RpcVersion,
    pub _phantom: std::marker::PhantomData<T>,
}

impl<T> Clone for SubscriptionSender<T> {
    fn clone(&self) -> Self {
        Self {
            subscription_id: self.subscription_id,
            subscriptions: self.subscriptions.clone(),
            tx: self.tx.clone(),
            version: self.version,
            _phantom: Default::default(),
        }
    }
}

impl<T: crate::dto::SerializeForVersion> SubscriptionSender<T> {
    pub async fn send(
        &self,
        value: T,
        subscription_name: &'static str,
    ) -> Result<(), mpsc::error::SendError<()>> {
        if !self.subscriptions.contains_key(&self.subscription_id) {
            // Race condition due to the subscription ending.
            return Ok(());
        }
        let notification = RpcNotification {
            jsonrpc: "2.0",
            method: subscription_name,
            params: SubscriptionResult {
                subscription_id: self.subscription_id,
                result: value,
            },
        }
        .serialize(crate::dto::Serializer::new(self.version))
        .unwrap();
        let data = serde_json::to_string(&notification).unwrap();
        self.tx
            .send(Ok(Message::Text(data.into())))
            .await
            .map_err(|_| mpsc::error::SendError(()))
    }

    pub async fn send_err(&self, err: RpcError) -> Result<(), mpsc::error::SendError<()>> {
        if !self.subscriptions.contains_key(&self.subscription_id) {
            // Race condition due to the subscription ending.
            return Ok(());
        }
        let notification = RpcNotification {
            jsonrpc: "2.0",
            method: "pathfinder_subscriptionError",
            params: SubscriptionResult {
                subscription_id: self.subscription_id,
                result: err,
            },
        }
        .serialize(crate::dto::Serializer::new(self.version))
        .unwrap();
        let data = serde_json::to_string(&notification).unwrap();
        self.tx
            .send(Ok(Message::Text(data.into())))
            .await
            .map_err(|_| mpsc::error::SendError(()))
    }
}

#[derive(Debug)]
struct RpcNotification<T> {
    jsonrpc: &'static str,
    method: &'static str,
    params: SubscriptionResult<T>,
}

#[derive(Debug)]
pub struct SubscriptionResult<T> {
    subscription_id: SubscriptionId,
    result: T,
}

impl<T> crate::dto::SerializeForVersion for RpcNotification<T>
where
    T: crate::dto::SerializeForVersion,
{
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("jsonrpc", &self.jsonrpc)?;
        serializer.serialize_field("method", &self.method)?;
        serializer.serialize_field("params", &self.params)?;
        serializer.end()
    }
}

impl<T> crate::dto::SerializeForVersion for SubscriptionResult<T>
where
    T: crate::dto::SerializeForVersion,
{
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("subscription_id", &self.subscription_id)?;
        serializer.serialize_field("result", &self.result)?;
        serializer.end()
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::ws::Message;
    use pathfinder_common::{BlockHash, BlockHeader, BlockNumber};
    use pathfinder_crypto::Felt;
    use pathfinder_storage::StorageBuilder;
    use tokio::sync::mpsc;

    use super::RpcSubscriptionEndpoint;
    use crate::context::{RpcContext, WebsocketContext};
    use crate::dto::{DeserializeForVersion, SerializeForVersion};
    use crate::error::ApplicationError;
    use crate::jsonrpc::websocket::WebsocketHistory;
    use crate::jsonrpc::{
        handle_json_rpc_socket,
        CatchUp,
        RpcRouter,
        RpcSubscriptionFlow,
        SubscriptionMessage,
    };
    use crate::types::request::SubscriptionBlockId;
    use crate::{Notifications, RpcVersion};

    #[tokio::test]
    async fn test_error_returned_from_catch_up() {
        struct ErrorFromCatchUp;

        impl RpcSubscriptionFlow for ErrorFromCatchUp {
            type Params = Params;
            type Notification = serde_json::Value;

            fn starting_block(_params: &Self::Params) -> SubscriptionBlockId {
                SubscriptionBlockId::Number(BlockNumber::GENESIS)
            }

            async fn catch_up(
                _state: &RpcContext,
                _params: &Self::Params,
                _from: BlockNumber,
                _to: BlockNumber,
            ) -> Result<CatchUp<Self::Notification>, crate::jsonrpc::RpcError> {
                Err(crate::jsonrpc::RpcError::InternalError(anyhow::anyhow!(
                    "error from catch_up"
                )))
            }

            async fn subscribe(
                _state: RpcContext,
                _version: RpcVersion,
                _params: Self::Params,
                _tx: tokio::sync::mpsc::Sender<SubscriptionMessage<Self::Notification>>,
            ) -> Result<(), crate::jsonrpc::RpcError> {
                Ok(())
            }
        }

        let router = setup(5, WebsocketHistory::Unlimited, ErrorFromCatchUp).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "test",
                    "params": {}
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let subscription_id: u64 = match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };
        let msg = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match msg {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(
            json,
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "pathfinder_subscriptionError",
                "params": {
                    "result": { "code": -32603, "message": "Internal error" },
                    "subscription_id": subscription_id.to_string()
                }
            })
        )
    }

    #[tokio::test]
    async fn test_error_returned_from_subscribe() {
        struct ErrorFromSubscribe;

        impl RpcSubscriptionFlow for ErrorFromSubscribe {
            type Params = Params;
            type Notification = serde_json::Value;

            fn starting_block(_params: &Self::Params) -> SubscriptionBlockId {
                SubscriptionBlockId::Number(BlockNumber::GENESIS)
            }

            async fn catch_up(
                _state: &RpcContext,
                _params: &Self::Params,
                _from: BlockNumber,
                _to: BlockNumber,
            ) -> Result<CatchUp<Self::Notification>, crate::jsonrpc::RpcError> {
                Ok(Default::default())
            }

            async fn subscribe(
                _state: RpcContext,
                _version: RpcVersion,
                _params: Self::Params,
                _tx: tokio::sync::mpsc::Sender<SubscriptionMessage<Self::Notification>>,
            ) -> Result<(), crate::jsonrpc::RpcError> {
                Err(crate::jsonrpc::RpcError::InternalError(anyhow::anyhow!(
                    "error from catch_up"
                )))
            }
        }

        let router = setup(5, WebsocketHistory::Unlimited, ErrorFromSubscribe).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "test",
                    "params": {}
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let subscription_id: u64 = match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };
        let msg = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match msg {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(
            json,
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "pathfinder_subscriptionError",
                "params": {
                    "result": { "code": -32603, "message": "Internal error" },
                    "subscription_id": subscription_id.to_string()
                }
            })
        )
    }

    #[tokio::test]
    async fn test_max_history_unlimited() {
        struct SubscribeUnlimitedHistory;

        impl RpcSubscriptionFlow for SubscribeUnlimitedHistory {
            type Params = Params;
            type Notification = BlockNumber;

            fn starting_block(_params: &Self::Params) -> SubscriptionBlockId {
                SubscriptionBlockId::Number(BlockNumber::GENESIS)
            }

            async fn catch_up(
                _state: &RpcContext,
                _params: &Self::Params,
                from: BlockNumber,
                to: BlockNumber,
            ) -> Result<CatchUp<Self::Notification>, crate::jsonrpc::RpcError> {
                let messages: Vec<_> = (from.get()..=to.get())
                    .map(BlockNumber::new_or_panic)
                    .map(|block| SubscriptionMessage {
                        notification: block,
                        block_number: block,
                        subscription_name: "pathfinder_unlimitedMaxHistory",
                    })
                    .collect();
                Ok(CatchUp {
                    messages,
                    // Stop the catch-up after one batch.
                    last_block: Some(to.parent().unwrap()),
                })
            }

            async fn subscribe(
                _state: RpcContext,
                _version: RpcVersion,
                _params: Self::Params,
                tx: tokio::sync::mpsc::Sender<SubscriptionMessage<Self::Notification>>,
            ) -> Result<(), crate::jsonrpc::RpcError> {
                drop(tx);
                Ok(())
            }
        }

        let router = setup(
            // We'll send one catch-up batch and then stop sending.
            SubscribeUnlimitedHistory::CATCH_UP_BATCH_SIZE,
            WebsocketHistory::Unlimited,
            SubscribeUnlimitedHistory,
        )
        .await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "test",
                    "params": {}
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let subscription_id: u64 = match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };
        let msg = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match msg {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        // Unlimited max history so messages start from GENESIS.
        const FIRST_BLOCK: u64 = 0;
        assert_eq!(
            json,
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "pathfinder_unlimitedMaxHistory",
                "params": {
                    "result": FIRST_BLOCK,
                    "subscription_id": subscription_id.to_string()
                }
            })
        )
    }

    #[tokio::test]
    async fn test_max_history_limited_error() {
        struct SubscribeLimitedHistory;
        #[derive(Debug, Clone)]
        struct ParamsLimitedHistory {
            from_block: u64,
        }

        impl DeserializeForVersion for ParamsLimitedHistory {
            fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
                value.deserialize_map(|value| {
                    Ok(Self {
                        from_block: value.deserialize("from_block")?,
                    })
                })
            }
        }

        impl RpcSubscriptionFlow for SubscribeLimitedHistory {
            type Params = ParamsLimitedHistory;
            type Notification = serde_json::Value;

            fn starting_block(params: &Self::Params) -> SubscriptionBlockId {
                let starting_block = BlockNumber::new_or_panic(params.from_block);
                SubscriptionBlockId::Number(starting_block)
            }

            async fn catch_up(
                _state: &RpcContext,
                _params: &Self::Params,
                _from: BlockNumber,
                _to: BlockNumber,
            ) -> Result<CatchUp<Self::Notification>, crate::jsonrpc::RpcError> {
                Ok(Default::default())
            }

            async fn subscribe(
                _state: RpcContext,
                _version: RpcVersion,
                _params: Self::Params,
                tx: tokio::sync::mpsc::Sender<SubscriptionMessage<Self::Notification>>,
            ) -> Result<(), crate::jsonrpc::RpcError> {
                drop(tx);
                Ok(())
            }
        }

        const WEBSOCKET_HISTORY_LIMIT: u64 = 10;
        // We'll send one catch-up batch and then stop sending.
        const NUM_BLOCKS: u64 = SubscribeLimitedHistory::CATCH_UP_BATCH_SIZE;

        let router = setup(
            NUM_BLOCKS,
            WebsocketHistory::Limited(WEBSOCKET_HISTORY_LIMIT),
            SubscribeLimitedHistory,
        )
        .await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        // Request one block more than the allowed limit.
        let bad_starting_block = NUM_BLOCKS - WEBSOCKET_HISTORY_LIMIT - 1;
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "test",
                    "params": {
                        "from_block": bad_starting_block,
                    }
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let error: serde_json::Value = match res {
            Message::Text(json) => {
                let mut json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["error"].take()
            }
            _ => panic!("Expected text message"),
        };
        let expected_error = ApplicationError::TooManyBlocksBack {
            limit: WEBSOCKET_HISTORY_LIMIT as usize,
            requested: WEBSOCKET_HISTORY_LIMIT as usize + 1,
        };
        let error_code = error["code"].as_i64().unwrap() as i32;
        let error_message = error["message"].as_str().unwrap();
        let requested_history = error["data"]["requested"].as_u64().unwrap() as usize;
        assert_eq!(error_code, expected_error.code(router.version));
        assert_eq!(
            error_message,
            format!("Cannot go back more than {WEBSOCKET_HISTORY_LIMIT} blocks")
        );
        assert_eq!(requested_history, WEBSOCKET_HISTORY_LIMIT as usize + 1);
    }

    #[tokio::test]
    async fn test_max_history_limited_ok() {
        struct SubscribeLimitedHistory;
        #[derive(Debug, Clone)]
        struct ParamsLimitedHistory {
            from_block: u64,
        }
        type SubscriptionMessage_ = SubscriptionMessage<BlockNumber>;

        impl SerializeForVersion for SubscriptionMessage_ {
            fn serialize(
                &self,
                serializer: crate::dto::Serializer,
            ) -> Result<crate::dto::Ok, crate::dto::Error> {
                let mut serializer = serializer.serialize_struct()?;
                serializer.serialize_field("notification", &self.notification)?;
                serializer.serialize_field("block_number", &self.block_number)?;
                serializer.serialize_field("subscription_name", &self.subscription_name)?;
                serializer.end()
            }
        }

        impl DeserializeForVersion for ParamsLimitedHistory {
            fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
                value.deserialize_map(|value| {
                    Ok(Self {
                        from_block: value.deserialize("from_block")?,
                    })
                })
            }
        }

        impl RpcSubscriptionFlow for SubscribeLimitedHistory {
            type Params = ParamsLimitedHistory;
            type Notification = BlockNumber;

            fn starting_block(params: &Self::Params) -> SubscriptionBlockId {
                let starting_block = BlockNumber::new_or_panic(params.from_block);
                SubscriptionBlockId::Number(starting_block)
            }

            async fn catch_up(
                _state: &RpcContext,
                _params: &Self::Params,
                from: BlockNumber,
                to: BlockNumber,
            ) -> Result<CatchUp<Self::Notification>, crate::jsonrpc::RpcError> {
                let messages: Vec<_> = (from.get()..=to.get())
                    .map(BlockNumber::new_or_panic)
                    .map(|block| SubscriptionMessage {
                        notification: block,
                        block_number: block,
                        subscription_name: "pathfinder_limitedMaxHistoryOk",
                    })
                    .collect();
                Ok(CatchUp {
                    messages,
                    // Stop the catch-up after one batch.
                    last_block: Some(to.parent().unwrap()),
                })
            }

            async fn subscribe(
                _state: RpcContext,
                _version: RpcVersion,
                _params: Self::Params,
                tx: tokio::sync::mpsc::Sender<SubscriptionMessage<Self::Notification>>,
            ) -> Result<(), crate::jsonrpc::RpcError> {
                drop(tx);
                Ok(())
            }
        }

        const WEBSOCKET_HISTORY_LIMIT: u64 = 10;
        // We'll send one catch-up batch and then stop sending.
        const NUM_BLOCKS: u64 = SubscribeLimitedHistory::CATCH_UP_BATCH_SIZE;

        let router = setup(
            NUM_BLOCKS,
            WebsocketHistory::Limited(WEBSOCKET_HISTORY_LIMIT),
            SubscribeLimitedHistory,
        )
        .await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        // Request the minimum valid starting block.
        let valid_starting_block = NUM_BLOCKS - WEBSOCKET_HISTORY_LIMIT;
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "test",
                    "params": {
                        "from_block": valid_starting_block,
                    }
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let subscription_id: u64 = match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };
        let msg = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match msg {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        // Limited max history so messages start from:
        const FIRST_BLOCK: u64 = NUM_BLOCKS - WEBSOCKET_HISTORY_LIMIT;
        assert_eq!(
            json,
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "pathfinder_limitedMaxHistoryOk",
                "params": {
                    "result": FIRST_BLOCK,
                    "subscription_id": subscription_id.to_string()
                }
            })
        )
    }

    #[derive(Debug, Clone)]
    struct Params;

    impl DeserializeForVersion for Params {
        fn deserialize(_: crate::dto::Value) -> Result<Self, serde_json::Error> {
            Ok(Self)
        }
    }

    async fn setup(
        num_blocks: u64,
        websocket_history: WebsocketHistory,
        endpoint: impl RpcSubscriptionEndpoint + 'static,
    ) -> RpcRouter {
        let storage = StorageBuilder::in_memory().unwrap();
        tokio::task::spawn_blocking({
            let storage = storage.clone();
            move || {
                let mut conn = storage.connection().unwrap();
                let db = conn.transaction().unwrap();

                let genesis_hash = BlockHash(Felt::from_u64(0));
                db.insert_block_header(&BlockHeader {
                    hash: genesis_hash,
                    number: BlockNumber::GENESIS,
                    parent_hash: BlockHash::ZERO,
                    ..Default::default()
                })
                .unwrap();

                let mut parent_hash = genesis_hash;
                for i in 1..num_blocks {
                    let hash = BlockHash(Felt::from_u64(i));
                    let header = BlockHeader {
                        hash,
                        number: BlockNumber::new_or_panic(i),
                        parent_hash,
                        ..Default::default()
                    };
                    db.insert_block_header(&header).unwrap();
                    parent_hash = hash;
                }
                db.commit().unwrap();
            }
        })
        .await
        .unwrap();
        let (_, pending_data) = tokio::sync::watch::channel(Default::default());
        let notifications = Notifications::default();
        let ctx = RpcContext::for_tests()
            .with_storage(storage)
            .with_notifications(notifications)
            .with_pending_data(pending_data.clone())
            .with_websockets(WebsocketContext::new(websocket_history));

        RpcRouter::builder(crate::RpcVersion::V08)
            .register("test", endpoint)
            .build(ctx)
    }
}
