use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pathfinder_common::event::{Event, EventIndex};
use pathfinder_common::receipt::Receipt;
use pathfinder_common::{BlockHash, BlockNumber, ContractAddress, EventKey, TransactionHash};
use pathfinder_storage::{AGGREGATE_BLOOM_BLOCK_RANGE_LEN, EVENT_KEY_FILTER_LIMIT};
use tokio::sync::mpsc;

use super::REORG_SUBSCRIPTION_NAME;
use crate::context::RpcContext;
use crate::dto::{NewTxnFinalityStatus, TxnFinalityStatus};
use crate::error::ApplicationError;
use crate::jsonrpc::{CatchUp, RpcError, RpcSubscriptionFlow, SubscriptionMessage};
use crate::method::get_events::EmittedEvent;
use crate::types::request::SubscriptionBlockId;
use crate::{Reorg, RpcVersion};

pub struct SubscribeEvents;

#[derive(Debug, Clone, Default)]
pub struct Params {
    from_addresses: HashSet<ContractAddress>,
    keys: Option<Vec<Vec<EventKey>>>,
    block_id: Option<SubscriptionBlockId>,
    finality_status: NewTxnFinalityStatus,
}

impl Params {
    fn matches(&self, event: &pathfinder_common::event::Event) -> bool {
        if !self.from_addresses.is_empty() && !self.from_addresses.contains(&event.from_address) {
            return false;
        }
        if let Some(keys) = &self.keys {
            let no_key_constraints = keys.iter().flatten().count() == 0;
            if no_key_constraints {
                return true;
            }
            if event.keys.len() < keys.len() {
                return false;
            }
            event
                .keys
                .iter()
                .zip(keys.iter())
                .all(|(key, filter)| filter.is_empty() || filter.contains(key))
        } else {
            true
        }
    }

    fn get_addresses(&self) -> Vec<ContractAddress> {
        let mut addresses: Vec<ContractAddress> = self.from_addresses.iter().cloned().collect();
        addresses.sort();
        addresses
    }
}

impl crate::dto::DeserializeForVersion for Option<Params> {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        if value.is_null() {
            // Params are optional.
            return Ok(None);
        }
        let version = value.version;
        value.deserialize_map(|value| {
            let raw_addresses = if version >= RpcVersion::V10 {
                value.deserialize_optional_array_or_scalar("from_address", |v| v.deserialize())?
            } else {
                let mut opt_address = vec![];
                if let Some(addr) = value.deserialize_optional("from_address")? {
                    opt_address.push(addr);
                }

                opt_address
            };
            let finality_status = if version < RpcVersion::V09 {
                NewTxnFinalityStatus::default()
            } else {
                value
                    .deserialize_optional("finality_status")?
                    .unwrap_or_default()
            };
            Ok(Some(Params {
                from_addresses: HashSet::from_iter(raw_addresses.into_iter().map(ContractAddress)),
                keys: value.deserialize_optional_array("keys", |value| {
                    value.deserialize_array(|value| Ok(EventKey(value.deserialize()?)))
                })?,
                block_id: value.deserialize_optional("block_id")?,
                finality_status,
            }))
        })
    }
}

#[derive(Debug)]
pub struct EventWithFinality {
    event: crate::method::get_events::EmittedEvent,
    finality: TxnFinalityStatus,
}

impl EventWithFinality {
    fn new(
        event: crate::method::get_events::EmittedEvent,
        finality: TxnFinalityStatus,
    ) -> EventWithFinality {
        Self { event, finality }
    }
}

impl crate::dto::SerializeForVersion for EventWithFinality {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;

        serializer.flatten(&self.event)?;
        if serializer.version >= RpcVersion::V09 {
            serializer.serialize_field("finality_status", &self.finality)?;
        }

        serializer.end()
    }
}

#[derive(Debug)]
pub enum Notification {
    EmittedEvent(EventWithFinality),
    Reorg(Arc<Reorg>),
}

impl crate::dto::SerializeForVersion for Notification {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self {
            Notification::EmittedEvent(event) => event.serialize(serializer),
            Notification::Reorg(reorg) => reorg.serialize(serializer),
        }
    }
}

const SUBSCRIPTION_NAME: &str = "starknet_subscriptionEvents";

impl RpcSubscriptionFlow for SubscribeEvents {
    type Params = Option<Params>;
    type Notification = Notification;
    const CATCH_UP_BATCH_SIZE: u64 = AGGREGATE_BLOOM_BLOCK_RANGE_LEN;

    fn validate_params(params: &Self::Params) -> Result<(), RpcError> {
        if let Some(params) = params {
            if let Some(keys) = &params.keys {
                if keys.len() > EVENT_KEY_FILTER_LIMIT {
                    return Err(RpcError::ApplicationError(
                        ApplicationError::TooManyKeysInFilter {
                            limit: EVENT_KEY_FILTER_LIMIT,
                            requested: keys.len(),
                        },
                    ));
                }
            }
        }
        Ok(())
    }

    fn starting_block(params: &Self::Params) -> SubscriptionBlockId {
        params
            .as_ref()
            .and_then(|req| req.block_id)
            .unwrap_or(SubscriptionBlockId::Latest)
    }

    async fn catch_up(
        state: &RpcContext,
        params: &Self::Params,
        from: BlockNumber,
        to: BlockNumber,
    ) -> Result<CatchUp<Self::Notification>, RpcError> {
        let params = params.clone().unwrap_or_default();
        let storage = state.storage.clone();
        let (events, last_l1_block, last_block) = util::task::spawn_blocking(move |_| -> Result<_, RpcError> {
            let mut conn = storage.connection()?;
            let db = conn.transaction()?;

            if db.blockchain_pruning_enabled() {
                let blockchain_history_tip = db
                    .earliest_block_number()
                    .map_err(RpcError::InternalError)?
                    .unwrap_or(BlockNumber::GENESIS);
                if from < blockchain_history_tip {
                    tracing::debug!(
                        from_block = %from,
                        %blockchain_history_tip,
                        "Event catch-up batch is below the blockchain history tip, sending an error and closing the subscription"
                    );
                    // Some of the blocks that were supposed to be part of this catch-up batch have
                    // been pruned in the meantime. Send the user an error because we cannot
                    // maintain data integrity at this point.
                    return Err(RpcError::InternalError(anyhow::anyhow!(
                        "Next block to be streamed ({from}) could not be found. It has likely \
                         been pruned during an ongoing subscription"
                    )));
                }
            }

            let last_l1_block = db.l1_l2_pointer().map_err(RpcError::InternalError)?;

            let (events, last_block) = db
                .events_in_range(
                    from,
                    to,
                    params.get_addresses(),
                    params.keys.unwrap_or_default(),
                )
                .map_err(RpcError::InternalError)?;

            Ok((events, last_l1_block, last_block))
        })
        .await
        .map_err(|e| RpcError::InternalError(e.into()))??;
        let messages = events
            .into_iter()
            .map(|event| {
                let block_number = event.block_number;
                let finality = if let Some(last_l1) = last_l1_block {
                    if block_number <= last_l1 {
                        TxnFinalityStatus::AcceptedOnL1
                    } else {
                        TxnFinalityStatus::AcceptedOnL2
                    }
                } else {
                    TxnFinalityStatus::AcceptedOnL2
                };
                SubscriptionMessage {
                    notification: Notification::EmittedEvent(EventWithFinality::new(
                        event.into(),
                        finality,
                    )),
                    block_number,
                    subscription_name: SUBSCRIPTION_NAME,
                }
            })
            .collect();
        Ok(CatchUp {
            messages,
            last_block,
        })
    }

    async fn subscribe(
        state: RpcContext,
        _version: RpcVersion,
        params: Self::Params,
        tx: mpsc::Sender<SubscriptionMessage<Self::Notification>>,
    ) -> Result<(), RpcError> {
        let mut params = params.unwrap_or_default();

        let mut blocks = state.notifications.l2_blocks.subscribe();
        let mut reorgs = state.notifications.reorgs.subscribe();
        let mut pending_data = state.pending_data.subscribe();

        if let Some(ref mut keys) = params.keys {
            // Truncate empty key lists from the end of the key filter.
            if let Some(last_non_empty) = keys.iter().rposition(|keys| !keys.is_empty()) {
                keys.truncate(last_non_empty + 1);
            }
        }

        // Keep track of the updates already sent for each block. This is done in order
        // to avoid sending duplicate notifications when seeing the same block multiple
        // times in pending data (as new transactions are added). Post Starknet v0.14.0
        // this includes the pre-confirmed block and an optional pre-latest block, which
        // is why we need the map.
        let mut sent_updates_per_block: HashMap<
            BlockNumber,
            HashSet<(TransactionHash, TxnFinalityStatus)>,
        > = HashMap::new();

        // The highest committed block height seen on the L2-block stream. A block at or
        // below this has been finalized, and its `sent_updates` were dropped when it
        // was committed, so it must not be re-emitted as pending. A not-yet-pruned
        // pending view can briefly show a just committed block as a parent.
        let mut latest_committed = BlockNumber::GENESIS;

        loop {
            tokio::select! {
                reorg = reorgs.recv() => {
                    match reorg {
                        Ok(reorg) => {
                            // Remove any blocks that we were keeping track of but were reorged
                            // away.
                            sent_updates_per_block.retain(|&block_num, _| block_num < reorg.starting_block_number);
                            if tx.send(SubscriptionMessage {
                                block_number: reorg.starting_block_number,
                                notification: Notification::Reorg(reorg),
                                subscription_name: REORG_SUBSCRIPTION_NAME,
                            }).await.is_err() {
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Error receiving reorg from notifications channel, node might be \
                                 lagging: {:?}",
                                e
                            );
                            return Ok(());
                        }
                    }
                }
                block = blocks.recv() => {
                    match block {
                        Ok(block) => {
                            let block_number = block.header.number;
                            let block_hash = block.header.hash;
                            latest_committed = std::cmp::max(latest_committed, block_number);

                            tracing::trace!(%block_number, %block_hash, "Received new block");

                            // We won't be needing to keep track of updates for this block anymore,
                            // so we remove it from the map.
                            let sent_updates = sent_updates_per_block
                                .remove(&block_number)
                                .unwrap_or_default();

                            let l2_txs_and_receipts = block
                                .transactions_and_receipts
                                .iter()
                                .zip(block.events.iter());

                            // Send all events that might have been missed in the pending data. This should only
                            // happen if the subscription started after the transactions were already evicted
                            // from pending data but before receiving the L2 block that contains them.
                            for ((_, receipt), events) in l2_txs_and_receipts {
                                let tx_and_finality = (receipt.transaction_hash, TxnFinalityStatus::AcceptedOnL2);
                                if sent_updates.contains(&tx_and_finality) {
                                    continue;
                                }
                                for (idx, event) in events.iter().enumerate() {
                                    if !params.matches(event) {
                                        continue;
                                    }
                                    let emitted = EmittedEvent {
                                        data: event.data.clone(),
                                        keys: event.keys.clone(),
                                        from_address: event.from_address,
                                        block_hash: Some(block_hash),
                                        block_number: Some(block_number),
                                        transaction_hash: receipt.transaction_hash,
                                        transaction_index: receipt.transaction_index,
                                        event_index: EventIndex(idx as u64),
                                    };
                                    let notification = Notification::EmittedEvent(
                                        EventWithFinality::new(emitted, TxnFinalityStatus::AcceptedOnL2)
                                    );
                                    let msg = SubscriptionMessage {
                                        notification,
                                        block_number,
                                        subscription_name: SUBSCRIPTION_NAME,
                                    };
                                    if tx.send(msg).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Error receiving block header from notifications channel, node might be \
                                 lagging: {:?}",
                                e
                            );
                            return Ok(());
                        }
                    }
                }
                pending_changed = pending_data.changed() => {
                    if let Err(e) = pending_changed {
                        tracing::debug!(error=%e, "Pending data channel closed, stopping subscription");
                        return Ok(());
                    }

                    let pending = pending_data.borrow_and_update().clone();
                    // pre-confirmed data is returned only if explicitly requested
                    if  !params.finality_status.is_pre_confirmed() {
                        continue;
                    }

                    tracing::trace!(block_number=%pending.pre_confirmed_block_number(), "Received pre-confirmed block update");

                    let pending_finality = TxnFinalityStatus::PreConfirmed;
                    let pre_confirmed_block_number = pending.pre_confirmed_block_number();
                    let sent_pending_updates = sent_updates_per_block
                        .entry(pre_confirmed_block_number)
                        .or_default();

                    if send_event_updates(
                        pending.pre_confirmed_tx_receipts_and_events(),
                        sent_pending_updates,
                        None,
                        pre_confirmed_block_number,
                        pending_finality,
                        &params,
                        &tx
                    ).await.is_err() {
                        // Close the subscription.
                        return Ok(());
                    }

                    // Emit every un-committed parent block (the immediate
                    // pre-latest and any deeper ancestors), oldest to newest.
                    for parent in pending.parent_blocks() {
                        let pre_latest_block = &parent.block;
                        let pre_latest_block_number = pre_latest_block.number;

                        // A just-committed block can still show up as a parent in a
                        // not-yet-pruned pending view for a brief moment. Its `sent_updates`
                        // were dropped when it was committed, so re-emitting it here would
                        // falsely regress finality to PRE_CONFIRMED for transactions that
                        // were already emitted as ACCEPTED_ON_L2.
                        if pre_latest_block_number <= latest_committed {
                            continue;
                        }

                        let sent_pre_latest_updates = sent_updates_per_block
                            .entry(pre_latest_block_number)
                            .or_default();

                        if !sent_pre_latest_updates.is_empty() {
                            // We've already processed this block as a parent (once it stops being
                            // the pre-confirmed tip, its contents are frozen), nothing to do.
                            continue;
                        }

                        tracing::trace!(block_number = %pre_latest_block_number, "Un-committed parent block update");

                        if send_event_updates(
                            &pre_latest_block.transaction_receipts,
                            sent_pre_latest_updates,
                            None,
                            pre_latest_block_number,
                            TxnFinalityStatus::PreConfirmed,
                            &params,
                            &tx,
                        )
                        .await
                        .is_err() {
                            // Close subscription.
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

async fn send_event_updates(
    event_updates: &[(Receipt, Vec<Event>)],
    sent_updates_for_block: &mut HashSet<(TransactionHash, TxnFinalityStatus)>,
    block_hash: Option<BlockHash>,
    block_number: BlockNumber,
    finality_status: TxnFinalityStatus,
    params: &Params,
    msg_tx: &mpsc::Sender<SubscriptionMessage<Notification>>,
) -> Result<(), mpsc::error::SendError<SubscriptionMessage<Notification>>> {
    for (receipt, events) in event_updates {
        let tx_and_finality = (receipt.transaction_hash, finality_status);
        if !sent_updates_for_block.insert(tx_and_finality) {
            continue;
        }
        for (idx, event) in events.iter().enumerate() {
            if !params.matches(event) {
                continue;
            }
            let emitted = EmittedEvent {
                data: event.data.clone(),
                keys: event.keys.clone(),
                from_address: event.from_address,
                block_hash,
                block_number: Some(block_number),
                transaction_hash: receipt.transaction_hash,
                transaction_index: receipt.transaction_index,
                event_index: EventIndex(idx as u64),
            };
            let notification =
                Notification::EmittedEvent(EventWithFinality::new(emitted, finality_status));
            let msg = SubscriptionMessage {
                notification,
                block_number,
                subscription_name: SUBSCRIPTION_NAME,
            };
            msg_tx.send(msg).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::extract::ws::Message;
    use pathfinder_common::event::Event;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::receipt::Receipt;
    use pathfinder_common::transaction::{Transaction, TransactionVariant};
    use pathfinder_common::L2Block;
    use pathfinder_crypto::Felt;
    use pathfinder_pending_data::PendingDataCache;
    use pathfinder_storage::StorageBuilder;
    use pretty_assertions_sorted::assert_eq;
    use starknet_gateway_types::reply::PreConfirmedBlock;
    use tokio::sync::mpsc;

    use crate::context::{RpcContext, WebsocketContext};
    use crate::jsonrpc::websocket::WebsocketHistory;
    use crate::jsonrpc::{handle_json_rpc_socket, RpcResponse, RpcRouter, RpcSubscriptionFlow};
    use crate::method::subscribe_events::SubscribeEvents;
    use crate::{v06, v07, v08, v09, v10, Notifications, Reorg, RpcVersion};

    #[tokio::test]
    async fn no_filtering() {
        let num_blocks = SubscribeEvents::CATCH_UP_BATCH_SIZE + 10;
        let (router, _pending_data_cache) = setup(num_blocks, RpcVersion::V08).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        let params = serde_json::json!(
            {"block_id": {"block_number": 0}}
        );
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "starknet_subscribeEvents",
                    "params": params
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let subscription_id = match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };
        for i in 0..num_blocks {
            let expected = sample_event_message(i, subscription_id, RpcVersion::V08);
            let event = sender_rx.recv().await.unwrap().unwrap();
            let json: serde_json::Value = match event {
                Message::Text(json) => serde_json::from_str(&json).unwrap(),
                _ => panic!("Expected text message"),
            };
            assert_eq!(json, expected);
        }
        for i in 0..10 {
            retry(|| {
                router
                    .context
                    .notifications
                    .l2_blocks
                    .send(sample_block(i).into())
            })
            .await
            .unwrap();
            let expected = sample_event_message(i, subscription_id, RpcVersion::V08);
            let event = sender_rx.recv().await.unwrap().unwrap();
            let json: serde_json::Value = match event {
                Message::Text(json) => serde_json::from_str(&json).unwrap(),
                _ => panic!("Expected text message"),
            };
            assert_eq!(json, expected);
        }
        assert!(sender_rx.is_empty());
    }

    #[tokio::test]
    async fn filter_from_address() {
        let (router, _pending_data_cache) =
            setup(SubscribeEvents::CATCH_UP_BATCH_SIZE + 10, RpcVersion::V08).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        let params = serde_json::json!(
            {
                "block_id": {"block_number": 0},
                "from_address": "0xdc",
            }
        );
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "starknet_subscribeEvents",
                    "params": params
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let subscription_id = match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };
        let expected = sample_event_message(0x16, subscription_id, RpcVersion::V08);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);
        retry(|| {
            router
                .context
                .notifications
                .l2_blocks
                .send(sample_block(0x8f).into())
        })
        .await
        .unwrap();
        router
            .context
            .notifications
            .l2_blocks
            .send(sample_block(0x16).into())
            .unwrap();
        let expected = sample_event_message(0x16, subscription_id, RpcVersion::V08);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);
        assert!(sender_rx.is_empty());
    }

    #[tokio::test]
    async fn filter_keys() {
        let (router, _pending_data_cache) =
            setup(SubscribeEvents::CATCH_UP_BATCH_SIZE + 10, RpcVersion::V08).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        let params = serde_json::json!(
            {
                "block_id": {"block_number": 0},
                "keys": [["0x16"], [], ["0x17", "0x18"]],
            }
        );
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "starknet_subscribeEvents",
                    "params": params
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let subscription_id = match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };
        let expected = sample_event_message(0x16, subscription_id, RpcVersion::V08);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);
        retry(|| {
            router
                .context
                .notifications
                .l2_blocks
                .send(sample_block(0x8f).into())
        })
        .await
        .unwrap();
        router
            .context
            .notifications
            .l2_blocks
            .send(sample_block(0x16).into())
            .unwrap();
        let expected = sample_event_message(0x16, subscription_id, RpcVersion::V08);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);
        assert!(sender_rx.is_empty());
    }

    #[tokio::test]
    async fn filter_from_address_and_keys() {
        let rpc_version = RpcVersion::V10;
        let (router, _pending_data_cache) =
            setup(SubscribeEvents::CATCH_UP_BATCH_SIZE + 10, rpc_version).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        let params = serde_json::json!(
            {
                "block_id": {"block_number": 0},
                "from_address": ["0xfe", "0xdc"],
                "keys": [["0x16"], [], ["0x17", "0x18"]],
            }
        );
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "starknet_subscribeEvents",
                    "params": params
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let subscription_id = match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };
        let expected = sample_event_message(0x16, subscription_id, rpc_version);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);
        retry(|| {
            router
                .context
                .notifications
                .l2_blocks
                .send(sample_block(0x8f).into())
        })
        .await
        .unwrap();
        router
            .context
            .notifications
            .l2_blocks
            .send(sample_block(0x16).into())
            .unwrap();
        let expected = sample_event_message(0x16, subscription_id, rpc_version);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);
        assert!(sender_rx.is_empty());
    }

    #[test_log::test(tokio::test)]
    async fn filter_keys_pre_confirmed() {
        let num_blocks = SubscribeEvents::CATCH_UP_BATCH_SIZE + 10;
        let (router, pending_data_cache) = setup(num_blocks, RpcVersion::V09).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        let params = serde_json::json!(
            {
                "block_id": {"block_number": 0},
                "keys": [["0x16", format!("{:x}", num_blocks), format!("{:x}", num_blocks + 1)]],
                "finality_status": "PRE_CONFIRMED",
            }
        );
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "starknet_subscribeEvents",
                    "params": params
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let subscription_id = match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };

        let expected = sample_event_message(0x16, subscription_id, RpcVersion::V09);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);

        let next_block_number = SubscribeEvents::CATCH_UP_BATCH_SIZE + 10;
        pending_data_cache.store(
            crate::PendingData::try_from_pre_confirmed_block(
                sample_pre_confirmed_block(next_block_number).into(),
                BlockNumber::new_or_panic(next_block_number),
            )
            .unwrap(),
        );
        let expected = sample_event_message_without_block_hash(next_block_number, subscription_id);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);

        router
            .context
            .notifications
            .l2_blocks
            .send(sample_block(next_block_number).into())
            .unwrap();

        let expected = sample_event_message(next_block_number, subscription_id, RpcVersion::V09);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);

        let next_block_number = next_block_number + 1;
        assert_eq!(
            router
                .context
                .notifications
                .l2_blocks
                .send(sample_block(next_block_number).into())
                .unwrap(),
            1
        );

        let expected = sample_event_message(next_block_number, subscription_id, RpcVersion::V09);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);
        assert!(sender_rx.is_empty());
    }

    #[test_log::test(tokio::test)]
    async fn committed_parent_is_not_re_emitted_as_pending() {
        let num_blocks = SubscribeEvents::CATCH_UP_BATCH_SIZE + 10;
        let parent = num_blocks;
        let tip = num_blocks + 1;
        let (router, pending_data_cache) = setup(num_blocks, RpcVersion::V09).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);

        // "0x16" (block 22, from the DB) is a catch-up sync point
        // `parent` and `tip` are the two pending blocks we care about.
        let params = serde_json::json!({
            "block_id": {"block_number": 0},
            "keys": [["0x16", format!("{parent:x}"), format!("{tip:x}")]],
            "finality_status": "PRE_CONFIRMED",
        });
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "starknet_subscribeEvents",
                    "params": params
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let subscription_id = match sender_rx.recv().await.unwrap().unwrap() {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };

        // Catch-up emits block 22 (sync point).
        assert_eq!(
            recv(&mut sender_rx).await,
            sample_event_message(0x16, subscription_id, RpcVersion::V09)
        );

        // Pending window: tip with the parent below it. The tip is emitted first,
        // then the parent, both PRE_CONFIRMED (no block hash).
        pending_data_cache.store(sample_pending_with_parent(tip, parent));
        assert_eq!(
            recv(&mut sender_rx).await,
            sample_event_message_without_block_hash(tip, subscription_id)
        );
        assert_eq!(
            recv(&mut sender_rx).await,
            sample_event_message_without_block_hash(parent, subscription_id)
        );

        // The parent commits: its event is re-sent as ACCEPTED_ON_L2 and its
        // sent set is dropped.
        router
            .context
            .notifications
            .l2_blocks
            .send(sample_block(parent).into())
            .unwrap();
        assert_eq!(
            recv(&mut sender_rx).await,
            sample_event_message(parent, subscription_id, RpcVersion::V09)
        );

        // A not-yet-pruned pending view still lists the now-committed parent. It
        // must NOT be re-emitted as PRE_CONFIRMED. (Don't commit the tip to
        // disambiguate — that would exercise the unguarded pre-confirmed/tip path
        // and mask the check.)
        pending_data_cache.store(sample_pending_with_parent(tip, parent));
        assert_recv_nothing(&mut sender_rx).await;
    }

    #[test_log::test(tokio::test)]
    async fn ignore_pre_confirmed_data() {
        let num_blocks = SubscribeEvents::CATCH_UP_BATCH_SIZE + 10;
        let (router, pending_data_cache) = setup(num_blocks, RpcVersion::V09).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        let params = serde_json::json!(
            {
                "block_id": {"block_number": 0},
                "keys": [["0x16", format!("{:x}", num_blocks), format!("{:x}", num_blocks + 1)]],
            }
        );
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "starknet_subscribeEvents",
                    "params": params
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let subscription_id = match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };

        let expected = sample_event_message(0x16, subscription_id, RpcVersion::V09);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);

        // Send pre-confirmed data, expecting it to be ignored.
        pending_data_cache.store(
            crate::PendingData::try_from_pre_confirmed_block(
                sample_pre_confirmed_block(num_blocks).into(),
                BlockNumber::new_or_panic(num_blocks),
            )
            .unwrap(),
        );
        assert!(sender_rx.is_empty());

        // Process a new block to make sure that we are still receiving new blocks, just
        // not pre-confirmed data.
        let next_block_number = num_blocks + 1;
        let num_receivers = retry(|| {
            router
                .context
                .notifications
                .l2_blocks
                .send(sample_block(next_block_number).into())
        })
        .await
        .unwrap();
        assert_eq!(num_receivers, 1);

        // Expect `num_blocks + 1` (new block) and not `num_blocks` (pending data).
        let expected = sample_event_message(next_block_number, subscription_id, RpcVersion::V09);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);
        assert!(sender_rx.is_empty());
    }

    #[tokio::test]
    async fn include_pre_confirmed_data_when_asked() {
        let num_blocks = SubscribeEvents::CATCH_UP_BATCH_SIZE + 10;
        let (router, pending_data_cache) = setup(num_blocks, RpcVersion::V09).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        let params = serde_json::json!(
            {
                "block_id": {"block_number": 0},
                "keys": [["0x16", format!("{:x}", num_blocks), format!("{:x}", num_blocks + 1)]],
                "finality_status": "PRE_CONFIRMED",
            }
        );
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "starknet_subscribeEvents",
                    "params": params
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let subscription_id = match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => panic!("Expected text message"),
        };

        let expected = sample_event_message(0x16, subscription_id, RpcVersion::V09);
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(json, expected);

        // Send pre-confirmed data.
        pending_data_cache.store(
            crate::PendingData::try_from_pre_confirmed_block(
                sample_pre_confirmed_block(num_blocks).into(),
                BlockNumber::new_or_panic(num_blocks),
            )
            .unwrap(),
        );
        let event = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match event {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };

        let expected = serde_json::json!({
            "jsonrpc":"2.0",
            "method":"starknet_subscriptionEvents",
            "params": {
                "result": {
                    "block_number": 34,
                    "data": ["0x24", "0x25", "0x27"],
                    "finality_status": "PRE_CONFIRMED",
                    "from_address": "0x154",
                    "keys": ["0x22", "0x23", "0x24"],
                    "transaction_hash": "0x84d0"
                },
                "subscription_id": subscription_id.to_string()
            }
        });
        assert_eq!(json, expected);
    }

    #[tokio::test]
    async fn too_many_keys_filter() {
        let (router, _pending_data_cache) =
            setup(SubscribeEvents::CATCH_UP_BATCH_SIZE + 10, RpcVersion::V08).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        let params = serde_json::json!(
            {
                "block_id": {"block_number": 0},
                "from_address": "0x10",
                "keys": [
                    ["0x10"],
                    ["0x10"],
                    ["0x11"],
                    ["0x13"],
                    ["0x14"],
                    ["0x15"],
                    ["0x16"],
                    ["0x17"],
                    ["0x18"],
                    ["0x19"],
                    ["0x1a"],
                    ["0x1b"],
                    ["0x1c"],
                    ["0x1d"],
                    ["0x1e"],
                    ["0x1f"],
                    ["0x20"],
                ],
            }
        );
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "starknet_subscribeEvents",
                    "params": params
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        match res {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(
                    json,
                    serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "error": {
                            "code": 34,
                            "data": { "limit": 16, "requested": 17 },
                            "message": "Too many keys provided in a filter"
                        }
                    }),
                );
            }
            _ => panic!("Expected text message"),
        }
    }

    #[test_log::test(tokio::test)]
    async fn reorg() {
        let (router, _pending_data_cache) = setup(1, RpcVersion::V08).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);

        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "starknet_subscribeEvents",
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

        // event from "latest" block
        let res = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match res {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(
            json,
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "starknet_subscriptionEvents",
                "params": {
                    "result": {
                        "block_hash": "0x0",
                        "block_number": 0,
                        "data": ["0x2", "0x3", "0x5"],
                        "from_address": "0x0",
                        "keys": ["0x0", "0x1", "0x2"],
                        "transaction_hash": "0x0"
                    },
                    "subscription_id": subscription_id.to_string()
                }
            })
        );

        retry(|| {
            router.context.notifications.reorgs.send(
                Reorg {
                    starting_block_number: BlockNumber::new_or_panic(1),
                    starting_block_hash: BlockHash(felt!("0x1")),
                    ending_block_number: BlockNumber::new_or_panic(2),
                    ending_block_hash: BlockHash(felt!("0x2")),
                }
                .into(),
            )
        })
        .await
        .unwrap();
        let res = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match res {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };
        assert_eq!(
            json,
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "starknet_subscriptionReorg",
                "params": {
                    "result": {
                        "starting_block_hash": "0x1",
                        "starting_block_number": 1,
                        "ending_block_hash": "0x2",
                        "ending_block_number": 2
                    },
                    "subscription_id": subscription_id.to_string()
                }
            })
        );
    }

    #[tokio::test]
    async fn subscribe_with_pending_block() {
        let (router, _pending_data_cache) = setup(1, RpcVersion::V08).await;
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);

        // Send subscription request with pending block
        receiver_tx
            .send(Ok(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "starknet_subscribeEvents",
                    "params": {"block_id": "pending"}
                })
                .to_string()
                .into(),
            )))
            .await
            .unwrap();

        // Expect error response
        let res = sender_rx.recv().await.unwrap().unwrap();
        let json: serde_json::Value = match res {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        };

        assert_eq!(
            json,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {
                        "reason": "Invalid block id"
                    }
                }
            })
        );
    }

    async fn setup(
        num_blocks: u64,
        version: RpcVersion,
    ) -> (RpcRouter, std::sync::Arc<PendingDataCache>) {
        assert!(num_blocks > 0);

        let storage = StorageBuilder::in_memory().unwrap();
        tokio::task::spawn_blocking({
            let storage = storage.clone();
            move || {
                let mut conn = storage.connection().unwrap();
                let db = conn.transaction().unwrap();
                for i in 0..num_blocks {
                    let header = sample_header(i);
                    db.insert_block_header(&header).unwrap();
                    let tx = sample_transaction(i);
                    let receipt = sample_receipt(i);
                    let event = sample_event(i);
                    db.insert_transaction_data(
                        BlockNumber::new_or_panic(i),
                        &[(tx, receipt)],
                        Some(&[vec![event]]),
                    )
                    .unwrap();
                }
                db.commit().unwrap();
                tracing::debug!("Inserted {} blocks", num_blocks);
            }
        })
        .await
        .unwrap();
        let pending_data_cache = std::sync::Arc::new(PendingDataCache::new());
        let notifications = Notifications::default();
        let ctx = RpcContext::for_tests()
            .with_storage(storage)
            .with_notifications(notifications)
            .with_pending_data_cache(pending_data_cache.clone())
            .with_websockets(WebsocketContext::for_test(WebsocketHistory::Unlimited));
        match version {
            RpcVersion::V06 => (v06::register_routes().build(ctx), pending_data_cache),
            RpcVersion::V07 => (v07::register_routes().build(ctx), pending_data_cache),
            RpcVersion::V08 => (v08::register_routes().build(ctx), pending_data_cache),
            RpcVersion::V09 => (v09::register_routes().build(ctx), pending_data_cache),
            RpcVersion::V10 => (v10::register_routes().build(ctx), pending_data_cache),
            RpcVersion::PathfinderV01 => {
                unreachable!("Did not expect RPC version for tests to be Pathfinder v0.1")
            }
        }
    }

    fn sample_header(block_number: u64) -> BlockHeader {
        BlockHeader {
            hash: BlockHash(Felt::from_u64(100 * block_number)),
            number: BlockNumber::new_or_panic(block_number),
            ..Default::default()
        }
    }

    fn sample_event(block_number: u64) -> Event {
        Event {
            data: vec![
                EventData(Felt::from_u64(block_number + 2)),
                EventData(Felt::from_u64(block_number + 3)),
                EventData(Felt::from_u64(block_number + 5)),
            ],
            from_address: ContractAddress(Felt::from_u64(10 * block_number)),
            keys: vec![
                EventKey(Felt::from_u64(block_number)),
                EventKey(Felt::from_u64(block_number + 1)),
                EventKey(Felt::from_u64(block_number + 2)),
            ],
        }
    }

    fn sample_transaction(block_number: u64) -> Transaction {
        Transaction {
            hash: TransactionHash(Felt::from_u64(1000 * block_number)),
            variant: TransactionVariant::DeclareV0(Default::default()),
        }
    }

    fn sample_receipt(block_number: u64) -> Receipt {
        Receipt {
            transaction_hash: TransactionHash(Felt::from_u64(1000 * block_number)),
            transaction_index: TransactionIndex::new_or_panic(0),
            ..Default::default()
        }
    }

    fn sample_block(block_number: u64) -> L2Block {
        L2Block {
            header: sample_header(block_number),
            transactions_and_receipts: vec![(
                sample_transaction(block_number),
                sample_receipt(block_number),
            )],
            events: vec![vec![sample_event(block_number)]],
            ..Default::default()
        }
    }

    fn sample_pre_confirmed_block(block_number: u64) -> PreConfirmedBlock {
        PreConfirmedBlock {
            transaction_receipts: vec![Some((
                sample_receipt(block_number),
                vec![sample_event(block_number)],
            ))],
            transactions: vec![sample_transaction(block_number)],
            ..Default::default()
        }
    }

    /// A pre-confirmed tip with with its un-committed parent.
    fn sample_pending_with_parent(tip: u64, parent: u64) -> crate::PendingData {
        let parent = crate::PendingData::try_from_pre_confirmed_block(
            sample_pre_confirmed_block(parent).into(),
            BlockNumber::new_or_panic(parent),
        )
        .unwrap()
        .to_pre_latest_data();
        let tip_view = crate::PendingData::try_from_pre_confirmed_block(
            sample_pre_confirmed_block(tip).into(),
            BlockNumber::new_or_panic(tip),
        )
        .unwrap();
        let blocks = pathfinder_pending_data::PendingBlocks {
            pre_confirmed: tip_view.blocks.pre_confirmed.clone(),
            parents: vec![parent],
        };
        crate::PendingData {
            blocks: blocks.into(),
            state_update: tip_view.state_update.clone(),
            aggregated_state_update: tip_view.aggregated_state_update.clone(),
            number: BlockNumber::new_or_panic(tip),
            aggregated_lower_bound: BlockNumber::GENESIS,
        }
    }

    async fn recv(rx: &mut mpsc::Receiver<Result<Message, RpcResponse>>) -> serde_json::Value {
        let res = rx.recv().await.unwrap().unwrap();
        match res {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        }
    }

    async fn assert_recv_nothing(rx: &mut mpsc::Receiver<Result<Message, RpcResponse>>) {
        let timeout = std::time::Duration::from_millis(100);
        tokio::time::timeout(timeout, rx.recv())
            .await
            .expect_err("Message received when none was expected");
    }

    fn sample_event_message(
        block_number: u64,
        subscription_id: u64,
        version: RpcVersion,
    ) -> serde_json::Value {
        let mut result = serde_json::json!({
            "keys": [
                Felt::from_u64(block_number),
                Felt::from_u64(block_number + 1),
                Felt::from_u64(block_number + 2),
            ],
            "data": [
                Felt::from_u64(block_number + 2),
                Felt::from_u64(block_number + 3),
                Felt::from_u64(block_number + 5),
            ],
            "from_address": Felt::from_u64(10 * block_number),
            "block_number": block_number,
            "block_hash": Felt::from_u64(100 * block_number),
            "transaction_hash": Felt::from_u64(1000 * block_number),
        });
        if version >= RpcVersion::V09 {
            result["finality_status"] = "ACCEPTED_ON_L2".into();
        }
        if version >= RpcVersion::V10 {
            result["transaction_index"] = 0.into();
            result["event_index"] = 0.into();
        }
        serde_json::json!({
            "jsonrpc":"2.0",
            "method":"starknet_subscriptionEvents",
            "params": {
                "result": result,
                "subscription_id": subscription_id.to_string()
            }
        })
    }

    fn sample_event_message_without_block_hash(
        block_number: u64,
        subscription_id: u64,
    ) -> serde_json::Value {
        let mut message = sample_event_message(block_number, subscription_id, RpcVersion::V08);
        message["params"]["result"]
            .as_object_mut()
            .unwrap()
            .remove("block_hash");
        message["params"]["result"]["finality_status"] = "PRE_CONFIRMED".into();

        message
    }

    // Retry to let other tasks make progress.
    async fn retry<T, E>(cb: impl Fn() -> Result<T, E>) -> Result<T, E>
    where
        E: std::fmt::Debug,
    {
        const RETRIES: u64 = 25;
        for i in 0..RETRIES {
            match cb() {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if i == RETRIES - 1 {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_secs(i)).await;
                }
            }
        }
        unreachable!()
    }
}
