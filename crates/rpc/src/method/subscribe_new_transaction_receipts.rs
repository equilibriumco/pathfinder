use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pathfinder_common::event::Event;
use pathfinder_common::receipt::Receipt;
use pathfinder_common::transaction::Transaction;
use pathfinder_common::{BlockHash, BlockNumber, ContractAddress, TransactionHash};
use tokio::sync::mpsc;

use super::REORG_SUBSCRIPTION_NAME;
use crate::context::RpcContext;
use crate::dto::{TxnFinalityStatus, TxnReceiptWithBlockInfo};
use crate::jsonrpc::{RpcError, RpcSubscriptionFlow, SubscriptionMessage};
use crate::{Reorg, RpcVersion};

pub struct SubscribeNewTransactionReceipts;

#[derive(Debug, Clone, PartialEq)]
pub struct Params {
    finality_status: Vec<TxnFinalityStatus>,
    sender_address: Option<HashSet<ContractAddress>>,
}

impl Default for Params {
    fn default() -> Self {
        Params {
            finality_status: vec![TxnFinalityStatus::AcceptedOnL2],
            sender_address: None,
        }
    }
}

impl Params {
    fn matches(&self, sender_address: &ContractAddress, finality: TxnFinalityStatus) -> bool {
        if let Some(addresses) = &self.sender_address {
            if !addresses.contains(sender_address) {
                return false;
            }
        }
        self.finality_status.contains(&finality)
    }
}

impl crate::dto::DeserializeForVersion for Option<Params> {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        if value.is_null() {
            return Ok(None);
        }
        value.deserialize_map(|value| {
            Ok(Some(Params {
                finality_status: value
                    .deserialize_optional_array("finality_status", |v| {
                        v.deserialize::<TxnFinalityStatusWithoutL1Accepted>()
                            .map(Into::into)
                    })?
                    .unwrap_or_else(|| vec![TxnFinalityStatus::AcceptedOnL2]),
                sender_address: value
                    .deserialize_optional_array("sender_address", |addr| {
                        Ok(ContractAddress(addr.deserialize()?))
                    })?
                    .map(|addrs| addrs.into_iter().collect()),
            }))
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TxnFinalityStatusWithoutL1Accepted {
    PreConfirmed,
    AcceptedOnL2,
}

impl crate::dto::DeserializeForVersion for TxnFinalityStatusWithoutL1Accepted {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let s: String = value.deserialize()?;
        match s.as_str() {
            "ACCEPTED_ON_L2" => Ok(Self::AcceptedOnL2),
            "PRE_CONFIRMED" => Ok(Self::PreConfirmed),
            _ => Err(serde::de::Error::unknown_variant(
                &s,
                &["ACCEPTED_ON_L2", "PRE_CONFIRMED"],
            )),
        }
    }
}

impl From<TxnFinalityStatusWithoutL1Accepted> for TxnFinalityStatus {
    fn from(status: TxnFinalityStatusWithoutL1Accepted) -> Self {
        match status {
            TxnFinalityStatusWithoutL1Accepted::PreConfirmed => TxnFinalityStatus::PreConfirmed,
            TxnFinalityStatusWithoutL1Accepted::AcceptedOnL2 => TxnFinalityStatus::AcceptedOnL2,
        }
    }
}

#[derive(Debug)]
pub enum Notification {
    EmittedTransaction(Box<TransactionWithReceipt>),
    Reorg(Arc<Reorg>),
}

#[derive(Debug)]
pub struct TransactionWithReceipt {
    block_hash: Option<BlockHash>,
    block_number: BlockNumber,
    receipt: Receipt,
    transaction: Transaction,
    events: Vec<Event>,
    finality: TxnFinalityStatus,
}

impl crate::dto::SerializeForVersion for Notification {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self {
            Notification::EmittedTransaction(tx) => tx.serialize(serializer),
            Notification::Reorg(reorg) => reorg.serialize(serializer),
        }
    }
}

impl crate::dto::SerializeForVersion for TransactionWithReceipt {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        TxnReceiptWithBlockInfo {
            receipt: &self.receipt,
            transaction: &self.transaction,
            events: &self.events,
            finality: self.finality,
            block_hash: self.block_hash.as_ref(),
            block_number: self.block_number,
        }
        .serialize(serializer)
    }
}

const SUBSCRIPTION_NAME: &str = "starknet_subscriptionNewTransactionReceipts";

impl RpcSubscriptionFlow for SubscribeNewTransactionReceipts {
    type Params = Option<Params>;
    type Notification = Notification;

    async fn subscribe(
        state: RpcContext,
        _version: RpcVersion,
        params: Self::Params,
        msg_tx: mpsc::Sender<SubscriptionMessage<Self::Notification>>,
    ) -> Result<(), RpcError> {
        let params = params.unwrap_or_default();

        let mut blocks = state.notifications.l2_blocks.subscribe();
        let mut reorgs = state.notifications.reorgs.subscribe();
        let mut pending_data = state.pending_data.subscribe();

        // Keep track of the updates already sent for each block. This is done in order
        // to avoid sending duplicate notifications when seeing the same block multiple
        // times in pending data (as new transactions are added). Post Starknet v0.14.0
        // this includes the pre-confirmed block and an optional pre-latest block, which
        // is why we need the map.
        let mut sent_updates_per_block: HashMap<
            BlockNumber,
            HashSet<(TransactionHash, TxnFinalityStatus)>,
        > = HashMap::new();

        loop {
            tokio::select! {
                reorg = reorgs.recv() => {
                    match reorg {
                        Ok(reorg) => {
                            // Remove any blocks that we were keeping track of but were reorged
                            // away.
                            sent_updates_per_block.retain(|&block_num, _| block_num < reorg.starting_block_number);
                            let block_number = reorg.starting_block_number;
                            if msg_tx.send(SubscriptionMessage {
                                notification: Notification::Reorg(reorg),
                                block_number,
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
                        Err(e) => {
                            tracing::debug!(error=%e, "Block channel closed, stopping subscription");
                            return Ok(());
                        }
                        Ok(block) => {
                            tracing::trace!(block_number=%block.header.number, "New block header");

                            // We won't be needing to keep track of updates for this block anymore,
                            // so we remove it from the map.
                            let sent_updates = sent_updates_per_block
                                .remove(&block.header.number)
                                .unwrap_or_default();

                            let l2_txs_and_receipts = block
                                .transactions_and_receipts
                                .iter()
                                .zip(block.events.iter());

                            // Send all transaction receipts that might have been missed in the pending data.
                            // This should only happen if the subscription started after the transactions were
                            // already evicted from pending data but before receiving the L2 block that contains
                            // them.
                            for ((tx, receipt), events) in l2_txs_and_receipts {
                                let sender_address = tx.variant.sender_address();
                                if !params.matches(&sender_address, TxnFinalityStatus::AcceptedOnL2) {
                                    continue;
                                }
                                let tx_and_finality = (tx.hash, TxnFinalityStatus::AcceptedOnL2);
                                if sent_updates.contains(&tx_and_finality) {
                                    continue;
                                }

                                let notification = Notification::EmittedTransaction(Box::new(TransactionWithReceipt {
                                    block_hash: Some(block.header.hash),
                                    block_number: block.header.number,
                                    receipt: receipt.clone(),
                                    transaction: tx.clone(),
                                    events: events.clone(),
                                    finality: TxnFinalityStatus::AcceptedOnL2,
                                }));
                                let msg =  SubscriptionMessage {
                                    notification,
                                    block_number: block.header.number,
                                    subscription_name: SUBSCRIPTION_NAME,
                                };
                                if msg_tx.send(msg).await.is_err() {
                                    // Close subscription.
                                    return Ok(());
                                };
                            }
                        }
                    }
                }
                pending_changed = pending_data.changed() => {
                    if let Err(e) = pending_changed {
                        tracing::debug!(error=%e, "Pending data channel closed, stopping subscription");
                        return Ok(());
                    }

                    let pending = pending_data.borrow_and_update().clone();
                    let pre_confirmed_block_number = pending.pre_confirmed_block_number();
                    let pending_finality_status = crate::dto::TxnFinalityStatus::PreConfirmed;

                    tracing::trace!(
                        block_number = %pre_confirmed_block_number,
                        finality_status = ?pending_finality_status,
                        "Pre-confirmed block update"
                    );

                    let sent_pending_updates = sent_updates_per_block
                        .entry(pre_confirmed_block_number)
                        .or_default();

                    let pending_txs_and_receipts = pending
                        .pre_confirmed_transactions()
                        .iter()
                        .zip(
                            pending
                                .pre_confirmed_tx_receipts_and_events()
                                .iter()
                        );

                    if send_tx_receipt_updates(
                        pending_txs_and_receipts,
                        sent_pending_updates,
                        None,
                        pre_confirmed_block_number,
                        pending_finality_status,
                        &params,
                        &msg_tx
                    )
                    .await
                    .is_err() {
                        // Close subscription.
                        return Ok(());
                    }

                    if let Some(pre_latest_block) = pending.pre_latest_block() {
                        let pre_latest_block_number = pre_latest_block.number;

                        let sent_pre_latest_updates = sent_updates_per_block
                            .entry(pre_latest_block_number)
                            .or_default();

                        if !sent_pre_latest_updates.is_empty() {
                            // We've already processed this block as pre-latest (and once it gets
                            // promoted to pre-latest, it cannot change anymore), nothing to do.
                            continue;
                        }

                        tracing::trace!(block_number = %pre_latest_block_number, "Pre-latest block update");

                        let pre_latest_txs_and_receipts = pre_latest_block
                            .transactions
                            .iter()
                            .zip(pre_latest_block.transaction_receipts.iter());

                        if send_tx_receipt_updates(
                            pre_latest_txs_and_receipts,
                            sent_pre_latest_updates,
                            None,
                            pre_latest_block_number,
                            TxnFinalityStatus::PreConfirmed,
                            &params,
                            &msg_tx,
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

/// Send transaction receipt updates that match the given [Params] and add them
/// to the list of sent updates for this block.
///
/// Skip updates that have already been transmitted.
async fn send_tx_receipt_updates(
    tx_receipt_updates: impl Iterator<Item = (&Transaction, &(Receipt, Vec<Event>))>,
    sent_updates_for_block: &mut HashSet<(TransactionHash, TxnFinalityStatus)>,
    block_hash: Option<BlockHash>,
    block_number: BlockNumber,
    finality_status: TxnFinalityStatus,
    params: &Params,
    msg_tx: &mpsc::Sender<SubscriptionMessage<Notification>>,
) -> Result<(), mpsc::error::SendError<SubscriptionMessage<Notification>>> {
    for (transaction, (receipt, events)) in tx_receipt_updates {
        let sender_address = transaction.variant.sender_address();
        if !params.matches(&sender_address, finality_status) {
            continue;
        }
        if !sent_updates_for_block.insert((transaction.hash, finality_status)) {
            continue;
        }

        let notification = Notification::EmittedTransaction(Box::new(TransactionWithReceipt {
            block_hash,
            block_number,
            receipt: receipt.clone(),
            transaction: transaction.clone(),
            events: events.clone(),
            finality: finality_status,
        }));
        let msg = SubscriptionMessage {
            notification,
            block_number,
            subscription_name: SUBSCRIPTION_NAME,
        };
        msg_tx.send(msg).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::extract::ws::Message;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::transaction::{DeclareTransactionV0V1, Transaction, TransactionVariant};
    use pathfinder_common::L2Block;
    use pathfinder_crypto::Felt;
    use pathfinder_pending_data::PendingDataCache;
    use pathfinder_storage::StorageBuilder;
    use pretty_assertions_sorted::assert_eq;
    use starknet_gateway_types::reply::PreConfirmedBlock;
    use tokio::sync::mpsc;

    use super::Params;
    use crate::context::{RpcContext, WebsocketContext};
    use crate::dto::TxnFinalityStatus;
    use crate::jsonrpc::websocket::WebsocketHistory;
    use crate::jsonrpc::{handle_json_rpc_socket, RpcResponse};
    use crate::{v09, Notifications, PendingData, RpcVersion};

    #[test]
    fn parse_params() {
        let params = crate::dto::Value::new(
            serde_json::json!({
                "finality_status": ["ACCEPTED_ON_L2", "PRE_CONFIRMED"],
                "sender_address": ["0x1", "0x2"]
            }),
            RpcVersion::V09,
        );
        let params: Option<Params> = params.deserialize().unwrap();
        assert_eq!(
            params.unwrap(),
            Params {
                finality_status: vec![
                    TxnFinalityStatus::AcceptedOnL2,
                    TxnFinalityStatus::PreConfirmed
                ],
                sender_address: Some(
                    [contract_address!("0x1"), contract_address!("0x2")]
                        .iter()
                        .cloned()
                        .collect()
                ),
            }
        );
    }

    #[test]
    fn parse_params_fails_for_invalid_finality_status() {
        let params = crate::dto::Value::new(
            serde_json::json!({
                "finality_status": ["ACCEPTED_ON_L2", "ACCEPTED_ON_L1"],
                "sender_address": ["0x1", "0x2"]
            }),
            RpcVersion::V09,
        );
        let error = params.deserialize::<Option<Params>>().unwrap_err();
        assert_eq!(
            error.to_string(),
            "unknown variant `ACCEPTED_ON_L1`, expected `ACCEPTED_ON_L2` or `PRE_CONFIRMED`"
                .to_string()
        );
    }

    #[test_log::test(tokio::test)]
    async fn pre_confirmed_no_filtering() {
        let Setup {
            tx,
            mut rx,
            pending_data_cache,
            ..
        } = setup();
        tx.send(Ok(Message::Text(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "starknet_subscribeNewTransactionReceipts",
                "params": {
                    "finality_status": ["ACCEPTED_ON_L2", "PRE_CONFIRMED"]
                }
            })
            .to_string()
            .into(),
        )))
        .await
        .unwrap();
        let response = rx.recv().await.unwrap().unwrap();
        let subscription_id = match response {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => {
                panic!("Expected text message");
            }
        };
        assert_recv_nothing(&mut rx).await;

        // First pre-confirmed block update.
        pending_data_cache.store(sample_pre_confirmed_block(
            BlockNumber::new_or_panic(1),
            vec![
                (contract_address!("0x1"), transaction_hash!("0x3")),
                (contract_address!("0x2"), transaction_hash!("0x4")),
            ],
        ));
        assert_eq!(
            recv(&mut rx).await,
            sample_pre_confirmed_receipt_message(1, "0x3", subscription_id)
        );
        assert_eq!(
            recv(&mut rx).await,
            sample_pre_confirmed_receipt_message(1, "0x4", subscription_id)
        );
        assert_recv_nothing(&mut rx).await;

        // We expect that the second pre-confirmed block update will ignore
        // transactions that were already sent.
        pending_data_cache.store(sample_pre_confirmed_block(
            BlockNumber::new_or_panic(1),
            vec![
                (contract_address!("0x1"), transaction_hash!("0x3")),
                (contract_address!("0x2"), transaction_hash!("0x4")),
                (contract_address!("0x1"), transaction_hash!("0x5")),
                (contract_address!("0x2"), transaction_hash!("0x6")),
            ],
        ));
        assert_eq!(
            recv(&mut rx).await,
            sample_pre_confirmed_receipt_message(1, "0x5", subscription_id)
        );
        assert_eq!(
            recv(&mut rx).await,
            sample_pre_confirmed_receipt_message(1, "0x6", subscription_id)
        );
        assert_recv_nothing(&mut rx).await;
    }

    #[tokio::test]
    async fn pre_confirmed_filtering_one_address() {
        let Setup {
            tx,
            mut rx,
            pending_data_cache,
            ..
        } = setup();
        tx.send(Ok(Message::Text(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "starknet_subscribeNewTransactionReceipts",
                "params": {
                    "sender_address": ["0x1"],
                    "finality_status": ["ACCEPTED_ON_L2", "PRE_CONFIRMED"]
                }
            })
            .to_string()
            .into(),
        )))
        .await
        .unwrap();
        let response = rx.recv().await.unwrap().unwrap();
        let subscription_id = match response {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => {
                panic!("Expected text message");
            }
        };
        assert_recv_nothing(&mut rx).await;
        pending_data_cache.store(sample_pre_confirmed_block(
            BlockNumber::new_or_panic(1),
            vec![
                (contract_address!("0x1"), transaction_hash!("0x1")),
                (contract_address!("0x2"), transaction_hash!("0x2")),
            ],
        ));
        assert_eq!(
            recv(&mut rx).await,
            sample_pre_confirmed_receipt_message(1, "0x1", subscription_id)
        );
        assert_recv_nothing(&mut rx).await;
    }

    #[tokio::test]
    async fn pre_confirmed_filtering_two_addresses() {
        let Setup {
            tx,
            mut rx,
            pending_data_cache,
            ..
        } = setup();
        tx.send(Ok(Message::Text(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "starknet_subscribeNewTransactionReceipts",
                "params": {
                    "sender_address": ["0x1", "0x2"],
                    "finality_status": ["ACCEPTED_ON_L2", "PRE_CONFIRMED"]
                }
            })
            .to_string()
            .into(),
        )))
        .await
        .unwrap();
        let response = rx.recv().await.unwrap().unwrap();
        let subscription_id = match response {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => {
                panic!("Expected text message");
            }
        };
        assert_recv_nothing(&mut rx).await;
        pending_data_cache.store(sample_pre_confirmed_block(
            BlockNumber::new_or_panic(1),
            vec![
                (contract_address!("0x1"), transaction_hash!("0x3")),
                (contract_address!("0x2"), transaction_hash!("0x4")),
                (contract_address!("0x3"), transaction_hash!("0x5")),
            ],
        ));
        assert_eq!(
            recv(&mut rx).await,
            sample_pre_confirmed_receipt_message(1, "0x3", subscription_id)
        );
        assert_eq!(
            recv(&mut rx).await,
            sample_pre_confirmed_receipt_message(1, "0x4", subscription_id)
        );
        assert_recv_nothing(&mut rx).await;
    }

    #[test_log::test(tokio::test)]
    async fn pre_confirmed_followed_by_block_with_extra_transactions() {
        let Setup {
            tx,
            mut rx,
            pending_data_cache,
            notifications,
        } = setup();
        tx.send(Ok(Message::Text(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "starknet_subscribeNewTransactionReceipts",
                "params": {
                    "finality_status": ["ACCEPTED_ON_L2", "PRE_CONFIRMED"]
                }
            })
            .to_string()
            .into(),
        )))
        .await
        .unwrap();
        let response = rx.recv().await.unwrap().unwrap();
        let subscription_id = match response {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => {
                panic!("Expected text message");
            }
        };
        assert_recv_nothing(&mut rx).await;

        // Send a pre-confirmed block with two transactions.
        pending_data_cache.store(sample_pre_confirmed_block(
            BlockNumber::new_or_panic(1),
            vec![
                (contract_address!("0x1"), transaction_hash!("0x3")),
                (contract_address!("0x2"), transaction_hash!("0x4")),
            ],
        ));
        assert_eq!(
            recv(&mut rx).await,
            sample_pre_confirmed_receipt_message(1, "0x3", subscription_id)
        );
        assert_eq!(
            recv(&mut rx).await,
            sample_pre_confirmed_receipt_message(1, "0x4", subscription_id)
        );
        assert_recv_nothing(&mut rx).await;

        // The finalized block is sent after the pre-confirmed block, but contains more
        // transactions
        notifications
            .l2_blocks
            .send(
                sample_block(
                    BlockNumber::new_or_panic(1),
                    vec![
                        (contract_address!("0x1"), transaction_hash!("0x3")),
                        (contract_address!("0x2"), transaction_hash!("0x4")),
                        (contract_address!("0x1"), transaction_hash!("0x5")),
                        (contract_address!("0x2"), transaction_hash!("0x6")),
                    ],
                )
                .into(),
            )
            .unwrap();
        // We expect transactions 0x3 and 0x4 to be re-sent, since the finality status
        // has changed to ACCEPTED_ON_L2.
        assert_eq!(
            recv(&mut rx).await,
            sample_receipt_message(1, "0x3", subscription_id)
        );
        assert_eq!(
            recv(&mut rx).await,
            sample_receipt_message(1, "0x4", subscription_id)
        );
        assert_eq!(
            recv(&mut rx).await,
            sample_receipt_message(1, "0x5", subscription_id)
        );
        assert_eq!(
            recv(&mut rx).await,
            sample_receipt_message(1, "0x6", subscription_id)
        );
        assert_recv_nothing(&mut rx).await;

        // The next pre-confirmed block is sent.
        pending_data_cache.store(sample_pre_confirmed_block(
            BlockNumber::new_or_panic(2),
            vec![
                (contract_address!("0x1"), transaction_hash!("0x7")),
                (contract_address!("0x2"), transaction_hash!("0x8")),
            ],
        ));
        assert_eq!(
            recv(&mut rx).await,
            sample_pre_confirmed_receipt_message(2, "0x7", subscription_id)
        );
        assert_eq!(
            recv(&mut rx).await,
            sample_pre_confirmed_receipt_message(2, "0x8", subscription_id)
        );
        assert_recv_nothing(&mut rx).await;
    }

    #[test_log::test(tokio::test)]
    async fn pre_confirmed_followed_by_block_with_filter_on_finality_status() {
        let Setup {
            tx,
            mut rx,
            pending_data_cache,
            notifications,
        } = setup();
        tx.send(Ok(Message::Text(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "starknet_subscribeNewTransactionReceipts",
                "params": {
                    "finality_status": ["ACCEPTED_ON_L2"]
                }
            })
            .to_string()
            .into(),
        )))
        .await
        .unwrap();
        let response = rx.recv().await.unwrap().unwrap();
        let subscription_id = match response {
            Message::Text(json) => {
                let json: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(json["jsonrpc"], "2.0");
                assert_eq!(json["id"], 1);
                json["result"].as_str().unwrap().parse().unwrap()
            }
            _ => {
                panic!("Expected text message");
            }
        };
        assert_recv_nothing(&mut rx).await;

        notifications
            .l2_blocks
            .send(
                sample_block(
                    BlockNumber::new_or_panic(0),
                    vec![(contract_address!("0x1"), transaction_hash!("0x1"))],
                )
                .into(),
            )
            .unwrap();
        assert_eq!(
            recv(&mut rx).await,
            sample_receipt_message(0, "0x1", subscription_id)
        );
        assert_recv_nothing(&mut rx).await;

        // Send a pre-confirmed block with two transactions: since we're filtering on
        // finality status ACCEPTED_ON_L2, we expect that the pre-confirmed
        // block will not send any receipts.
        pending_data_cache.store(sample_pre_confirmed_block(
            BlockNumber::new_or_panic(1),
            vec![
                (contract_address!("0x1"), transaction_hash!("0x3")),
                (contract_address!("0x2"), transaction_hash!("0x4")),
            ],
        ));
        assert_recv_nothing(&mut rx).await;

        // The finalized block is sent after the pre-confirmed block, but contains more
        // transactions.
        notifications
            .l2_blocks
            .send(
                sample_block(
                    BlockNumber::new_or_panic(1),
                    vec![
                        (contract_address!("0x1"), transaction_hash!("0x3")),
                        (contract_address!("0x2"), transaction_hash!("0x4")),
                        (contract_address!("0x1"), transaction_hash!("0x5")),
                        (contract_address!("0x2"), transaction_hash!("0x6")),
                    ],
                )
                .into(),
            )
            .unwrap();
        assert_eq!(
            recv(&mut rx).await,
            sample_receipt_message(1, "0x3", subscription_id)
        );
        assert_eq!(
            recv(&mut rx).await,
            sample_receipt_message(1, "0x4", subscription_id)
        );
        assert_eq!(
            recv(&mut rx).await,
            sample_receipt_message(1, "0x5", subscription_id)
        );
        assert_eq!(
            recv(&mut rx).await,
            sample_receipt_message(1, "0x6", subscription_id)
        );
        assert_recv_nothing(&mut rx).await;

        // The next pre-confirmed block is sent, we expect no receipts.
        pending_data_cache.store(sample_pre_confirmed_block(
            BlockNumber::new_or_panic(2),
            vec![
                (contract_address!("0x1"), transaction_hash!("0x7")),
                (contract_address!("0x2"), transaction_hash!("0x8")),
            ],
        ));
        assert_recv_nothing(&mut rx).await;
    }

    async fn recv(rx: &mut mpsc::Receiver<Result<Message, RpcResponse>>) -> serde_json::Value {
        let res = rx.recv().await.unwrap().unwrap();
        match res {
            Message::Text(json) => serde_json::from_str(&json).unwrap(),
            _ => panic!("Expected text message"),
        }
    }

    /// Waits for the receiver to receive nothing within a short timeout.
    /// If it receives a message, it panics.
    async fn assert_recv_nothing(rx: &mut mpsc::Receiver<Result<Message, RpcResponse>>) {
        let timeout = std::time::Duration::from_millis(100);
        tokio::time::timeout(timeout, rx.recv())
            .await
            .expect_err("Message received when none was expected");
    }

    fn sample_pre_confirmed_block(
        block_number: BlockNumber,
        txs: Vec<(ContractAddress, TransactionHash)>,
    ) -> PendingData {
        let pre_confirmed_block = PreConfirmedBlock {
            status: starknet_gateway_types::reply::Status::PreConfirmed,
            transactions: txs
                .iter()
                .map(|(sender_address, hash)| Transaction {
                    variant: TransactionVariant::DeclareV0(DeclareTransactionV0V1 {
                        sender_address: *sender_address,
                        ..Default::default()
                    }),
                    hash: *hash,
                })
                .collect(),
            transaction_receipts: txs
                .iter()
                .map(|(_sender_address, hash)| {
                    Some((
                        pathfinder_common::receipt::Receipt {
                            transaction_hash: *hash,
                            ..Default::default()
                        },
                        vec![],
                    ))
                })
                .collect(),
            transaction_state_diffs: vec![],
            ..Default::default()
        };
        PendingData::try_from_pre_confirmed_block(pre_confirmed_block.into(), block_number).unwrap()
    }

    fn sample_pre_confirmed_receipt_message(
        block_number: u64,
        hash: &str,
        subscription_id: u64,
    ) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc":"2.0",
            "method":"starknet_subscriptionNewTransactionReceipts",
            "params": {
                "result": {
                    "block_number": block_number,
                    "type": "DECLARE",
                    "transaction_hash": hash,
                    "actual_fee": {
                        "amount": "0x0",
                        "unit": "WEI"
                    },
                    "finality_status": "PRE_CONFIRMED",
                    "execution_status": "SUCCEEDED",
                    "messages_sent": [],
                    "events": [],
                    "execution_resources": {
                        "l1_gas": 0,
                        "l1_data_gas": 0,
                        "l2_gas": 0,
                    }
                },
                "subscription_id": subscription_id.to_string()
            }
        })
    }

    fn sample_receipt_message(
        block_number: u64,
        hash: &str,
        subscription_id: u64,
    ) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc":"2.0",
            "method":"starknet_subscriptionNewTransactionReceipts",
            "params": {
                "result": {
                    "block_hash": Felt::from_u64(block_number),
                    "block_number": block_number,
                    "type": "DECLARE",
                    "transaction_hash": hash,
                    "actual_fee": {
                        "amount": "0x0",
                        "unit": "WEI"
                    },
                    "finality_status": "ACCEPTED_ON_L2",
                    "execution_status": "SUCCEEDED",
                    "messages_sent": [],
                    "events": [],
                    "execution_resources": {
                        "l1_gas": 0,
                        "l1_data_gas": 0,
                        "l2_gas": 0,
                    }
                },
                "subscription_id": subscription_id.to_string()
            }
        })
    }

    fn sample_block(
        block_number: BlockNumber,
        txs: Vec<(ContractAddress, TransactionHash)>,
    ) -> L2Block {
        L2Block {
            header: sample_header(block_number.get()),
            transactions_and_receipts: txs
                .iter()
                .map(|(sender_address, hash)| {
                    let tx = Transaction {
                        variant: TransactionVariant::DeclareV0(DeclareTransactionV0V1 {
                            sender_address: *sender_address,
                            ..Default::default()
                        }),
                        hash: *hash,
                    };
                    let receipt = pathfinder_common::receipt::Receipt {
                        transaction_hash: *hash,
                        ..Default::default()
                    };

                    (tx, receipt)
                })
                .collect(),
            events: vec![vec![]; txs.len()],
            ..Default::default()
        }
    }

    fn sample_header(block_number: u64) -> BlockHeader {
        BlockHeader {
            hash: BlockHash(Felt::from_u64(block_number)),
            number: BlockNumber::new_or_panic(block_number),
            parent_hash: BlockHash::ZERO,
            ..Default::default()
        }
    }

    fn setup() -> Setup {
        let storage = StorageBuilder::in_memory().unwrap();
        {
            let mut conn = storage.connection().unwrap();
            let db = conn.transaction().unwrap();
            db.insert_block_header(&sample_header(0)).unwrap();
            db.commit().unwrap();
        }
        let pending_data_cache = std::sync::Arc::new(PendingDataCache::new());
        let notifications = Notifications::default();
        let ctx = RpcContext::for_tests()
            .with_storage(storage)
            .with_notifications(notifications.clone())
            .with_pending_data_cache(pending_data_cache.clone())
            .with_websockets(WebsocketContext::new(WebsocketHistory::Unlimited, 1024));
        let router = v09::register_routes().build(ctx);
        let (sender_tx, sender_rx) = mpsc::channel(1024);
        let (receiver_tx, receiver_rx) = mpsc::channel(1024);
        handle_json_rpc_socket(router.clone(), sender_tx, receiver_rx);
        Setup {
            tx: receiver_tx,
            rx: sender_rx,
            pending_data_cache,
            notifications,
        }
    }

    struct Setup {
        tx: mpsc::Sender<Result<Message, axum::Error>>,
        rx: mpsc::Receiver<Result<Message, RpcResponse>>,
        pending_data_cache: std::sync::Arc<PendingDataCache>,
        notifications: Notifications,
    }
}
