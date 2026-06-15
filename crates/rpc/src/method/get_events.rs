use std::collections::HashSet;
use std::str::FromStr;

use anyhow::Context;
use pathfinder_common::event::EventIndex;
use pathfinder_common::prelude::*;
use pathfinder_storage::{EventFilterError, EVENT_KEY_FILTER_LIMIT};
use tokio::task::JoinHandle;

use crate::context::RpcContext;
use crate::dto::{self, SerializeForVersion, Serializer};
use crate::pending::PendingData;
use crate::types::BlockId;
use crate::RpcVersion;

pub const EVENT_PAGE_SIZE_LIMIT: usize = 1024;

#[derive(Debug)]
pub enum GetEventsError {
    Internal(anyhow::Error),
    Custom(anyhow::Error),
    BlockNotFound,
    PageSizeTooBig,
    InvalidContinuationToken,
    TooManyKeysInFilter { limit: usize, requested: usize },
}

impl From<anyhow::Error> for GetEventsError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}

crate::error::impl_from_pending_read_error!(GetEventsError);

impl From<GetEventsError> for crate::error::ApplicationError {
    fn from(e: GetEventsError) -> Self {
        match e {
            GetEventsError::Internal(internal) => Self::Internal(internal),
            GetEventsError::Custom(internal) => Self::Custom(internal),
            GetEventsError::BlockNotFound => Self::BlockNotFound,
            GetEventsError::PageSizeTooBig => Self::PageSizeTooBig,
            GetEventsError::InvalidContinuationToken => Self::InvalidContinuationToken,
            GetEventsError::TooManyKeysInFilter { limit, requested } => {
                Self::TooManyKeysInFilter { limit, requested }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetEventsInput {
    filter: EventFilter,
}

impl crate::dto::DeserializeForVersion for GetEventsInput {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                filter: value.deserialize("filter")?,
            })
        })
    }
}

/// Contains event filter parameters passed to `starknet_getEvents`.
#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct EventFilter {
    pub from_block: Option<BlockId>,
    pub to_block: Option<BlockId>,
    pub addresses: HashSet<ContractAddress>,
    pub keys: Vec<Vec<EventKey>>,
    pub chunk_size: usize,
    /// Offset, measured in events, which points to the requested chunk
    pub continuation_token: Option<String>,
}

impl EventFilter {
    fn get_addresses(&self) -> Vec<ContractAddress> {
        let mut addresses: Vec<ContractAddress> = self.addresses.iter().cloned().collect();
        addresses.sort();
        addresses
    }
}

impl crate::dto::DeserializeForVersion for EventFilter {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let version = value.version;
        value.deserialize_map(|value| {
            let raw_addresses = if version >= RpcVersion::V10 {
                value.deserialize_optional_array_or_scalar("address", |v| v.deserialize())?
            } else {
                let mut opt_address = vec![];
                if let Some(addr) = value.deserialize_optional("address")? {
                    opt_address.push(addr);
                }

                opt_address
            };

            Ok(Self {
                from_block: value.deserialize_optional("from_block")?,
                to_block: value.deserialize_optional("to_block")?,
                addresses: HashSet::from_iter(raw_addresses.into_iter().map(ContractAddress)),
                keys: value
                    .deserialize_optional_array("keys", |value| {
                        value.deserialize_array(|value| value.deserialize().map(EventKey))
                    })?
                    .unwrap_or_default(),
                chunk_size: value.deserialize("chunk_size")?,
                continuation_token: value.deserialize_optional_serde("continuation_token")?,
            })
        })
    }
}

/// Returns events matching the specified filter
pub async fn get_events(
    context: RpcContext,
    input: GetEventsInput,
    rpc_version: RpcVersion,
) -> Result<GetEventsResult, GetEventsError> {
    // The [Block::PreConfirmed] in ranges makes things quite complicated. This
    // implementation splits the ranges into the following buckets:
    //
    // 1. pre-confirmed     :     pre-confirmed -> query pre-confirmed only
    // 2. pre-confirmed     : non-pre-confirmed -> return empty result
    // 3. non-pre-confirmed : non-pre-confirmed -> query db only
    // 4. non-pre-confirmed :     pre-confirmed -> query db and potentially append
    //    pending events
    //
    // The database query for 3 and 4 is combined into one step.
    //
    // 4 requires some additional logic to handle some edge cases:
    //  a) if from_block_number > pre_confirmed_block_number -> return empty result
    //  b) Query database
    //  c) if full page -> return page
    //      check if there are matching events in the pre-confirmed block
    //      and return a continuation token for the pre-confirmed block
    //  d) else if empty / partially full -> append events from start of
    //      pre-confirmed
    //      if there are more pre-confirmed events return a continuation token
    //      with the appropriate offset within the pre-confirmed block

    use BlockId::*;

    let mut request = input.filter;

    let request_ct = request
        .continuation_token
        .as_ref()
        .map(|s| {
            s.parse::<ContinuationToken>()
                .map_err(|_| GetEventsError::InvalidContinuationToken)
        })
        .transpose()?;

    if request.keys.len() > EVENT_KEY_FILTER_LIMIT {
        return Err(GetEventsError::TooManyKeysInFilter {
            limit: EVENT_KEY_FILTER_LIMIT,
            requested: request.keys.len(),
        });
    }
    if request.chunk_size > EVENT_PAGE_SIZE_LIMIT {
        return Err(GetEventsError::PageSizeTooBig);
    }

    let storage = context.storage.clone();

    // truncate empty key lists from the end of the key filter
    if let Some(last_non_empty) = request.keys.iter().rposition(|keys| !keys.is_empty()) {
        request.keys.truncate(last_non_empty + 1);
    }

    // blocking task to perform database event query
    let span = tracing::Span::current();
    let db_events: JoinHandle<Result<_, GetEventsError>> = util::task::spawn_blocking(move |_| {
        let _g = span.enter();
        let mut connection = storage
            .connection()
            .context("Opening database connection")?;

        let transaction = connection
            .transaction()
            .context("Creating database transaction")?;

        let requires_pre_confirmed = matches!(request.from_block, Some(PreConfirmed))
            || matches!(request.to_block, Some(PreConfirmed));

        // Missing pending data only errs when the request explicitly asked for it.
        let pending: Option<PendingData> = if requires_pre_confirmed {
            Some(context.pending_data.get(&transaction, rpc_version)?)
        } else {
            context
                .pending_data
                .get_optional(&transaction, rpc_version)?
        };

        // Replace from/to blocks with `BlockId::PreConfirmed` if their numbers match
        // the pre-latest/pre-confirmed block number.
        let from_block_id = request.from_block.map(|from_id| match from_id {
            Number(from)
                if pending
                    .as_ref()
                    .is_some_and(|p| p.is_pre_latest_or_pre_confirmed(from)) =>
            {
                PreConfirmed
            }
            _ => from_id,
        });
        let to_block_id = request.to_block.map(|to_id| match to_id {
            Number(to)
                if pending
                    .as_ref()
                    .is_some_and(|p| p.is_pre_latest_or_pre_confirmed(to)) =>
            {
                PreConfirmed
            }
            _ => to_id,
        });

        // Handle the trivial (1), (2) and (4a) cases — all of which need pending data,
        // which a pre-confirmed bound guarantees is present.
        if let Some(pending) = pending.as_ref() {
            match (&from_block_id, &to_block_id) {
                (Some(PreConfirmed), to) => {
                    if matches!(to, Some(PreConfirmed) | None) {
                        let (pending_events, pending_ct) = get_pending_events(
                            pending,
                            request.chunk_size,
                            &request.keys,
                            &request.addresses,
                            request_ct,
                        )?;
                        return Ok(GetEventsResult {
                            events: pending_events,
                            continuation_token: pending_ct.map(|ct| ct.to_string()),
                        });
                    } else {
                        return Ok(GetEventsResult {
                            events: Vec::new(),
                            continuation_token: None,
                        });
                    }
                }
                (Some(Number(from_block)), Some(PreConfirmed))
                    if from_block > &pending.pre_confirmed_block_number() =>
                {
                    return Ok(GetEventsResult {
                        events: Vec::new(),
                        continuation_token: None,
                    });
                }
                _ => {}
            }
        }

        let from_block = map_from_block_to_number(&transaction, from_block_id)?;
        let to_block = map_to_block_to_number(&transaction, to_block_id)?;

        match (from_block, to_block) {
            (Some(from), Some(to)) if from > to => {
                return Ok(GetEventsResult {
                    events: Vec::new(),
                    continuation_token: None,
                })
            }
            _ => {}
        }

        // Handle cases (3) and (4) where `from_block` is non-pending.

        let (from_block, requested_offset) = match request_ct {
            Some(token) if from_block.is_some_and(|from| from > token.block_number) => {
                return Err(GetEventsError::InvalidContinuationToken)
            }
            Some(token) => (Some(token.block_number), token.offset),
            None => (from_block, 0),
        };

        let constraints = pathfinder_storage::EventConstraints {
            from_block,
            to_block,
            contract_addresses: request.get_addresses(),
            keys: request.keys.clone(),
            page_size: request.chunk_size,
            offset: requested_offset,
        };

        // Fetch events from DB and append pending events if needed.
        let page = transaction
            .events(
                &constraints,
                context.config.get_events_event_filter_block_range_limit,
            )
            .map_err(|e| match e {
                EventFilterError::Internal(e) => GetEventsError::Internal(e),
                EventFilterError::PageSizeTooSmall => GetEventsError::Custom(e.into()),
            })?;

        let mut events: Vec<_> = page.events.into_iter().map(|e| e.into()).collect();
        let db_ct = page.continuation_token.map(|ct| ContinuationToken {
            block_number: ct.block_number,
            offset: ct.offset,
        });

        // TODO: Verify the added `| None` in review.
        let append_from_pending =
            db_ct.is_none() && matches!(to_block_id, Some(PreConfirmed) | None);

        // Append pending events only when there is a pending block to draw from.
        let continuation_token = match pending.as_ref() {
            Some(pending) if append_from_pending => {
                if events.len() < request.chunk_size {
                    let amount_to_take_from_pending = request.chunk_size - events.len();
                    let (pending_events, pending_ct) = get_pending_events(
                        pending,
                        amount_to_take_from_pending,
                        &request.keys,
                        &request.addresses,
                        request_ct,
                    )?;
                    events.extend(pending_events);
                    pending_ct
                } else {
                    // We have a full page from the database, but there might be more pending
                    // events. Return a continuation token for the pending block.
                    let pending_block = pending
                        .pre_latest_block_number()
                        .unwrap_or_else(|| pending.pre_confirmed_block_number());
                    Some(ContinuationToken {
                        block_number: pending_block,
                        offset: 0,
                    })
                }
            }
            _ => db_ct,
        };

        Ok(GetEventsResult {
            events,
            continuation_token: continuation_token.map(|ct| ct.to_string()),
        })
    });

    db_events
        .await
        .context("Database read panic or shutting down")?
}

/// Get as many pending events as possible (up to `max_amount`) that match the
/// given [EventFilter].
///
/// Produces an optional [ContinuationToken] if there are more pending events
/// that match the filter. Takes in an optional continuation token that has been
/// presumably generated by an earlier call of `starknet_getEvents`.
fn get_pending_events(
    pending: &PendingData,
    max_amount: usize,
    keys: &[Vec<EventKey>],
    addresses: &HashSet<ContractAddress>,
    continuation_token: Option<ContinuationToken>,
) -> Result<(Vec<EmittedEvent>, Option<ContinuationToken>), GetEventsError> {
    let keys: Vec<std::collections::HashSet<_>> = keys
        .iter()
        .map(|keys| keys.iter().copied().collect())
        .collect();

    let pending_block = pending.pre_confirmed_block_number();

    // If we have a continuation token and it points to a pre-latest/pending block,
    // we use its values. Otherwise we take whatever events we have from pending
    // data, if the continuation token is valid (not pointing past pending block).
    let (start_block, start_offset) = match continuation_token {
        Some(ct) if ct.block_number > pending_block => {
            return Err(GetEventsError::InvalidContinuationToken)
        }
        Some(ct) if pending.is_pre_latest_or_pre_confirmed(ct.block_number) => {
            (ct.block_number, ct.offset)
        }
        _ => (
            pending.pre_latest_block_number().unwrap_or(pending_block),
            0,
        ),
    };

    let mut events = Vec::new();

    let new_continuation_token = match pending.pre_latest_block() {
        Some(pre_latest_block) if pre_latest_block.number == start_block => {
            // Fetch from pre-latest and pre-confirmed.
            let pre_latest_events_exhausted = match_and_fill_events(
                &pre_latest_block.transaction_receipts,
                &mut events,
                start_offset,
                max_amount,
                &keys,
                addresses,
                pre_latest_block.number,
            );

            let taken_from_pre_latest = events.len();

            if taken_from_pre_latest == max_amount {
                let continuation_token = if pre_latest_events_exhausted {
                    // We exhausted the pre-latest block but there might be more events in
                    // the pending block.
                    ContinuationToken {
                        block_number: pending_block,
                        offset: 0,
                    }
                } else {
                    // We've filled up a page but still have more events in the pre-latest
                    // block.
                    ContinuationToken {
                        block_number: pre_latest_block.number,
                        offset: start_offset + max_amount,
                    }
                };

                return Ok((events, Some(continuation_token)));
            }

            let amount_to_take = max_amount - taken_from_pre_latest;

            let pending_events_exhausted = match_and_fill_events(
                pending.pre_confirmed_tx_receipts_and_events(),
                &mut events,
                // Continuation token was used on pre-latest block, no offset for pending.
                0,
                amount_to_take,
                &keys,
                addresses,
                pending_block,
            );

            if pending_events_exhausted {
                None
            } else {
                let offset_in_pending = events.len() - taken_from_pre_latest;
                // We filled up a page but still have more events in the pending block.
                let continuation_token = ContinuationToken {
                    block_number: pending_block,
                    offset: offset_in_pending,
                };
                Some(continuation_token)
            }
        }
        _ => {
            // Fetch from pending/pre-confirmed block only.
            let pending_events_exhausted = match_and_fill_events(
                pending.pre_confirmed_tx_receipts_and_events(),
                &mut events,
                start_offset,
                max_amount,
                &keys,
                addresses,
                pending_block,
            );

            if pending_events_exhausted {
                None
            } else {
                // We filled up a page but still have more events in the pending block.
                let continuation_token = ContinuationToken {
                    block_number: pending_block,
                    offset: start_offset + events.len(),
                };
                Some(continuation_token)
            }
        }
    };

    Ok((events, new_continuation_token))
}

// Maps `to_block` BlockId to a block number which can be used by the events
// query.
//
// This block id specifies the upper end of the range, so pending/latest/None
// means there's no upper limit.
fn map_to_block_to_number(
    tx: &pathfinder_storage::Transaction<'_>,
    block: Option<BlockId>,
) -> Result<Option<BlockNumber>, GetEventsError> {
    use BlockId::*;

    match block {
        Some(Hash(hash)) => {
            let number = tx
                .block_id(hash.into())
                .context("Querying block number")?
                .ok_or(GetEventsError::BlockNotFound)?
                .0;

            Ok(Some(number))
        }
        Some(Number(number)) => Ok(Some(number)),
        Some(L1Accepted) => {
            let number = tx
                .l1_l2_pointer()
                .context("Querying L1-L2 pointer")?
                .ok_or(GetEventsError::BlockNotFound)?;

            Ok(Some(number))
        }
        Some(PreConfirmed) | Some(Latest) | None => Ok(None),
    }
}

// Maps `from_block` BlockId to a block number which can be used by the events
// query.
//
// This block id specifies the lower end of the range, so pending/latest means
// a lower limit here.
fn map_from_block_to_number(
    tx: &pathfinder_storage::Transaction<'_>,
    block: Option<BlockId>,
) -> Result<Option<BlockNumber>, GetEventsError> {
    use BlockId::*;

    match block {
        Some(Hash(hash)) => {
            let number = tx
                .block_id(hash.into())
                .context("Querying block number")?
                .ok_or(GetEventsError::BlockNotFound)?
                .0;

            Ok(Some(number))
        }
        Some(Number(number)) => Ok(Some(number)),
        Some(L1Accepted) => {
            let number = tx
                .l1_l2_pointer()
                .context("Querying L1-L2 pointer")?
                .ok_or(GetEventsError::BlockNotFound)?;

            Ok(Some(number))
        }
        Some(PreConfirmed) | Some(Latest) => {
            let number = tx
                .block_id(pathfinder_common::BlockId::Latest)
                .context("Querying latest block number")?
                .ok_or(GetEventsError::BlockNotFound)?
                .0;
            Ok(Some(number))
        }
        None => Ok(None),
    }
}

/// Appends up to `max_amount` events from `src` to `dst` based on the filter
/// requirements.
///
/// Returns whether all events from `src` that match the filter have been
/// exhausted.
fn match_and_fill_events(
    src: &[(
        pathfinder_common::receipt::Receipt,
        Vec<pathfinder_common::event::Event>,
    )],
    dst: &mut Vec<EmittedEvent>,
    skip: usize,
    max_amount: usize,
    keys: &[std::collections::HashSet<EventKey>],
    addresses: &HashSet<ContractAddress>,
    block_number: BlockNumber,
) -> bool {
    let original_len = dst.len();

    let key_filter_is_empty = keys.iter().flatten().count() == 0;

    let pending_events = src
        .iter()
        .flat_map(|(receipt, events)| {
            events.iter().zip(
                std::iter::repeat((receipt.transaction_hash, receipt.transaction_index))
                    .enumerate(),
            )
        })
        .filter(|(event, _)| {
            if addresses.is_empty() {
                true
            } else {
                addresses.contains(&event.from_address)
            }
        })
        .filter(|(event, _)| {
            if key_filter_is_empty {
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
        })
        .skip(skip)
        // We need to take an extra event to determine the return value.
        .take(max_amount + 1)
        .map(|(event, (idx, tx_info))| EmittedEvent {
            data: event.data.clone(),
            keys: event.keys.clone(),
            from_address: event.from_address,
            block_hash: None,
            block_number: Some(block_number),
            transaction_hash: tx_info.0,
            transaction_index: tx_info.1,
            event_index: EventIndex(idx as u64),
        });

    dst.extend(pending_events);
    let amount_exceeded = dst.len() > (original_len + max_amount);
    if amount_exceeded {
        debug_assert_eq!(
            dst.len(),
            original_len + max_amount + 1,
            "Amount should be exceeded by the extra event"
        );
        dst.pop();
    }

    !amount_exceeded
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ContinuationToken {
    block_number: BlockNumber,
    offset: usize,
}

impl FromStr for ContinuationToken {
    type Err = ParseContinuationTokenError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((block_number, offset)) = s.split_once('-') {
            let block_number = block_number
                .parse::<u64>()
                .map_err(|_| ParseContinuationTokenError)?;
            let offset = offset.parse().map_err(|_| ParseContinuationTokenError)?;

            let block_number = BlockNumber::new(block_number).ok_or(ParseContinuationTokenError)?;

            Ok(ContinuationToken {
                block_number,
                offset,
            })
        } else {
            Err(ParseContinuationTokenError)
        }
    }
}

impl std::fmt::Display for ContinuationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.block_number.get(), self.offset)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ParseContinuationTokenError;

/// Describes an emitted event returned by starknet_getEvents
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmittedEvent {
    pub data: Vec<EventData>,
    pub keys: Vec<EventKey>,
    pub from_address: ContractAddress,
    /// [`None`] for pending events.
    pub block_hash: Option<BlockHash>,
    /// [`None`] for pending events.
    pub block_number: Option<BlockNumber>,
    pub transaction_hash: TransactionHash,
    pub transaction_index: TransactionIndex,
    pub event_index: EventIndex,
}

impl From<pathfinder_storage::EmittedEvent> for EmittedEvent {
    fn from(event: pathfinder_storage::EmittedEvent) -> Self {
        Self {
            data: event.data,
            keys: event.keys,
            from_address: event.from_address,
            block_hash: Some(event.block_hash),
            block_number: Some(event.block_number),
            transaction_hash: event.transaction_hash,
            transaction_index: event.transaction_index,
            event_index: event.event_index,
        }
    }
}

// Result type for starknet_getEvents
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetEventsResult {
    pub events: Vec<EmittedEvent>,
    /// Offset, measured in events, which points to the chunk that follows
    /// currently requested chunk (`events`)
    pub continuation_token: Option<String>,
}

impl SerializeForVersion for EmittedEvent {
    fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
        let mut serializer = serializer.serialize_struct()?;

        serializer.serialize_iter("data", self.data.len(), &mut self.data.iter().map(|d| d.0))?;
        serializer.serialize_iter("keys", self.keys.len(), &mut self.keys.iter().map(|d| d.0))?;
        serializer.serialize_field("from_address", &self.from_address)?;
        serializer.serialize_optional("block_hash", self.block_hash)?;
        serializer.serialize_optional("block_number", self.block_number)?;
        serializer.serialize_field("transaction_hash", &self.transaction_hash)?;
        if serializer.version >= RpcVersion::V10 {
            serializer.serialize_field("transaction_index", &self.transaction_index.get())?;
            serializer.serialize_field("event_index", &self.event_index.0)?;
        }

        serializer.end()
    }
}

impl SerializeForVersion for &'_ EmittedEvent {
    fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
        (*self).serialize(serializer)
    }
}

impl SerializeForVersion for GetEventsResult {
    fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
        let mut serializer = serializer.serialize_struct()?;

        serializer.serialize_iter("events", self.events.len(), &mut self.events.iter())?;
        serializer.serialize_optional("continuation_token", self.continuation_token.clone())?;

        serializer.end()
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_crypto::Felt;
    use pathfinder_storage::test_utils;
    use pretty_assertions_sorted::assert_eq;
    use serde_json::json;

    use super::{EmittedEvent, GetEventsResult, *};
    use crate::dto::DeserializeForVersion;
    use crate::RpcVersion;

    fn make_contract_address_filter(addr: &str) -> HashSet<ContractAddress> {
        let f = Felt::from_hex_str(addr).expect("test address to be valid");
        wrap_contract_address_filter(ContractAddress(f))
    }

    fn wrap_contract_address_filter(addr: ContractAddress) -> HashSet<ContractAddress> {
        let mut hs = HashSet::new();
        hs.insert(addr);
        hs
    }

    #[rstest::rstest]
    #[case::positional_with_optionals(json!([{
        "from_block":{"block_number":0},
        "to_block":"latest",
        "address":"0x1",
        "keys":[["0x2"],[]],
        "chunk_size":3,
        "continuation_token":"4"}]), true
    )]
    #[case::named_with_optionals(json!({"filter":{
        "from_block":{"block_number":0},
        "to_block":"latest",
        "address":"0x1","keys":[["0x2"],[]],
        "chunk_size":3,
        "continuation_token":"4"}}), true
    )]
    #[case::positional_without_optionals(json!([{"chunk_size":5}]), false)]
    #[case::named_without_optionals(json!({"filter":{"chunk_size":5}}), false)]
    fn parsing(#[case] input: serde_json::Value, #[case] with_optionals: bool) {
        let filter = if with_optionals {
            EventFilter {
                from_block: Some(BlockId::Number(BlockNumber::new_or_panic(0))),
                to_block: Some(BlockId::Latest),
                addresses: make_contract_address_filter("0x1"),
                keys: vec![vec![event_key!("0x2")], vec![]],
                chunk_size: 3,
                continuation_token: Some("4".to_string()),
            }
        } else {
            EventFilter {
                chunk_size: 5,
                ..Default::default()
            }
        };
        let expected = GetEventsInput { filter };

        let input =
            GetEventsInput::deserialize(crate::dto::Value::new(input, RpcVersion::V09)).unwrap();
        assert_eq!(input, expected);
    }

    #[test]
    fn parsing_single_address() {
        let input = json!({
            "filter": {
                "from_block": {"block_number": 0},
                "to_block": {"block_number": 1000},
                "address": "0x17c378e4fa718fd3405324eee83c5c7c515d72010fb30977b08b84b0fa217a9",
                "chunk_size": 1024
            }
        });

        let filter = EventFilter {
            from_block: Some(BlockId::Number(BlockNumber::new_or_panic(0))),
            to_block: Some(BlockId::Number(BlockNumber::new_or_panic(1000))),
            addresses: make_contract_address_filter(
                "0x17c378e4fa718fd3405324eee83c5c7c515d72010fb30977b08b84b0fa217a9",
            ),
            chunk_size: 1024,
            ..Default::default()
        };
        let expected = GetEventsInput { filter };

        let input =
            GetEventsInput::deserialize(crate::dto::Value::new(input, RpcVersion::V10)).unwrap();
        assert_eq!(input, expected);
    }

    #[rstest::rstest]
    #[case::positional(json!([{
        "address": ["0x10", "0x20"],
        "chunk_size": 5
    }]))]
    #[case::named(json!({"filter":{
        "address": ["0x20", "0x10"],
        "chunk_size": 5
    }}))]
    fn parsing_multiple_addresses(#[case] input: serde_json::Value) {
        let mut addresses = HashSet::new();
        addresses.insert(contract_address!("0x10"));
        addresses.insert(contract_address!("0x20"));
        let filter = EventFilter {
            addresses,
            chunk_size: 5,
            ..Default::default()
        };
        let expected = GetEventsInput { filter };

        let input =
            GetEventsInput::deserialize(crate::dto::Value::new(input, RpcVersion::V10)).unwrap();
        assert_eq!(input, expected);
    }

    #[test]
    fn continuation_token() {
        use assert_matches::assert_matches;

        assert_matches!(
            "1234".parse::<ContinuationToken>(),
            Err(ParseContinuationTokenError)
        );
        assert_matches!(
            "invalid".parse::<ContinuationToken>(),
            Err(ParseContinuationTokenError)
        );
        assert_matches!(
            "1234-5678-9012".parse::<ContinuationToken>(),
            Err(ParseContinuationTokenError)
        );
        assert_matches!(
            "-1234-5678".parse::<ContinuationToken>(),
            Err(ParseContinuationTokenError)
        );

        assert_eq!(
            "1234-4567".parse::<ContinuationToken>().unwrap(),
            ContinuationToken {
                block_number: BlockNumber::new_or_panic(1234),
                offset: 4567
            }
        );
    }

    fn setup() -> (RpcContext, Vec<EmittedEvent>) {
        let (storage, test_data) = test_utils::setup_test_storage();
        let events = test_data
            .events
            .into_iter()
            .map(EmittedEvent::from)
            .collect();
        let context = RpcContext::for_tests().with_storage(storage);

        (context, events)
    }

    impl PartialEq for GetEventsError {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (Self::Internal(l), Self::Internal(r)) => l.to_string() == r.to_string(),
                _ => core::mem::discriminant(self) == core::mem::discriminant(other),
            }
        }
    }

    const RPC_VERSION: RpcVersion = RpcVersion::V09;

    #[tokio::test]
    async fn get_events_with_empty_filter() {
        let (context, events) = setup();

        let input = GetEventsInput {
            filter: EventFilter {
                chunk_size: test_utils::NUM_EVENTS,
                ..Default::default()
            },
        };
        let result = get_events(context, input, RPC_VERSION).await.unwrap();

        assert_eq!(
            result,
            GetEventsResult {
                events,
                continuation_token: Some("4-0".to_string()),
            }
        );
    }

    #[tokio::test]
    async fn get_events_with_fully_specified_filter() {
        let (context, events) = setup();

        let expected_event = &events[1];
        let expected_result = GetEventsResult {
            events: vec![expected_event.clone()],
            continuation_token: None,
        };
        let input = GetEventsInput {
            filter: EventFilter {
                from_block: Some(expected_event.block_number.unwrap().into()),
                to_block: Some(expected_event.block_number.unwrap().into()),
                addresses: wrap_contract_address_filter(expected_event.from_address),
                // we're using a key which is present in _all_ events
                keys: vec![vec![], vec![event_key!("0xdeadbeef")]],
                chunk_size: test_utils::NUM_EVENTS,
                continuation_token: None,
            },
        };
        let result = get_events(context.clone(), input.clone(), RPC_VERSION)
            .await
            .unwrap();
        assert_eq!(result, expected_result);
    }

    #[tokio::test]
    async fn get_events_by_block() {
        let (context, events) = setup();

        const BLOCK_NUMBER: usize = 2;
        let input = GetEventsInput {
            filter: EventFilter {
                from_block: Some(BlockNumber::new_or_panic(BLOCK_NUMBER as u64).into()),
                to_block: Some(BlockNumber::new_or_panic(BLOCK_NUMBER as u64).into()),
                chunk_size: test_utils::NUM_EVENTS,
                ..Default::default()
            },
        };

        let result = get_events(context, input, RPC_VERSION).await.unwrap();

        let expected_events = &events[test_utils::EVENTS_PER_BLOCK * BLOCK_NUMBER
            ..test_utils::EVENTS_PER_BLOCK * (BLOCK_NUMBER + 1)];
        assert_eq!(
            result,
            GetEventsResult {
                events: expected_events.to_vec(),
                continuation_token: None,
            }
        );
    }

    #[tokio::test]
    async fn get_events_from_latest_block() {
        let (context, events) = setup();

        const LATEST_BLOCK_NUMBER: usize = 3;
        let input = GetEventsInput {
            filter: EventFilter {
                from_block: Some(BlockId::Latest),
                to_block: Some(BlockId::Latest),
                chunk_size: test_utils::NUM_EVENTS,
                ..Default::default()
            },
        };

        let result = get_events(context, input, RPC_VERSION).await.unwrap();

        let expected_events = &events[test_utils::EVENTS_PER_BLOCK * LATEST_BLOCK_NUMBER
            ..test_utils::EVENTS_PER_BLOCK * (LATEST_BLOCK_NUMBER + 1)];
        assert_eq!(
            result,
            GetEventsResult {
                events: expected_events.to_vec(),
                continuation_token: None,
            }
        );
    }

    #[tokio::test]
    async fn get_events_with_invalid_page_size() {
        let (context, _) = setup();

        let input = GetEventsInput {
            filter: EventFilter {
                chunk_size: pathfinder_storage::EVENT_PAGE_SIZE_LIMIT + 1,
                ..Default::default()
            },
        };
        let error = get_events(context, input, RPC_VERSION).await.unwrap_err();

        assert_eq!(GetEventsError::PageSizeTooBig, error);
    }

    #[tokio::test]
    async fn get_events_with_too_many_keys_in_filter() {
        let (context, _) = setup();

        let limit = EVENT_KEY_FILTER_LIMIT;

        let keys = [vec![event_key!("0x01")]]
            .iter()
            .cloned()
            .cycle()
            .take(limit + 1)
            .collect::<Vec<_>>();

        let input = GetEventsInput {
            filter: EventFilter {
                keys,
                chunk_size: 10,
                ..Default::default()
            },
        };
        let error = get_events(context, input, RPC_VERSION).await.unwrap_err();

        assert_eq!(
            GetEventsError::TooManyKeysInFilter {
                limit,
                requested: limit + 1
            },
            error
        );
    }

    #[tokio::test]
    async fn get_events_from_block_greater_than_to_block_returns_empty_page() {
        let (context, _) = setup();

        let input = GetEventsInput {
            filter: EventFilter {
                from_block: Some(BlockId::Number(BlockNumber::new_or_panic(3))),
                to_block: Some(BlockId::Number(BlockNumber::new_or_panic(1))),
                ..Default::default()
            },
        };
        let result = get_events(context, input, RPC_VERSION).await.unwrap();

        assert_eq!(
            GetEventsResult {
                events: vec![],
                continuation_token: None,
            },
            result
        );
    }

    #[tokio::test]
    async fn get_events_by_key_with_paging() {
        let (context, events) = setup();

        let expected_events = &events[27..33];
        let keys_for_expected_events: Vec<Vec<_>> =
            vec![expected_events.iter().map(|e| e.keys[0]).collect()];

        let input = GetEventsInput {
            filter: EventFilter {
                keys: keys_for_expected_events.clone(),
                chunk_size: 1,
                ..Default::default()
            },
        };
        let result = get_events(context.clone(), input, RPC_VERSION)
            .await
            .unwrap();
        assert_eq!(
            result,
            GetEventsResult {
                events: expected_events[..1].to_vec(),
                continuation_token: Some("0-1".to_string()),
            }
        );

        let input = GetEventsInput {
            filter: EventFilter {
                keys: keys_for_expected_events.clone(),
                chunk_size: 2,
                continuation_token: Some("0-1".to_string()),
                ..Default::default()
            },
        };
        let result = get_events(context.clone(), input, RPC_VERSION)
            .await
            .unwrap();
        assert_eq!(
            result,
            GetEventsResult {
                events: expected_events[1..3].to_vec(),
                continuation_token: Some("3-0".to_string()),
            }
        );

        let input = GetEventsInput {
            filter: EventFilter {
                keys: keys_for_expected_events.clone(),
                chunk_size: 3,
                continuation_token: Some("3-0".to_string()),
                ..Default::default()
            },
        };
        let result = get_events(context.clone(), input, RPC_VERSION)
            .await
            .unwrap();
        assert_eq!(
            result,
            GetEventsResult {
                events: expected_events[3..].to_vec(),
                continuation_token: Some("4-0".to_string()),
            }
        );
        let input = GetEventsInput {
            filter: EventFilter {
                keys: keys_for_expected_events.clone(),
                chunk_size: 1,
                continuation_token: Some("4-0".to_string()),
                ..Default::default()
            },
        };
        let result = get_events(context.clone(), input, RPC_VERSION)
            .await
            .unwrap();
        assert_eq!(
            result,
            GetEventsResult {
                events: vec![],
                continuation_token: None,
            }
        );

        // nonexistent page
        let input = GetEventsInput {
            filter: EventFilter {
                keys: keys_for_expected_events.clone(),
                chunk_size: 1,
                // Offset pointing to after the last event
                continuation_token: Some("2-6".to_string()),
                ..Default::default()
            },
        };
        let result = get_events(context, input, RPC_VERSION).await.unwrap();
        assert_eq!(result.events, &[]);
        assert_eq!(result.continuation_token, None);
    }

    mod pending {
        use pretty_assertions_sorted::assert_eq;

        use super::*;

        fn unavailable_cache() -> std::sync::Arc<pathfinder_pending_data::PendingDataCache> {
            let cache = std::sync::Arc::new(pathfinder_pending_data::PendingDataCache::new());
            cache.mark_unavailable("syncing");
            cache
        }

        #[tokio::test]
        async fn db_range_succeeds_when_pending_unavailable() {
            let (storage, _) = pathfinder_storage::test_utils::setup_test_storage();
            let context = RpcContext::for_tests()
                .with_storage(storage)
                .with_pending_data_cache(unavailable_cache());

            // Range bounded entirely within the database. Never touches pending
            // data, so an unavailable cache must not turn it into an error.
            let input = GetEventsInput {
                filter: EventFilter {
                    from_block: Some(BlockId::Number(BlockNumber::new_or_panic(0))),
                    to_block: Some(BlockId::Latest),
                    chunk_size: 1024,
                    ..Default::default()
                },
            };
            let result = get_events(context, input, RPC_VERSION).await.unwrap();
            assert!(!result.events.is_empty());
        }

        #[tokio::test]
        async fn explicit_pre_confirmed_errors_when_unavailable() {
            let context = RpcContext::for_tests().with_pending_data_cache(unavailable_cache());

            // Explicitly requesting pre-confirmed makes the data mandatory: an unavailable
            // cache should error here.
            let input = GetEventsInput {
                filter: EventFilter {
                    from_block: Some(BlockId::PreConfirmed),
                    to_block: Some(BlockId::PreConfirmed),
                    chunk_size: 1024,
                    ..Default::default()
                },
            };
            let error = get_events(context, input, RPC_VERSION).await.unwrap_err();
            assert!(matches!(error, GetEventsError::Custom(_)));
        }

        #[tokio::test]
        async fn backward_range() {
            let context = RpcContext::for_tests_with_pre_confirmed().await;

            let input = GetEventsInput {
                filter: EventFilter {
                    from_block: Some(BlockId::PreConfirmed),
                    to_block: Some(BlockId::Latest),
                    chunk_size: 100,
                    ..Default::default()
                },
            };
            let result = get_events(context, input, RPC_VERSION).await.unwrap();
            assert!(result.events.is_empty());
        }

        #[tokio::test]
        async fn all_events() {
            let context = RpcContext::for_tests_with_pre_confirmed().await;

            let mut input = GetEventsInput {
                filter: EventFilter {
                    to_block: Some(BlockId::Latest),
                    chunk_size: 1024,
                    ..Default::default()
                },
            };

            let events = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert_eq!(events.events.len(), 1);

            input.filter.from_block = Some(BlockId::PreConfirmed);
            input.filter.to_block = Some(BlockId::PreConfirmed);
            let pending_events = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert_eq!(pending_events.events.len(), 3);

            input.filter.from_block = None;
            let all_events = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();

            let expected = events
                .events
                .into_iter()
                .chain(pending_events.events.into_iter())
                .collect::<Vec<_>>();

            assert_eq!(all_events.events, expected);
            assert!(all_events.continuation_token.is_none());
        }

        #[tokio::test]
        async fn paging() {
            let context = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;

            let mut input = GetEventsInput {
                filter: EventFilter {
                    to_block: Some(BlockId::PreConfirmed),
                    chunk_size: 1024,
                    ..Default::default()
                },
            };

            // Block 0 has a single event. Blocks, 1 and 2 have no events. Pre-latest block
            // (3 in this case) has 3 events. Pre-confirmed block (4) also has 3 events.
            let all = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap()
                .events;

            // Check edge case where the page is full with events from the DB but this was
            // the last page from the DB -- should continue from offset 0 of the pre-latest
            // block next time.
            input.filter.chunk_size = 1;
            input.filter.continuation_token = None;
            let result = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert_eq!(result.events, &all[0..1]);
            assert_eq!(result.continuation_token, Some("3-0".to_string()));

            // Check edge case where the page is full with events from the pre-latest block
            // - should continue from offset 0 of the pre-confirmed block next time.
            input.filter.chunk_size = 3;
            input.filter.continuation_token = result.continuation_token;
            let result = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert_eq!(result.events, &all[1..4]);
            assert_eq!(result.continuation_token, Some("4-0".to_string()));

            // Page includes a DB event and an event from the pre-latest block, but there
            // are more two events in this block for the next page.
            input.filter.chunk_size = 2;
            input.filter.continuation_token = None;
            let result = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert_eq!(result.events, &all[0..2]);
            assert_eq!(result.continuation_token, Some("3-1".to_string()));

            // Take the remaining events from the pre-latest block and one from
            // pre-confirmed. There are two more events in pre-confirmed for the
            // next page.
            input.filter.chunk_size = 3;
            input.filter.continuation_token = result.continuation_token;
            let result = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert_eq!(result.events, &all[2..5]);
            assert_eq!(result.continuation_token, Some("4-1".to_string()));

            // Only two events remain though.
            input.filter.chunk_size = 128;
            input.filter.continuation_token = result.continuation_token;
            let result = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert_eq!(result.events, &all[5..7]);
            assert_eq!(result.continuation_token, None);

            // Continuation token for a page that does exist, should return all events (even
            // from pre-latest/pre-confirmed) with sufficient page size.
            input.filter.chunk_size = 128;
            input.filter.continuation_token = Some("0-0".to_string());
            let result = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert_eq!(result.events, all);
            assert_eq!(result.continuation_token, None);

            // Non-existent page in pre-latest block - offset too large. Should return
            // pre-confirmed block events.
            input.filter.chunk_size = 128;
            input.filter.continuation_token = Some("3-3".to_string());
            let result = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert_eq!(result.events, &all[4..7]);
            assert_eq!(result.continuation_token, None);

            // Non-existent page in pre-confirmed block - offset too large. Should return no
            // events.
            input.filter.chunk_size = 128;
            input.filter.continuation_token = Some("4-3".to_string());
            let result = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert_eq!(result.events, &[]);
            assert_eq!(result.continuation_token, None);

            // Non-existent page - block number does not exist.
            input.filter.chunk_size = 128;
            input.filter.continuation_token = Some("5-0".to_string());
            let error = get_events(context.clone(), input, RPC_VERSION)
                .await
                .unwrap_err();
            assert_eq!(error, GetEventsError::InvalidContinuationToken);
        }

        #[tokio::test]
        async fn paging_with_no_more_matching_events_in_pre_confirmed() {
            let context = RpcContext::for_tests_with_pre_confirmed().await;

            let mut input = GetEventsInput {
                filter: EventFilter {
                    from_block: None,
                    to_block: Some(BlockId::PreConfirmed),
                    addresses: HashSet::new(),
                    keys: vec![vec![
                        event_key_bytes!(b"event 0 key"),
                        event_key_bytes!(b"preconfirmed key 2"),
                    ]],
                    chunk_size: 1024,
                    continuation_token: None,
                },
            };

            let all = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap()
                .events;
            assert_eq!(all.len(), 2);

            // returns a continuation token if there are more matches in pending
            input.filter.chunk_size = 1;
            let result = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert_eq!(result.events, &all[0..1]);
            assert_eq!(result.continuation_token, Some("3-0".to_string()));
        }

        #[tokio::test]
        async fn key_matching() {
            let context = RpcContext::for_tests_with_pre_confirmed().await;

            let mut input = GetEventsInput {
                filter: EventFilter {
                    from_block: Some(BlockId::PreConfirmed),
                    to_block: Some(BlockId::PreConfirmed),
                    addresses: HashSet::new(),
                    keys: vec![],
                    chunk_size: 1024,
                    continuation_token: None,
                },
            };

            let all = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap()
                .events;
            assert_eq!(all.len(), 3);

            input.filter.keys = vec![vec![event_key_bytes!(b"preconfirmed key 2")]];
            let events = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap()
                .events;
            assert_eq!(events, &all[2..3]);

            input.filter.keys = vec![vec![], vec![event_key_bytes!(b"second preconfirmed key")]];
            let events = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap()
                .events;
            assert_eq!(events, &all[1..2]);
        }

        #[tokio::test]
        async fn from_block_past_pre_confirmed() {
            let context = RpcContext::for_tests_with_pre_confirmed().await;

            let input = GetEventsInput {
                filter: EventFilter {
                    from_block: Some(BlockId::Number(BlockNumber::new_or_panic(4))),
                    to_block: Some(BlockId::PreConfirmed),
                    chunk_size: 100,
                    ..Default::default()
                },
            };
            let result = get_events(context, input, RPC_VERSION).await.unwrap();
            assert!(result.events.is_empty());
        }

        #[tokio::test]
        async fn from_block_pre_confirmed_to_block_none() {
            let context = RpcContext::for_tests_with_pre_confirmed().await;

            let input = GetEventsInput {
                filter: EventFilter {
                    from_block: Some(BlockId::PreConfirmed),
                    to_block: None,
                    chunk_size: 100,
                    ..Default::default()
                },
            };
            let result = get_events(context, input, RPC_VERSION).await.unwrap();
            assert!(!result.events.is_empty());
        }

        #[tokio::test]
        async fn pre_confirmed_block_by_number_returns_only_pending_data() {
            let context = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;

            const PRE_LATEST_BLOCK: BlockNumber = BlockNumber::new_or_panic(3);
            const PRE_CONFIRMED_BLOCK: BlockNumber = BlockNumber::new_or_panic(4);

            let mut input = GetEventsInput {
                filter: EventFilter {
                    from_block: Some(BlockId::Number(PRE_LATEST_BLOCK)),
                    to_block: None,
                    chunk_size: 128,
                    ..Default::default()
                },
            };
            let result = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert!(!result.events.is_empty());
            // Events from pending data have a speculative block number but no
            // hash.
            assert!(result.events.iter().all(|e| e.block_hash.is_none()));
            assert!(result.events.iter().all(|e| {
                e.block_number == Some(PRE_LATEST_BLOCK)
                    || e.block_number == Some(PRE_CONFIRMED_BLOCK)
            }));

            input.filter.from_block = Some(BlockId::Number(PRE_CONFIRMED_BLOCK));
            input.filter.to_block = None;

            let result = get_events(context.clone(), input.clone(), RPC_VERSION)
                .await
                .unwrap();
            assert!(!result.events.is_empty());
            assert!(result.events.iter().all(|e| e.block_hash.is_none()));
            assert!(result.events.iter().all(|e| {
                e.block_number == Some(PRE_LATEST_BLOCK)
                    || e.block_number == Some(PRE_CONFIRMED_BLOCK)
            }));

            input.filter.from_block = Some(BlockId::Number(PRE_LATEST_BLOCK));
            input.filter.to_block = Some(BlockId::Number(PRE_CONFIRMED_BLOCK));

            let result = get_events(context, input, RPC_VERSION).await.unwrap();
            assert!(!result.events.is_empty());
            assert!(result.events.iter().all(|e| e.block_hash.is_none()));
            assert!(result.events.iter().all(|e| {
                e.block_number == Some(PRE_LATEST_BLOCK)
                    || e.block_number == Some(PRE_CONFIRMED_BLOCK)
            }));
        }

        #[tokio::test]
        async fn pending_events_have_block_number_but_no_block_hash() {
            let context = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;

            const PRE_LATEST_BLOCK: BlockNumber = BlockNumber::new_or_panic(3);
            const PRE_CONFIRMED_BLOCK: BlockNumber = BlockNumber::new_or_panic(4);

            // Pre-confirmed only.
            let input = GetEventsInput {
                filter: EventFilter {
                    from_block: Some(BlockId::PreConfirmed),
                    to_block: Some(BlockId::PreConfirmed),
                    chunk_size: 1024,
                    ..Default::default()
                },
            };
            let result = get_events(context.clone(), input, RPC_VERSION)
                .await
                .unwrap();
            assert!(!result.events.is_empty());
            assert!(result
                .events
                .iter()
                .all(|e| e.block_number.is_some() && e.block_hash.is_none()));

            // All pending events (from pre-latest through pre-confirmed) have
            // block_number set and block_hash unset.
            let input = GetEventsInput {
                filter: EventFilter {
                    from_block: Some(BlockId::Number(PRE_LATEST_BLOCK)),
                    to_block: Some(BlockId::Number(PRE_CONFIRMED_BLOCK)),
                    chunk_size: 1024,
                    ..Default::default()
                },
            };
            let result = get_events(context, input, RPC_VERSION).await.unwrap();
            assert!(!result.events.is_empty());
            assert!(result.events.iter().all(|e| e.block_hash.is_none()));
            let has_pre_latest = result
                .events
                .iter()
                .any(|e| e.block_number == Some(PRE_LATEST_BLOCK));
            let has_pre_confirmed = result
                .events
                .iter()
                .any(|e| e.block_number == Some(PRE_CONFIRMED_BLOCK));
            assert!(has_pre_latest);
            assert!(has_pre_confirmed);
        }
    }
}
