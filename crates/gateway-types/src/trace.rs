use std::cell::Cell;
use std::marker::PhantomData;

use pathfinder_common::{ContractAddress, TransactionHash};
use pathfinder_crypto::Felt;
use serde::de::{Error as _, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::reply::transaction::ExecutionResources;

/// Traces served from the feeder gateway are an *untrusted, authoritative*
/// surface: for the mainnet block range where local re-execution is impossible
/// the RPC layer is hard-wired to fall back to the gateway, so a compromised,
/// malicious or byzantine gateway can hand us an arbitrarily crafted response.
/// The constants below bound that surface so a single crafted response cannot
/// exhaust memory or the call stack while deserialising.
///
/// Maximum nesting depth of [`FunctionInvocation::internal_calls`]. Honest
/// mainnet traces nest only a handful of levels deep, so this ceiling is far
/// above anything legitimate while still preventing unbounded recursion (which
/// would overflow the stack and `SIGSEGV`, escaping the RPC layer's
/// `catch_unwind`).
///
/// It is deliberately kept below `serde_json`'s built-in recursion limit of
/// 128. That limit counts every nested JSON container and a single
/// `internal_calls` level costs two of them (the array plus the child object),
/// so `serde_json` aborts at roughly 63 levels on the `from_slice`/`from_str`
/// paths used in production. A cap at or above that point would never fire:
/// `serde_json` would bail out first with its own generic recursion error.
/// Keeping it well below that threshold guarantees our explicit, clearly-worded
/// cap is the one that triggers.
pub const MAX_TRACE_CALL_DEPTH: usize = 50;

/// Maximum number of elements accepted in a single `Vec<Felt>` field of a trace
/// (`calldata`, `result`, event `data`/`keys`, message `payload`,
/// transaction `signature`). Bounds per-field allocation.
pub const MAX_TRACE_FELT_COUNT: usize = 1_000_000;

/// Maximum number of items accepted in a single collection field of a trace
/// (`internal_calls`, `events`, `messages`) at one nesting level.
pub const MAX_TRACE_ITEM_COUNT: usize = 1_000_000;

thread_local! {
    /// Current `internal_calls` nesting depth while deserialising a trace.
    /// Reset to zero once each top-level field has been fully parsed because
    /// every increment is paired with a [`DepthGuard`] that decrements on drop.
    static CALL_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Increments the thread-local [`CALL_DEPTH`] on construction and decrements it
/// on drop, so the counter is restored even if deserialisation fails part-way
/// through a nested subtree.
struct DepthGuard;

impl DepthGuard {
    /// Enters one `internal_calls` nesting level, returning the guard and the
    /// new depth, or an error if the configured ceiling would be exceeded.
    fn enter<E: serde::de::Error>() -> Result<Self, E> {
        let depth = CALL_DEPTH.with(|c| {
            let depth = c.get() + 1;
            c.set(depth);
            depth
        });
        let guard = Self;
        if depth > MAX_TRACE_CALL_DEPTH {
            return Err(E::custom(format!(
                "trace call nesting exceeds depth cap of {MAX_TRACE_CALL_DEPTH}"
            )));
        }
        Ok(guard)
    }
}

impl Drop for DepthGuard {
    fn drop(&mut self) {
        CALL_DEPTH.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

/// Deserialises a sequence while rejecting anything longer than `cap` elements,
/// bounding the pre-allocation to `cap` so a malicious length hint cannot force
/// a large allocation up front.
fn deserialize_capped_seq<'de, D, T>(deserializer: D, cap: usize) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct CapVisitor<T> {
        cap: usize,
        _marker: PhantomData<T>,
    }

    impl<'de, T: Deserialize<'de>> Visitor<'de> for CapVisitor<T> {
        type Value = Vec<T>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "a sequence of at most {} elements", self.cap)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(self.cap));
            while let Some(element) = seq.next_element()? {
                if out.len() >= self.cap {
                    return Err(A::Error::custom(format!(
                        "trace sequence exceeds cap of {}",
                        self.cap
                    )));
                }
                out.push(element);
            }
            Ok(out)
        }
    }

    deserializer.deserialize_seq(CapVisitor {
        cap,
        _marker: PhantomData,
    })
}

/// [`deserialize_capped_seq`] specialised for `Vec<Felt>` fields.
fn capped_felts<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Felt>, D::Error> {
    deserialize_capped_seq(deserializer, MAX_TRACE_FELT_COUNT)
}

/// [`deserialize_capped_seq`] specialised for `Vec<Event>` fields.
fn capped_events<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Event>, D::Error> {
    deserialize_capped_seq(deserializer, MAX_TRACE_ITEM_COUNT)
}

/// [`deserialize_capped_seq`] specialised for `Vec<MsgToL1>` fields.
fn capped_messages<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<MsgToL1>, D::Error> {
    deserialize_capped_seq(deserializer, MAX_TRACE_ITEM_COUNT)
}

/// Deserialises the recursive `internal_calls` field. Each level enters one
/// nesting frame (bounding recursion via [`DepthGuard`]) and caps the fan-out.
fn capped_internal_calls<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<FunctionInvocation>, D::Error> {
    let _guard = DepthGuard::enter::<D::Error>()?;
    deserialize_capped_seq(deserializer, MAX_TRACE_ITEM_COUNT)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionTrace {
    pub revert_error: Option<String>,
    pub validate_invocation: Option<FunctionInvocation>,
    pub function_invocation: Option<FunctionInvocation>,
    pub fee_transfer_invocation: Option<FunctionInvocation>,
    #[serde(deserialize_with = "capped_felts")]
    pub signature: Vec<Felt>,
    // This is present for get_block_traces but not for an individual transaction's
    // trace in get_transaction_trace.
    pub transaction_hash: Option<TransactionHash>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockTrace {
    pub traces: Vec<TransactionTrace>,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub enum CallType {
    #[serde(rename = "CALL")]
    Call,
    #[serde(rename = "DELEGATE")]
    Delegate,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub order: i64,
    #[serde(deserialize_with = "capped_felts")]
    pub data: Vec<Felt>,
    #[serde(deserialize_with = "capped_felts")]
    pub keys: Vec<Felt>,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionInvocation {
    #[serde(deserialize_with = "capped_felts")]
    pub calldata: Vec<Felt>,
    pub contract_address: ContractAddress,
    #[serde(default)]
    pub selector: Option<Felt>,
    #[serde(default)]
    pub call_type: Option<CallType>,
    #[serde(default)]
    pub caller_address: Felt,
    #[serde(default, deserialize_with = "capped_internal_calls")]
    pub internal_calls: Vec<FunctionInvocation>,
    #[serde(default)]
    pub class_hash: Option<Felt>,
    #[serde(default)]
    pub entry_point_type: Option<EntryPointType>,
    #[serde(default, deserialize_with = "capped_events")]
    pub events: Vec<Event>,
    #[serde(default, deserialize_with = "capped_messages")]
    pub messages: Vec<MsgToL1>,
    #[serde(default, deserialize_with = "capped_felts")]
    pub result: Vec<Felt>,
    pub execution_resources: ExecutionResources,
    #[serde(default)]
    pub failed: bool,
    #[serde(default)]
    pub gas_consumed: Option<u128>,
    #[serde(default)]
    pub cairo_native: bool,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub enum EntryPointType {
    #[serde(rename = "CONSTRUCTOR")]
    Constructor,
    #[serde(rename = "EXTERNAL")]
    External,
    #[serde(rename = "L1_HANDLER")]
    L1Handler,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MsgToL1 {
    pub order: usize,
    #[serde(deserialize_with = "capped_felts")]
    pub payload: Vec<Felt>,
    pub to_address: Felt,
}

#[cfg(test)]
mod tests {
    use super::*;

    mod block {
        use starknet_gateway_test_fixtures::traces::{TESTNET_889_517, TESTNET_GENESIS};
        use starknet_gateway_test_fixtures::v0_13_4::traces::SEPOLIA_TESTNET_30000;

        use super::*;

        #[test]
        fn parse_genesis() {
            serde_json::from_slice::<BlockTrace>(TESTNET_GENESIS).unwrap();
        }

        #[test]
        fn parse_889_517() {
            // The latest block trace on testnet at the time.
            serde_json::from_slice::<BlockTrace>(TESTNET_889_517).unwrap();
        }

        #[test]
        fn parse_sepolia_testnet_30000_starknet_0_13_4() {
            serde_json::from_str::<BlockTrace>(SEPOLIA_TESTNET_30000).unwrap();
        }
    }

    mod transactions {
        use starknet_gateway_test_fixtures::traces::{
            SEPOLIA_TESTNET_TX_0X6A4A,
            TESTNET_TX_0_0,
            TESTNET_TX_899_517_0,
        };

        use super::*;

        #[test]
        fn parse_genesis() {
            serde_json::from_slice::<TransactionTrace>(TESTNET_TX_0_0).unwrap();
        }

        #[test]
        fn parse_889_517() {
            // The latest block trace on testnet at the time.
            serde_json::from_slice::<TransactionTrace>(TESTNET_TX_899_517_0).unwrap();
        }

        #[test]
        fn parse_0x6a4a() {
            serde_json::from_slice::<TransactionTrace>(SEPOLIA_TESTNET_TX_0X6A4A).unwrap();
        }
    }

    mod hardening {
        use serde_json::json;

        use super::*;

        /// Builds a minimal, valid `FunctionInvocation` JSON object with
        /// `internal_calls` nested `depth` levels deep.
        fn nested_invocation(depth: usize) -> serde_json::Value {
            let execution_resources = json!({
                "builtin_instance_counter": {},
                "n_steps": 0,
                "n_memory_holes": 0,
            });
            let mut node = json!({
                "calldata": [],
                "contract_address": "0x1",
                "execution_resources": execution_resources,
                "internal_calls": [],
            });
            for _ in 0..depth {
                let child = node;
                node = json!({
                    "calldata": [],
                    "contract_address": "0x1",
                    "execution_resources": {
                        "builtin_instance_counter": {},
                        "n_steps": 0,
                        "n_memory_holes": 0,
                    },
                    "internal_calls": [child],
                });
            }
            node
        }

        #[test]
        fn accepts_nesting_up_to_the_depth_cap() {
            // `MAX_TRACE_CALL_DEPTH` counts `internal_calls` levels, so a tree
            // with that many nested children is exactly at the limit.
            let value = nested_invocation(MAX_TRACE_CALL_DEPTH - 1);
            serde_json::from_value::<FunctionInvocation>(value)
                .expect("nesting at the cap should be accepted");
            // Deserialisation is a scoped operation: the thread-local depth
            // counter must return to zero afterwards.
            assert_eq!(CALL_DEPTH.with(|c| c.get()), 0);
        }

        #[test]
        fn rejects_nesting_beyond_the_depth_cap() {
            let value = nested_invocation(MAX_TRACE_CALL_DEPTH + 5);
            let err = serde_json::from_value::<FunctionInvocation>(value)
                .expect_err("nesting beyond the cap must be rejected");
            assert!(
                err.to_string().contains("depth cap"),
                "unexpected error: {err}"
            );
            assert_eq!(CALL_DEPTH.with(|c| c.get()), 0);
        }

        #[test]
        fn rejects_unknown_fields_on_function_invocation() {
            let value = json!({
                "calldata": [],
                "contract_address": "0x1",
                "execution_resources": {
                    "builtin_instance_counter": {},
                    "n_steps": 0,
                    "n_memory_holes": 0,
                },
                "surprise": "field",
            });
            let err = serde_json::from_value::<FunctionInvocation>(value)
                .expect_err("unknown fields must be rejected");
            assert!(
                err.to_string().contains("unknown field"),
                "unexpected error: {err}"
            );
        }

        #[test]
        fn rejects_overlong_felt_vectors() {
            // One past the cap in a `Vec<Felt>` field must be rejected without
            // materialising the whole sequence.
            let calldata: Vec<serde_json::Value> =
                (0..=MAX_TRACE_FELT_COUNT).map(|_| json!("0x1")).collect();
            let value = json!({
                "calldata": calldata,
                "contract_address": "0x1",
                "execution_resources": {
                    "builtin_instance_counter": {},
                    "n_steps": 0,
                    "n_memory_holes": 0,
                },
            });
            let err = serde_json::from_value::<FunctionInvocation>(value)
                .expect_err("overlong calldata must be rejected");
            assert!(
                err.to_string().contains("exceeds cap"),
                "unexpected error: {err}"
            );
        }
    }
}
