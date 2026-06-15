use pathfinder_common::event::Event;
use pathfinder_common::receipt::Receipt;
use pathfinder_common::transaction::{Transaction, TransactionKind, TransactionVariant};
use pathfinder_common::{BlockHash, BlockNumber, TransactionHash, TransactionVersion};
use serde::ser::Error;

use super::H256Hex;
use crate::dto;
use crate::dto::{SerializeForVersion, Serializer};

#[derive(Copy, Clone)]
pub enum TxnStatus {
    Received,
    PreConfirmed,
    AcceptedOnL2,
    AcceptedOnL1,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TxnExecutionStatus {
    Succeeded,
    Reverted {
        // Revert reason optional for backward compatibility with gateway.
        reason: Option<String>,
    },
}

impl From<&pathfinder_common::receipt::ExecutionStatus> for TxnExecutionStatus {
    fn from(value: &pathfinder_common::receipt::ExecutionStatus) -> Self {
        use pathfinder_common::receipt::ExecutionStatus;
        match value {
            ExecutionStatus::Succeeded => Self::Succeeded,
            ExecutionStatus::Reverted { reason } => Self::Reverted {
                reason: Some(reason.clone()),
            },
        }
    }
}

struct TxnExecutionStatusWithRevertReason<'a>(pub &'a pathfinder_common::receipt::ExecutionStatus);

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum TxnFinalityStatus {
    PreConfirmed,
    AcceptedOnL2,
    AcceptedOnL1,
}

pub struct TxnReceiptWithBlockInfo<'a> {
    pub block_hash: Option<&'a BlockHash>,
    pub block_number: BlockNumber,
    pub receipt: &'a Receipt,
    pub transaction: &'a Transaction,
    pub events: &'a [Event],
    pub finality: TxnFinalityStatus,
}

pub struct TxnReceipt<'a> {
    pub receipt: &'a Receipt,
    pub transaction: &'a Transaction,
    pub events: &'a [Event],
    pub finality: TxnFinalityStatus,
}

pub struct InvokeTxnReceipt<'a>(pub &'a TxnReceipt<'a>);
pub struct L1HandlerTxnReceipt<'a>(pub &'a TxnReceipt<'a>);
pub struct DeclareTxnReceipt<'a>(pub &'a TxnReceipt<'a>);
pub struct DeployTxnReceipt<'a>(pub &'a TxnReceipt<'a>);
pub struct DeployAccountTxnReceipt<'a>(pub &'a TxnReceipt<'a>);

pub struct CommonReceiptProperties<'a>(pub &'a TxnReceipt<'a>);

#[derive(Copy, Clone)]
pub struct PriceUnit<'a>(pub &'a TransactionVersion);

pub struct FeePayment<'a> {
    amount: &'a pathfinder_common::Fee,
    transaction_version: &'a TransactionVersion,
}
pub struct MsgToL1<'a>(pub &'a pathfinder_common::receipt::L2ToL1Message);
pub struct ExecutionResources<'a>(pub &'a pathfinder_common::receipt::ExecutionResources);
pub struct ComputationResources<'a>(pub &'a pathfinder_common::receipt::ExecutionResources);

impl SerializeForVersion for TxnStatus {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self {
            TxnStatus::Received => "RECEIVED",
            TxnStatus::PreConfirmed => "PRE_CONFIRMED",
            TxnStatus::AcceptedOnL2 => "ACCEPTED_ON_L2",
            TxnStatus::AcceptedOnL1 => "ACCEPTED_ON_L1",
        }
        .serialize(serializer)
    }
}

impl SerializeForVersion for TxnExecutionStatus {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self {
            TxnExecutionStatus::Succeeded => "SUCCEEDED",
            TxnExecutionStatus::Reverted { .. } => "REVERTED",
        }
        .serialize(serializer)
    }
}

impl SerializeForVersion for TxnExecutionStatusWithRevertReason<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        use pathfinder_common::receipt::ExecutionStatus;

        let mut serializer = serializer.serialize_struct()?;

        match self.0 {
            ExecutionStatus::Succeeded => {
                serializer.serialize_field("execution_status", &TxnExecutionStatus::Succeeded)?;
            }
            ExecutionStatus::Reverted { reason } => {
                serializer.serialize_field(
                    "execution_status",
                    &TxnExecutionStatus::Reverted {
                        reason: Some(reason.clone()),
                    },
                )?;
                serializer.serialize_field("revert_reason", reason)?;
            }
        }

        serializer.end()
    }
}

impl SerializeForVersion for TxnFinalityStatus {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self {
            TxnFinalityStatus::PreConfirmed => "PRE_CONFIRMED",
            TxnFinalityStatus::AcceptedOnL2 => "ACCEPTED_ON_L2",
            TxnFinalityStatus::AcceptedOnL1 => "ACCEPTED_ON_L1",
        }
        .serialize(serializer)
    }
}

impl crate::dto::DeserializeForVersion for TxnFinalityStatus {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let s: String = value.deserialize()?;
        match s.as_str() {
            "ACCEPTED_ON_L1" => Ok(Self::AcceptedOnL1),
            "ACCEPTED_ON_L2" => Ok(Self::AcceptedOnL2),
            "PRE_CONFIRMED" => Ok(Self::PreConfirmed),
            _ => Err(serde::de::Error::unknown_variant(
                &s,
                &["ACCEPTED_ON_L1", "ACCEPTED_ON_L2", "PRE_CONFIRMED"],
            )),
        }
    }
}

impl SerializeForVersion for TxnReceiptWithBlockInfo<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        let Self {
            block_hash,
            block_number,
            receipt,
            transaction,
            events,
            finality,
        } = self;
        let mut serializer = serializer.serialize_struct()?;

        serializer.flatten(&TxnReceipt {
            receipt,
            transaction,
            events,
            finality: *finality,
        })?;

        serializer.serialize_optional("block_hash", block_hash.cloned())?;
        // Block number is required.
        serializer.serialize_field("block_number", block_number)?;

        serializer.end()
    }
}

impl SerializeForVersion for TxnReceipt<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self.transaction.variant.kind() {
            TransactionKind::Declare => serializer.serialize(&DeclareTxnReceipt(self)),
            TransactionKind::Deploy => serializer.serialize(&DeployTxnReceipt(self)),
            TransactionKind::DeployAccount => serializer.serialize(&DeployAccountTxnReceipt(self)),
            TransactionKind::Invoke => serializer.serialize(&InvokeTxnReceipt(self)),
            TransactionKind::L1Handler => serializer.serialize(&L1HandlerTxnReceipt(self)),
        }
    }
}

impl SerializeForVersion for DeclareTxnReceipt<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        if self.0.transaction.variant.kind() != TransactionKind::Declare {
            return Err(serde_json::error::Error::custom(
                "expected Declare transaction",
            ));
        }

        let mut serializer = serializer.serialize_struct()?;

        serializer.serialize_field("type", &"DECLARE")?;
        serializer.flatten(&CommonReceiptProperties(self.0))?;

        serializer.end()
    }
}

impl SerializeForVersion for DeployTxnReceipt<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        let contract_address = match &self.0.transaction.variant {
            // Partial match here is safe since this variant is deprecated.
            // i.e. no risk of forgetting to handle a new variant.
            TransactionVariant::DeployV0(tx) => &tx.contract_address,
            TransactionVariant::DeployV1(tx) => &tx.contract_address,
            _ => {
                return Err(serde_json::error::Error::custom(
                    "expected Deploy transaction",
                ))
            }
        };

        let mut serializer = serializer.serialize_struct()?;

        serializer.flatten(&CommonReceiptProperties(self.0))?;
        serializer.serialize_field("type", &"DEPLOY")?;
        serializer.serialize_field("contract_address", contract_address)?;

        serializer.end()
    }
}
impl SerializeForVersion for DeployAccountTxnReceipt<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        let contract_address = match &self.0.transaction.variant {
            TransactionVariant::DeployAccountV1(tx) => &tx.contract_address,
            TransactionVariant::DeployAccountV3(tx) => &tx.contract_address,
            TransactionVariant::DeclareV0(_)
            | TransactionVariant::DeclareV1(_)
            | TransactionVariant::DeclareV2(_)
            | TransactionVariant::DeclareV3(_)
            | TransactionVariant::DeployV0(_)
            | TransactionVariant::DeployV1(_)
            | TransactionVariant::InvokeV0(_)
            | TransactionVariant::InvokeV1(_)
            | TransactionVariant::InvokeV3(_)
            | TransactionVariant::L1Handler(_) => {
                return Err(serde_json::error::Error::custom(
                    "expected deploy account transaction",
                ))
            }
        };

        let mut serializer = serializer.serialize_struct()?;

        serializer.flatten(&CommonReceiptProperties(self.0))?;
        serializer.serialize_field("type", &"DEPLOY_ACCOUNT")?;
        serializer.serialize_field("contract_address", contract_address)?;

        serializer.end()
    }
}
impl SerializeForVersion for InvokeTxnReceipt<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        if self.0.transaction.variant.kind() != TransactionKind::Invoke {
            return Err(serde_json::error::Error::custom(
                "expected Invoke transaction",
            ));
        }

        let mut serializer = serializer.serialize_struct()?;

        serializer.serialize_field("type", &"INVOKE")?;
        serializer.flatten(&CommonReceiptProperties(self.0))?;

        serializer.end()
    }
}
impl SerializeForVersion for L1HandlerTxnReceipt<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        let message_hash = match &self.0.transaction.variant {
            TransactionVariant::L1Handler(tx) => tx.calculate_message_hash(),
            TransactionVariant::DeclareV0(_)
            | TransactionVariant::DeclareV1(_)
            | TransactionVariant::DeclareV2(_)
            | TransactionVariant::DeclareV3(_)
            | TransactionVariant::DeployV0(_)
            | TransactionVariant::DeployV1(_)
            | TransactionVariant::DeployAccountV1(_)
            | TransactionVariant::DeployAccountV3(_)
            | TransactionVariant::InvokeV0(_)
            | TransactionVariant::InvokeV1(_)
            | TransactionVariant::InvokeV3(_) => {
                return Err(serde_json::error::Error::custom(
                    "expected L1Handler transaction",
                ))
            }
        };

        let mut serializer = serializer.serialize_struct()?;

        serializer.flatten(&CommonReceiptProperties(self.0))?;
        serializer.serialize_field("type", &"L1_HANDLER")?;
        serializer.serialize_field("message_hash", &H256Hex(message_hash))?;

        serializer.end()
    }
}

impl SerializeForVersion for CommonReceiptProperties<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;

        serializer.serialize_field("transaction_hash", &self.0.transaction.hash)?;
        serializer.serialize_field(
            "actual_fee",
            &FeePayment {
                amount: &self.0.receipt.actual_fee,
                transaction_version: &self.0.transaction.version(),
            },
        )?;
        serializer.serialize_field("finality_status", &self.0.finality)?;
        serializer.serialize_iter(
            "messages_sent",
            self.0.receipt.l2_to_l1_messages.len(),
            &mut self.0.receipt.l2_to_l1_messages.iter().map(MsgToL1),
        )?;
        serializer.serialize_iter(
            "events",
            self.0.events.len(),
            &mut self.0.events.iter().map(|e| dto::Event {
                address: &e.from_address,
                keys: &e.keys,
                data: &e.data,
            }),
        )?;
        serializer.serialize_field(
            "execution_resources",
            &ExecutionResources(&self.0.receipt.execution_resources),
        )?;
        serializer.flatten(&TxnExecutionStatusWithRevertReason(
            &self.0.receipt.execution_status,
        ))?;

        serializer.end()
    }
}

impl SerializeForVersion for FeePayment<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;

        serializer.serialize_field("amount", self.amount)?;
        serializer.serialize_field("unit", &PriceUnit(self.transaction_version))?;

        serializer.end()
    }
}

impl SerializeForVersion for MsgToL1<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;

        serializer.serialize_field("from_address", &self.0.from_address)?;
        serializer.serialize_field("to_address", &self.0.to_address)?;
        serializer.serialize_iter("payload", self.0.payload.len(), &mut self.0.payload.iter())?;

        serializer.end()
    }
}

impl SerializeForVersion for ExecutionResources<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;

        serializer.serialize_field("l1_gas", &self.0.total_gas_consumed.l1_gas)?;
        serializer.serialize_field("l1_data_gas", &self.0.total_gas_consumed.l1_data_gas)?;
        serializer.serialize_field("l2_gas", &self.0.l2_gas)?;

        serializer.end()
    }
}

impl SerializeForVersion for PriceUnit<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self.0 {
            &TransactionVersion::ZERO | &TransactionVersion::ONE | &TransactionVersion::TWO => {
                "WEI"
            }
            _ => "FRI",
        }
        .serialize(serializer)
    }
}

impl SerializeForVersion for ComputationResources<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        use std::num::NonZeroU64;

        // We're technically breaking the spec here if `steps` is zero but turns out
        // there _are_ transactions on Starknet mainnet with steps being zero:
        // https://starkscan.co/tx/0x04026b1598e5915737d439e8b8493cce9e47a5a334948e28f55c391bc2e0c2e2
        let steps = self.0.n_steps;
        let memory_holes = NonZeroU64::new(self.0.n_memory_holes);
        let range_check = NonZeroU64::new(self.0.builtins.range_check);
        let pedersen = NonZeroU64::new(self.0.builtins.pedersen);
        let poseidon = NonZeroU64::new(self.0.builtins.poseidon);
        let ec_op = NonZeroU64::new(self.0.builtins.ec_op);
        let edcsa = NonZeroU64::new(self.0.builtins.ecdsa);
        let bitwise = NonZeroU64::new(self.0.builtins.bitwise);
        let keccak = NonZeroU64::new(self.0.builtins.keccak);
        let segment_arena = NonZeroU64::new(self.0.builtins.segment_arena);

        let mut serializer = serializer.serialize_struct()?;

        serializer.serialize_field("steps", &steps)?;
        serializer.serialize_optional("memory_holes", memory_holes)?;
        serializer.serialize_optional("range_check_builtin_applications", range_check)?;
        serializer.serialize_optional("pedersen_builtin_applications", pedersen)?;
        serializer.serialize_optional("poseidon_builtin_applications", poseidon)?;
        serializer.serialize_optional("ec_op_builtin_applications", ec_op)?;
        serializer.serialize_optional("ecdsa_builtin_applications", edcsa)?;
        serializer.serialize_optional("bitwise_builtin_applications", bitwise)?;
        serializer.serialize_optional("keccak_builtin_applications", keccak)?;
        serializer.serialize_optional("segment_arena_builtin", segment_arena)?;

        serializer.end()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions_sorted::assert_eq;
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::dto::Serializer;

    #[rstest]
    #[case::received(TxnStatus::Received, "RECEIVED")]
    #[case::accepted_on_l2(TxnStatus::AcceptedOnL2, "ACCEPTED_ON_L2")]
    #[case::accepted_on_l1(TxnStatus::AcceptedOnL1, "ACCEPTED_ON_L1")]
    fn txn_status(#[case] input: TxnStatus, #[case] expected: &str) {
        let expected = json!(expected);
        let encoded = input.serialize(Serializer::default()).unwrap();
        assert_eq!(encoded, expected);
    }

    #[rstest]
    #[case::accepted_on_l2(TxnFinalityStatus::AcceptedOnL2, "ACCEPTED_ON_L2")]
    #[case::accepted_on_l1(TxnFinalityStatus::AcceptedOnL1, "ACCEPTED_ON_L1")]
    fn txn_finality_status(#[case] input: TxnFinalityStatus, #[case] expected: &str) {
        let expected = json!(expected);
        let encoded = input.serialize(Serializer::default()).unwrap();
        assert_eq!(encoded, expected);
    }

    #[rstest]
    #[case::succeeded(TxnExecutionStatus::Succeeded, "SUCCEEDED")]
    #[case::reverted_missing_reason(TxnExecutionStatus::Reverted { reason: None }, "REVERTED")]
    #[case::reverted_with_reason(TxnExecutionStatus::Reverted { reason: Some("Reverted because".to_string()) }, "REVERTED")]
    fn txn_execution_status(#[case] input: TxnExecutionStatus, #[case] expected: &str) {
        let expected = json!(expected);
        let encoded = input.serialize(Serializer::default()).unwrap();
        assert_eq!(encoded, expected);
    }

    #[test]
    fn txn_execution_status_with_revert_reason() {
        let input = TxnExecutionStatusWithRevertReason(
            &pathfinder_common::receipt::ExecutionStatus::Succeeded,
        );
        let expected = json!({"execution_status": "SUCCEEDED"});
        let encoded = input.serialize(Serializer::default()).unwrap();
        assert_eq!(encoded, expected);

        let reverted_status = pathfinder_common::receipt::ExecutionStatus::Reverted {
            reason: "reason".to_owned(),
        };
        let input = TxnExecutionStatusWithRevertReason(&reverted_status);
        let expected = json!({"execution_status": "REVERTED", "revert_reason": "reason"});
        let encoded = input.serialize(Serializer::default()).unwrap();
        assert_eq!(encoded, expected);
    }
}
