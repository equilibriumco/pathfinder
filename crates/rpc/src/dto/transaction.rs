use pathfinder_common::transaction::TransactionVariant;
use pathfinder_common::TransactionHash;
use serde::de::Error;

use super::{DeserializeForVersion, U128Hex, U64Hex};
use crate::dto::{SerializeForVersion, Serializer};
use crate::{dto, RpcVersion};

#[derive(Debug, Default, Eq, PartialEq)]
pub struct TransactionResponseFlags(pub Vec<TransactionResponseFlag>);

#[derive(Debug, Eq, PartialEq)]
pub enum TransactionResponseFlag {
    IncludeProofFacts,
}

impl crate::dto::DeserializeForVersion for TransactionResponseFlag {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let value: String = value.deserialize()?;
        match value.as_str() {
            "INCLUDE_PROOF_FACTS" => Ok(Self::IncludeProofFacts),
            _ => Err(serde_json::Error::custom("Invalid response flag")),
        }
    }
}

impl crate::dto::DeserializeForVersion for TransactionResponseFlags {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let array = value.deserialize_array(TransactionResponseFlag::deserialize)?;
        Ok(Self(array))
    }
}

pub struct TransactionWithHash<'a> {
    pub transaction: &'a pathfinder_common::transaction::Transaction,
    pub include_proof_facts: bool,
}

impl SerializeForVersion for (&pathfinder_common::transaction::Transaction, bool) {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        let (tx, include_proof_facts) = *self;
        let mut s = serializer.serialize_struct()?;
        match &tx.variant {
            TransactionVariant::DeclareV0(tx) => {
                s.serialize_field("type", &"DECLARE")?;
                s.serialize_field("sender_address", &tx.sender_address)?;
                s.serialize_field("max_fee", &tx.max_fee)?;
                s.serialize_field("version", &"0x0")?;
                s.serialize_iter("signature", tx.signature.len(), &mut tx.signature.iter())?;
                s.serialize_field("class_hash", &tx.class_hash)?;
            }
            TransactionVariant::DeclareV1(tx) => {
                s.serialize_field("type", &"DECLARE")?;
                s.serialize_field("sender_address", &tx.sender_address)?;
                s.serialize_field("max_fee", &tx.max_fee)?;
                s.serialize_field("version", &"0x1")?;
                s.serialize_iter("signature", tx.signature.len(), &mut tx.signature.iter())?;
                s.serialize_field("nonce", &tx.nonce)?;
                s.serialize_field("class_hash", &tx.class_hash)?;
            }
            TransactionVariant::DeclareV2(tx) => {
                s.serialize_field("type", &"DECLARE")?;
                s.serialize_field("sender_address", &tx.sender_address)?;
                s.serialize_field("compiled_class_hash", &tx.compiled_class_hash)?;
                s.serialize_field("max_fee", &tx.max_fee)?;
                s.serialize_field("version", &"0x2")?;
                s.serialize_iter("signature", tx.signature.len(), &mut tx.signature.iter())?;
                s.serialize_field("nonce", &tx.nonce)?;
                s.serialize_field("class_hash", &tx.class_hash)?;
            }
            TransactionVariant::DeclareV3(tx) => {
                s.serialize_field("type", &"DECLARE")?;
                s.serialize_field("sender_address", &tx.sender_address)?;
                s.serialize_field("compiled_class_hash", &tx.compiled_class_hash)?;
                s.serialize_field("version", &"0x3")?;
                s.serialize_iter("signature", tx.signature.len(), &mut tx.signature.iter())?;
                s.serialize_field("nonce", &tx.nonce)?;
                s.serialize_field("class_hash", &tx.class_hash)?;
                s.serialize_field("resource_bounds", &tx.resource_bounds)?;
                s.serialize_field("tip", &U64Hex(tx.tip.0))?;
                s.serialize_iter(
                    "paymaster_data",
                    tx.paymaster_data.len(),
                    &mut tx.paymaster_data.iter(),
                )?;
                s.serialize_iter(
                    "account_deployment_data",
                    tx.account_deployment_data.len(),
                    &mut tx.account_deployment_data.iter(),
                )?;
                s.serialize_field(
                    "nonce_data_availability_mode",
                    &tx.nonce_data_availability_mode,
                )?;
                s.serialize_field("fee_data_availability_mode", &tx.fee_data_availability_mode)?;
            }
            TransactionVariant::DeployV0(tx) => {
                s.serialize_field("version", &"0x0")?;
                s.serialize_field("type", &"DEPLOY")?;
                s.serialize_field("contract_address_salt", &tx.contract_address_salt)?;
                s.serialize_iter(
                    "constructor_calldata",
                    tx.constructor_calldata.len(),
                    &mut tx.constructor_calldata.iter(),
                )?;
                s.serialize_field("class_hash", &tx.class_hash)?;
            }
            TransactionVariant::DeployV1(tx) => {
                s.serialize_field("version", &"0x1")?;
                s.serialize_field("type", &"DEPLOY")?;
                s.serialize_field("contract_address_salt", &tx.contract_address_salt)?;
                s.serialize_iter(
                    "constructor_calldata",
                    tx.constructor_calldata.len(),
                    &mut tx.constructor_calldata.iter(),
                )?;
                s.serialize_field("class_hash", &tx.class_hash)?;
            }
            TransactionVariant::DeployAccountV1(tx) => {
                s.serialize_field("type", &"DEPLOY_ACCOUNT")?;
                s.serialize_field("max_fee", &tx.max_fee)?;
                s.serialize_field("version", &"0x1")?;
                s.serialize_iter("signature", tx.signature.len(), &mut tx.signature.iter())?;
                s.serialize_field("nonce", &tx.nonce)?;
                s.serialize_field("contract_address_salt", &tx.contract_address_salt)?;
                s.serialize_iter(
                    "constructor_calldata",
                    tx.constructor_calldata.len(),
                    &mut tx.constructor_calldata.iter(),
                )?;
                s.serialize_field("class_hash", &tx.class_hash)?;
            }
            TransactionVariant::DeployAccountV3(tx) => {
                s.serialize_field("type", &"DEPLOY_ACCOUNT")?;
                s.serialize_field("version", &"0x3")?;
                s.serialize_iter("signature", tx.signature.len(), &mut tx.signature.iter())?;
                s.serialize_field("nonce", &tx.nonce)?;
                s.serialize_field("contract_address_salt", &tx.contract_address_salt)?;
                s.serialize_iter(
                    "constructor_calldata",
                    tx.constructor_calldata.len(),
                    &mut tx.constructor_calldata.iter(),
                )?;
                s.serialize_field("class_hash", &tx.class_hash)?;
                s.serialize_field("resource_bounds", &tx.resource_bounds)?;
                s.serialize_field("tip", &U64Hex(tx.tip.0))?;
                s.serialize_iter(
                    "paymaster_data",
                    tx.paymaster_data.len(),
                    &mut tx.paymaster_data.iter(),
                )?;
                s.serialize_field(
                    "nonce_data_availability_mode",
                    &tx.nonce_data_availability_mode,
                )?;
                s.serialize_field("fee_data_availability_mode", &tx.fee_data_availability_mode)?;
            }
            TransactionVariant::InvokeV0(tx) => {
                s.serialize_field("type", &"INVOKE")?;
                s.serialize_field("max_fee", &tx.max_fee)?;
                s.serialize_field("version", &"0x0")?;
                s.serialize_iter("signature", tx.signature.len(), &mut tx.signature.iter())?;
                s.serialize_field("contract_address", &tx.sender_address)?;
                s.serialize_field("entry_point_selector", &tx.entry_point_selector)?;
                s.serialize_iter("calldata", tx.calldata.len(), &mut tx.calldata.iter())?;
            }
            TransactionVariant::InvokeV1(tx) => {
                s.serialize_field("type", &"INVOKE")?;
                s.serialize_field("sender_address", &tx.sender_address)?;
                s.serialize_iter("calldata", tx.calldata.len(), &mut tx.calldata.iter())?;
                s.serialize_field("max_fee", &tx.max_fee)?;
                s.serialize_field("version", &"0x1")?;
                s.serialize_iter("signature", tx.signature.len(), &mut tx.signature.iter())?;
                s.serialize_field("nonce", &tx.nonce)?;
            }
            TransactionVariant::InvokeV3(tx) => {
                s.serialize_field("type", &"INVOKE")?;
                s.serialize_field("sender_address", &tx.sender_address)?;
                s.serialize_iter("calldata", tx.calldata.len(), &mut tx.calldata.iter())?;
                s.serialize_field("version", &"0x3")?;
                s.serialize_iter("signature", tx.signature.len(), &mut tx.signature.iter())?;
                s.serialize_field("nonce", &tx.nonce)?;
                s.serialize_field("resource_bounds", &tx.resource_bounds)?;
                s.serialize_field("tip", &U64Hex(tx.tip.0))?;
                s.serialize_iter(
                    "paymaster_data",
                    tx.paymaster_data.len(),
                    &mut tx.paymaster_data.iter(),
                )?;
                s.serialize_iter(
                    "account_deployment_data",
                    tx.account_deployment_data.len(),
                    &mut tx.account_deployment_data.iter(),
                )?;
                s.serialize_field(
                    "nonce_data_availability_mode",
                    &tx.nonce_data_availability_mode,
                )?;
                s.serialize_field("fee_data_availability_mode", &tx.fee_data_availability_mode)?;

                if include_proof_facts && serializer.version >= RpcVersion::V10 {
                    s.serialize_field("proof_facts", &tx.proof_facts)?;
                }
            }
            TransactionVariant::L1Handler(tx) => {
                s.serialize_field("version", &"0x0")?;
                s.serialize_field("type", &"L1_HANDLER")?;
                s.serialize_field("nonce", &tx.nonce)?;
                s.serialize_field("contract_address", &tx.contract_address)?;
                s.serialize_field("entry_point_selector", &tx.entry_point_selector)?;
                s.serialize_iter("calldata", tx.calldata.len(), &mut tx.calldata.iter())?;
            }
        }
        s.end()
    }
}

impl SerializeForVersion for TransactionWithHash<'_> {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut s = serializer.serialize_struct()?;
        s.serialize_field("transaction_hash", &self.transaction.hash)?;
        s.flatten(&(self.transaction, self.include_proof_facts))?;
        s.end()
    }
}

impl SerializeForVersion for pathfinder_common::transaction::ResourceBounds {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("l1_gas", &self.l1_gas)?;
        serializer.serialize_field("l2_gas", &self.l2_gas)?;
        // `l1_data_gas` is serialized as (0, 0) even if it's not set
        // See https://github.com/equilibriumco/pathfinder/issues/2571
        serializer.serialize_field("l1_data_gas", &self.l1_data_gas.unwrap_or_default())?;
        serializer.end()
    }
}

impl DeserializeForVersion for pathfinder_common::transaction::ResourceBounds {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                l1_gas: value.deserialize("l1_gas")?,
                l2_gas: value.deserialize("l2_gas")?,
                // `l1_data_gas` is *required*
                // See https://github.com/equilibriumco/pathfinder/issues/2571
                l1_data_gas: Some(value.deserialize("l1_data_gas")?),
            })
        })
    }
}

impl SerializeForVersion for pathfinder_common::transaction::ResourceBound {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("max_amount", &self.max_amount)?;
        serializer.serialize_field("max_price_per_unit", &self.max_price_per_unit)?;
        serializer.end()
    }
}

impl DeserializeForVersion for pathfinder_common::transaction::ResourceBound {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                max_amount: pathfinder_common::ResourceAmount(
                    value.deserialize::<U64Hex>("max_amount")?.0,
                ),
                max_price_per_unit: pathfinder_common::ResourcePricePerUnit(
                    value.deserialize::<U128Hex>("max_price_per_unit")?.0,
                ),
            })
        })
    }
}

impl SerializeForVersion for pathfinder_common::transaction::DataAvailabilityMode {
    fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self {
            pathfinder_common::transaction::DataAvailabilityMode::L1 => {
                serializer.serialize_str("L1")
            }
            pathfinder_common::transaction::DataAvailabilityMode::L2 => {
                serializer.serialize_str("L2")
            }
        }
    }
}

impl DeserializeForVersion for pathfinder_common::transaction::DataAvailabilityMode {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let value: String = value.deserialize()?;
        match value.as_str() {
            "L1" => Ok(Self::L1),
            "L2" => Ok(Self::L2),
            _ => Err(serde_json::Error::custom("invalid data availability mode")),
        }
    }
}

impl DeserializeForVersion for pathfinder_common::TransactionIndex {
    fn deserialize(value: dto::Value) -> Result<Self, serde_json::Error> {
        let idx = value.deserialize()?;
        Self::new(idx).ok_or_else(|| serde_json::Error::custom("Invalid transaction index"))
    }
}

impl crate::dto::SerializeForVersion for Vec<pathfinder_common::CallParam> {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_iter(self.len(), &mut self.iter())
    }
}

impl crate::dto::SerializeForVersion for Vec<pathfinder_common::AccountDeploymentDataElem> {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_iter(self.len(), &mut self.iter())
    }
}

impl crate::dto::SerializeForVersion for Vec<pathfinder_common::TransactionSignatureElem> {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_iter(self.len(), &mut self.iter())
    }
}

impl crate::dto::SerializeForVersion for Vec<pathfinder_common::ConstructorParam> {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_iter(self.len(), &mut self.iter())
    }
}

impl crate::dto::SerializeForVersion for Vec<pathfinder_common::PaymasterDataElem> {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_iter(self.len(), &mut self.iter())
    }
}

impl crate::dto::SerializeForVersion for Vec<pathfinder_common::ProofFactElem> {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_iter(self.len(), &mut self.iter())
    }
}

impl crate::dto::SerializeForVersion for pathfinder_common::Proof {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        (&self).serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::transaction::{ResourceBound, ResourceBounds};
    use pathfinder_common::{ResourceAmount, ResourcePricePerUnit};
    use serde_json::json;
    use starknet_api::transaction::fields::Resource;

    use crate::dto::{SerializeForVersion, Serializer};
    use crate::RpcVersion;

    mod transaction {
        use pathfinder_common::macro_prelude::*;
        use pathfinder_common::transaction::*;
        use pathfinder_common::{ResourceAmount, ResourcePricePerUnit, Tip};
        use pretty_assertions_sorted::assert_eq;
        use serde_json::json;
        use starknet_api::transaction_hash;

        use super::*;
        use crate::dto::{SerializeForVersion, Serializer};
        use crate::RpcVersion;

        #[test]
        fn declare_v0() {
            let uut = TransactionVariant::DeclareV0(DeclareTransactionV0V1 {
                class_hash: class_hash!("0x123"),
                max_fee: fee!("0x1111"),
                nonce: transaction_nonce!("0xaabbcc"),
                sender_address: contract_address!("0xabc"),
                signature: vec![
                    transaction_signature_elem!("0xa1b1"),
                    transaction_signature_elem!("0x1a1b"),
                ],
            });
            let uut = &Transaction {
                variant: uut,
                hash: transaction_hash!("0x123"),
            };

            let expected = json!({
                "type": "DECLARE",
                "version": "0x0",
                "sender_address": "0xabc",
                "max_fee": "0x1111",
                "signature": ["0xa1b1", "0x1a1b"],
                "class_hash": "0x123",
            });
            let result = (uut, false)
                .serialize(Serializer {
                    version: RpcVersion::V09,
                })
                .unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn declare_v1() {
            let uut = TransactionVariant::DeclareV1(DeclareTransactionV0V1 {
                class_hash: class_hash!("0x123"),
                max_fee: fee!("0x1111"),
                nonce: transaction_nonce!("0xaabbcc"),
                sender_address: contract_address!("0xabc"),
                signature: vec![
                    transaction_signature_elem!("0xa1b1"),
                    transaction_signature_elem!("0x1a1b"),
                ],
            });
            let uut = &Transaction {
                variant: uut,
                hash: transaction_hash!("0x123"),
            };

            let expected = json!({
                "type": "DECLARE",
                "version": "0x1",
                "sender_address": "0xabc",
                "max_fee": "0x1111",
                "signature": ["0xa1b1", "0x1a1b"],
                "class_hash": "0x123",
                "nonce": "0xaabbcc",
            });
            let result = (uut, false)
                .serialize(Serializer {
                    version: RpcVersion::V09,
                })
                .unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn declare_v2() {
            let uut: TransactionVariant = DeclareTransactionV2 {
                class_hash: class_hash!("0x123"),
                max_fee: fee!("0x1111"),
                nonce: transaction_nonce!("0xaabbcc"),
                sender_address: contract_address!("0xabc"),
                signature: vec![
                    transaction_signature_elem!("0xa1b1"),
                    transaction_signature_elem!("0x1a1b"),
                ],
                compiled_class_hash: casm_hash!("0xbbbbb"),
            }
            .into();
            let uut = &Transaction {
                variant: uut,
                hash: transaction_hash!("0x123"),
            };

            let expected = json!({
                "type": "DECLARE",
                "version": "0x2",
                "sender_address": "0xabc",
                "max_fee": "0x1111",
                "signature": ["0xa1b1", "0x1a1b"],
                "class_hash": "0x123",
                "nonce": "0xaabbcc",
                "compiled_class_hash": "0xbbbbb",
            });
            let result = (uut, false)
                .serialize(Serializer {
                    version: RpcVersion::V09,
                })
                .unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn declare_v3() {
            let uut: TransactionVariant = DeclareTransactionV3 {
                class_hash: class_hash!("0x123"),
                nonce: transaction_nonce!("0xaabbcc"),
                sender_address: contract_address!("0xabc"),
                signature: vec![
                    transaction_signature_elem!("0xa1b1"),
                    transaction_signature_elem!("0x1a1b"),
                ],
                compiled_class_hash: casm_hash!("0xbbbbb"),
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                resource_bounds: ResourceBounds {
                    l1_gas: ResourceBound {
                        max_amount: ResourceAmount(256),
                        max_price_per_unit: ResourcePricePerUnit(10),
                    },
                    l2_gas: Default::default(),
                    l1_data_gas: Default::default(),
                },
                tip: Tip(5),
                paymaster_data: vec![],
                account_deployment_data: vec![],
            }
            .into();
            let uut = &Transaction {
                variant: uut,
                hash: transaction_hash!("0x123"),
            };

            let expected = json!({
                "type": "DECLARE",
                "version": "0x3",
                "sender_address": "0xabc",
                "signature": ["0xa1b1", "0x1a1b"],
                "class_hash": "0x123",
                "nonce": "0xaabbcc",
                "compiled_class_hash": "0xbbbbb",
                "nonce_data_availability_mode": "L1",
                "fee_data_availability_mode": "L1",
                "resource_bounds": {
                    "l1_gas": {
                        "max_amount": "0x100",
                        "max_price_per_unit": "0xa",
                    },
                    "l2_gas": {
                        "max_amount": "0x0",
                        "max_price_per_unit": "0x0",
                    },
                    "l1_data_gas": {
                        "max_amount": "0x0",
                        "max_price_per_unit": "0x0",
                    }
                },
                "tip": "0x5",
                "paymaster_data": [],
                "account_deployment_data": [],
            });
            let result = (uut, false)
                .serialize(Serializer {
                    version: RpcVersion::V09,
                })
                .unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn deploy() {
            let uut: TransactionVariant = DeployTransactionV0 {
                contract_address: contract_address!("0xabc"),
                contract_address_salt: contract_address_salt!("0xeeee"),
                class_hash: class_hash!("0x123"),
                constructor_calldata: vec![
                    constructor_param!("0xbbb0"),
                    constructor_param!("0xbbb1"),
                ],
            }
            .into();
            let uut = &Transaction {
                variant: uut,
                hash: transaction_hash!("0x123"),
            };

            let expected = json!({
                "type": "DEPLOY",
                "contract_address_salt": "0xeeee",
                "class_hash": "0x123",
                "constructor_calldata": ["0xbbb0","0xbbb1"],
                "version": "0x0",
            });
            let result = (uut, false)
                .serialize(Serializer {
                    version: RpcVersion::V09,
                })
                .unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn deploy_account_v1() {
            let uut: TransactionVariant = DeployAccountTransactionV1 {
                contract_address: contract_address!("0xabc"),
                max_fee: fee!("0x1111"),
                signature: vec![
                    transaction_signature_elem!("0xa1b1"),
                    transaction_signature_elem!("0x1a1b"),
                ],
                nonce: transaction_nonce!("0xaabbcc"),
                contract_address_salt: contract_address_salt!("0xeeee"),
                constructor_calldata: vec![call_param!("0xbbb0"), call_param!("0xbbb1")],
                class_hash: class_hash!("0x123"),
            }
            .into();
            let uut = &Transaction {
                variant: uut,
                hash: transaction_hash!("0x123"),
            };

            let expected = json!({
                "type": "DEPLOY_ACCOUNT",
                "max_fee": "0x1111",
                "version": "0x1",
                "signature": ["0xa1b1", "0x1a1b"],
                "nonce": "0xaabbcc",
                "contract_address_salt": "0xeeee",
                "constructor_calldata": ["0xbbb0","0xbbb1"],
                "class_hash": "0x123",
            });
            let result = (uut, false)
                .serialize(Serializer {
                    version: RpcVersion::V09,
                })
                .unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn deploy_account_v3() {
            let uut: TransactionVariant = DeployAccountTransactionV3 {
                contract_address: contract_address!("0xabc"),
                signature: vec![
                    transaction_signature_elem!("0xa1b1"),
                    transaction_signature_elem!("0x1a1b"),
                ],
                nonce: transaction_nonce!("0xaabbcc"),
                contract_address_salt: contract_address_salt!("0xeeee"),
                constructor_calldata: vec![call_param!("0xbbb0"), call_param!("0xbbb1")],
                class_hash: class_hash!("0x123"),
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                resource_bounds: ResourceBounds {
                    l1_gas: ResourceBound {
                        max_amount: ResourceAmount(256),
                        max_price_per_unit: ResourcePricePerUnit(10),
                    },
                    l2_gas: Default::default(),
                    l1_data_gas: Default::default(),
                },
                tip: Tip(5),
                paymaster_data: vec![],
            }
            .into();
            let uut = &Transaction {
                variant: uut,
                hash: transaction_hash!("0x123"),
            };

            let expected = json!({
                "type": "DEPLOY_ACCOUNT",
                "version": "0x3",
                "signature": ["0xa1b1", "0x1a1b"],
                "nonce": "0xaabbcc",
                "contract_address_salt": "0xeeee",
                "constructor_calldata": ["0xbbb0","0xbbb1"],
                "class_hash": "0x123",
                "nonce_data_availability_mode": "L1",
                "fee_data_availability_mode": "L1",
                "resource_bounds": {
                    "l1_gas": {
                        "max_amount": "0x100",
                        "max_price_per_unit": "0xa",
                    },
                    "l2_gas": {
                        "max_amount": "0x0",
                        "max_price_per_unit": "0x0",
                    },
                    "l1_data_gas": {
                        "max_amount": "0x0",
                        "max_price_per_unit": "0x0",
                    }
                },
                "tip": "0x5",
                "paymaster_data": [],
            });
            let result = (uut, false)
                .serialize(Serializer {
                    version: RpcVersion::V09,
                })
                .unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn invoke_v0() {
            let uut: TransactionVariant = InvokeTransactionV0 {
                calldata: vec![call_param!("0xfff1"), call_param!("0xfff0")],
                sender_address: contract_address!("0xabc"),
                entry_point_selector: entry_point!("0xdead"),
                entry_point_type: Some(EntryPointType::External),
                max_fee: fee!("0x1111"),
                signature: vec![
                    transaction_signature_elem!("0xa1b1"),
                    transaction_signature_elem!("0x1a1b"),
                ],
            }
            .into();
            let uut = &Transaction {
                variant: uut,
                hash: transaction_hash!("0x123"),
            };

            let expected = json!({
                "type": "INVOKE",
                "version": "0x0",
                "calldata": ["0xfff1","0xfff0"],
                "contract_address": "0xabc",
                "entry_point_selector": "0xdead",
                "max_fee": "0x1111",
                "signature": ["0xa1b1", "0x1a1b"],
            });
            let result = (uut, false)
                .serialize(Serializer {
                    version: RpcVersion::V09,
                })
                .unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn invoke_v1() {
            let uut: TransactionVariant = InvokeTransactionV1 {
                calldata: vec![call_param!("0xfff1"), call_param!("0xfff0")],
                sender_address: contract_address!("0xabc"),
                max_fee: fee!("0x1111"),
                signature: vec![
                    transaction_signature_elem!("0xa1b1"),
                    transaction_signature_elem!("0x1a1b"),
                ],
                nonce: transaction_nonce!("0xaabbcc"),
            }
            .into();
            let uut = &Transaction {
                variant: uut,
                hash: transaction_hash!("0x123"),
            };

            let expected = json!({
                "type": "INVOKE",
                "version": "0x1",
                "calldata": ["0xfff1","0xfff0"],
                "sender_address": "0xabc",
                "max_fee": "0x1111",
                "signature": ["0xa1b1", "0x1a1b"],
                "nonce": "0xaabbcc",
            });
            let result = (uut, false)
                .serialize(Serializer {
                    version: RpcVersion::V09,
                })
                .unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn invoke_v3() {
            let uut: TransactionVariant = InvokeTransactionV3 {
                calldata: vec![call_param!("0xfff1"), call_param!("0xfff0")],
                sender_address: contract_address!("0xabc"),
                signature: vec![
                    transaction_signature_elem!("0xa1b1"),
                    transaction_signature_elem!("0x1a1b"),
                ],
                nonce: transaction_nonce!("0xaabbcc"),
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                resource_bounds: ResourceBounds {
                    l1_gas: ResourceBound {
                        max_amount: ResourceAmount(256),
                        max_price_per_unit: ResourcePricePerUnit(10),
                    },
                    l2_gas: Default::default(),
                    l1_data_gas: Default::default(),
                },
                tip: Tip(5),
                paymaster_data: vec![],
                account_deployment_data: vec![],
                proof_facts: vec![],
            }
            .into();
            let uut = &Transaction {
                variant: uut,
                hash: transaction_hash!("0x123"),
            };

            let expected = json!({
                "type": "INVOKE",
                "version": "0x3",
                "calldata": ["0xfff1","0xfff0"],
                "sender_address": "0xabc",
                "signature": ["0xa1b1", "0x1a1b"],
                "nonce": "0xaabbcc",
                "nonce_data_availability_mode": "L1",
                "fee_data_availability_mode": "L1",
                "resource_bounds": {
                    "l1_gas": {
                        "max_amount": "0x100",
                        "max_price_per_unit": "0xa",
                    },
                    "l2_gas": {
                        "max_amount": "0x0",
                        "max_price_per_unit": "0x0",
                    },
                    "l1_data_gas": {
                        "max_amount": "0x0",
                        "max_price_per_unit": "0x0",
                    }
                },
                "tip": "0x5",
                "paymaster_data": [],
                "account_deployment_data": [],
            });
            let result = (uut, false)
                .serialize(Serializer {
                    version: RpcVersion::V09,
                })
                .unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn l1_handler() {
            let uut: TransactionVariant = L1HandlerTransaction {
                contract_address: contract_address!("0xabc"),
                entry_point_selector: entry_point!("0xdead"),
                nonce: transaction_nonce!("0xaabbcc"),
                calldata: vec![call_param!("0xfff1"), call_param!("0xfff0")],
            }
            .into();
            let uut = &Transaction {
                variant: uut,
                hash: transaction_hash!("0x123"),
            };

            let expected = json!({
                "type": "L1_HANDLER",
                "contract_address": "0xabc",
                "entry_point_selector": "0xdead",
                "nonce": "0xaabbcc",
                "calldata": ["0xfff1","0xfff0"],
                "version": "0x0",
            });
            let result = (uut, false)
                .serialize(Serializer {
                    version: RpcVersion::V09,
                })
                .unwrap();

            assert_eq!(result, expected);
        }
    }

    #[test]
    fn resource_bounds() {
        let resource_bounds = ResourceBounds {
            l1_gas: ResourceBound {
                max_amount: ResourceAmount(1),
                max_price_per_unit: ResourcePricePerUnit(2),
            },
            l2_gas: ResourceBound {
                max_amount: ResourceAmount(3),
                max_price_per_unit: ResourcePricePerUnit(4),
            },
            l1_data_gas: Some(ResourceBound {
                max_amount: ResourceAmount(5),
                max_price_per_unit: ResourcePricePerUnit(6),
            }),
        };

        pretty_assertions_sorted::assert_eq!(
            resource_bounds
                .serialize(Serializer::new(RpcVersion::V09))
                .unwrap(),
            json!({
                "l1_gas": {
                    "max_amount": "0x1",
                    "max_price_per_unit": "0x2",
                },
                "l2_gas": {
                    "max_amount": "0x3",
                    "max_price_per_unit": "0x4",
                },
                "l1_data_gas": {
                    "max_amount": "0x5",
                    "max_price_per_unit": "0x6",
                },
            })
        );
    }
}
