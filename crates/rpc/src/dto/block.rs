use pathfinder_common::{
    EventCommitment,
    GasPrice,
    L1DataAvailabilityMode,
    StarknetVersion,
    TransactionCommitment,
};
use serde::de::Error;

use crate::dto::SerializeStruct;
use crate::{Reorg, RpcVersion};

impl crate::dto::DeserializeForVersion for crate::types::request::BlockId {
    fn deserialize(value: super::Value) -> Result<Self, serde_json::Error> {
        if value.is_string() {
            let value: String = value.deserialize()?;
            match value.as_str() {
                "latest" => Ok(Self::Latest),
                "l1_accepted" => Ok(Self::L1Accepted),
                "pre_confirmed" => Ok(Self::PreConfirmed),
                _ => Err(serde_json::Error::custom("Invalid block id")),
            }
        } else {
            value.deserialize_map(|value| {
                if value.contains_key("block_number") {
                    Ok(Self::Number(
                        pathfinder_common::BlockNumber::new(value.deserialize("block_number")?)
                            .ok_or_else(|| serde_json::Error::custom("Invalid block number"))?,
                    ))
                } else if value.contains_key("block_hash") {
                    Ok(Self::Hash(pathfinder_common::BlockHash(
                        value.deserialize("block_hash")?,
                    )))
                } else {
                    Err(serde_json::Error::custom("Invalid block id"))
                }
            })
        }
    }
}

impl crate::dto::SerializeForVersion for pathfinder_common::BlockHeader {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("block_hash", &self.hash)?;
        serializer.serialize_field("parent_hash", &self.parent_hash)?;
        serializer.serialize_field("block_number", &self.number.get())?;
        serializer.serialize_field("new_root", &self.state_commitment)?;
        serializer.serialize_field("timestamp", &self.timestamp.get())?;
        serializer.serialize_field("sequencer_address", &self.sequencer_address)?;
        serializer.serialize_field(
            "l1_gas_price",
            &ResourcePrice {
                price_in_wei: self.eth_l1_gas_price,
                price_in_fri: self.strk_l1_gas_price,
            },
        )?;
        serializer.serialize_field("starknet_version", &self.starknet_version.to_string())?;

        serializer.serialize_field(
            "l1_data_gas_price",
            &ResourcePrice {
                price_in_wei: self.eth_l1_data_gas_price,
                price_in_fri: self.strk_l1_data_gas_price,
            },
        )?;
        serializer.serialize_field(
            "l1_da_mode",
            &match self.l1_da_mode {
                L1DataAvailabilityMode::Blob => "BLOB",
                L1DataAvailabilityMode::Calldata => "CALLDATA",
            },
        )?;

        serializer.serialize_field(
            "l2_gas_price",
            &ResourcePrice {
                price_in_wei: self.eth_l2_gas_price,
                price_in_fri: self.strk_l2_gas_price,
            },
        )?;

        if serializer.version >= RpcVersion::V10 {
            if self.starknet_version < StarknetVersion::V_0_13_2 {
                // Pathfinder storage stores 0.13.2-style event and transaction commitments for
                // pre-0.13.2 blocks. This is required so that we can serve the
                // 0.13.2-style commitments over the P2P sync protocol. To avoid
                // confusion, we return zeroed commitments for such blocks.
                serializer.serialize_field("event_commitment", &EventCommitment::ZERO)?;
                serializer
                    .serialize_field("transaction_commitment", &TransactionCommitment::ZERO)?;
            } else {
                serializer.serialize_field("event_commitment", &self.event_commitment)?;
                serializer
                    .serialize_field("transaction_commitment", &self.transaction_commitment)?;
            }
            serializer.serialize_field("receipt_commitment", &self.receipt_commitment)?;
            serializer.serialize_field("state_diff_commitment", &self.state_diff_commitment)?;
            serializer.serialize_field("event_count", &self.event_count)?;
            serializer.serialize_field("transaction_count", &self.transaction_count)?;
            serializer.serialize_field("state_diff_length", &self.state_diff_length)?;
        }

        serializer.end()
    }
}

impl crate::dto::SerializeForVersion
    for (
        pathfinder_common::BlockNumber,
        &starknet_gateway_types::reply::PreLatestBlock,
    )
{
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let (block_number, pending_block) = *self;

        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("block_number", &block_number)?;
        serializer.serialize_field("timestamp", &pending_block.timestamp.get())?;
        serializer.serialize_field("sequencer_address", &pending_block.sequencer_address)?;
        serializer.serialize_field(
            "l1_gas_price",
            &ResourcePrice {
                price_in_wei: pending_block.l1_gas_price.price_in_wei,
                price_in_fri: pending_block.l1_gas_price.price_in_fri,
            },
        )?;
        serializer.serialize_field(
            "starknet_version",
            &pending_block.starknet_version.to_string(),
        )?;

        serializer.serialize_field(
            "l1_data_gas_price",
            &ResourcePrice {
                price_in_wei: pending_block.l1_data_gas_price.price_in_wei,
                price_in_fri: pending_block.l1_data_gas_price.price_in_fri,
            },
        )?;
        serializer.serialize_field(
            "l1_da_mode",
            &match pending_block.l1_da_mode {
                starknet_gateway_types::reply::L1DataAvailabilityMode::Blob => "BLOB",
                starknet_gateway_types::reply::L1DataAvailabilityMode::Calldata => "CALLDATA",
            },
        )?;

        serializer.serialize_field(
            "l2_gas_price",
            &ResourcePrice {
                price_in_wei: pending_block.l2_gas_price.price_in_wei,
                price_in_fri: pending_block.l2_gas_price.price_in_fri,
            },
        )?;

        serializer.end()
    }
}

impl crate::dto::SerializeForVersion for crate::pending::PreConfirmedBlock {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("block_number", &self.number.get())?;
        serializer.serialize_field("timestamp", &self.timestamp.get())?;
        serializer.serialize_field("sequencer_address", &self.sequencer_address)?;
        serializer.serialize_field(
            "l1_gas_price",
            &ResourcePrice {
                price_in_wei: self.l1_gas_price.price_in_wei,
                price_in_fri: self.l1_gas_price.price_in_fri,
            },
        )?;
        serializer.serialize_field("starknet_version", &self.starknet_version.to_string())?;

        serializer.serialize_field(
            "l1_data_gas_price",
            &ResourcePrice {
                price_in_wei: self.l1_data_gas_price.price_in_wei,
                price_in_fri: self.l1_data_gas_price.price_in_fri,
            },
        )?;
        serializer.serialize_field(
            "l1_da_mode",
            &match self.l1_da_mode {
                L1DataAvailabilityMode::Blob => "BLOB",
                L1DataAvailabilityMode::Calldata => "CALLDATA",
            },
        )?;

        serializer.serialize_field(
            "l2_gas_price",
            &ResourcePrice {
                price_in_wei: self.l2_gas_price.price_in_wei,
                price_in_fri: self.l2_gas_price.price_in_fri,
            },
        )?;

        serializer.end()
    }
}

impl crate::dto::SerializeForVersion
    for (
        &Option<pathfinder_common::BlockHash>,
        &crate::pending::PreConfirmedBlock,
    )
{
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.flatten(self.1)?;
        serializer.end()
    }
}

#[derive(Debug)]
struct ResourcePrice {
    pub price_in_wei: GasPrice,
    pub price_in_fri: GasPrice,
}

impl crate::dto::SerializeForVersion for ResourcePrice {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("price_in_wei", &crate::dto::U128Hex(self.price_in_wei.0))?;
        serializer.serialize_field("price_in_fri", &crate::dto::U128Hex(self.price_in_fri.0))?;
        serializer.end()
    }
}

impl crate::dto::SerializeForVersion for Reorg {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("starting_block_number", &self.starting_block_number.get())?;
        serializer.serialize_field("starting_block_hash", &self.starting_block_hash)?;
        serializer.serialize_field("ending_block_number", &self.ending_block_number.get())?;
        serializer.serialize_field("ending_block_hash", &self.ending_block_hash)?;
        serializer.end()
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use serde_json::json;
    use starknet_gateway_types::reply::{GasPrices, PreLatestBlock};

    use crate::dto::{SerializeForVersion, Serializer};
    use crate::RpcVersion;

    #[test]
    fn block_header() {
        let header = BlockHeader::builder()
            .number(BlockNumber::new_or_panic(1000000))
            .timestamp(BlockTimestamp::new_or_panic(1734728886))
            .sequencer_address(sequencer_address!(
                "0x1176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8"
            ))
            .state_commitment(state_commitment!(
                "0x7bd9798e3b03e6dfc12db132d48e4a0dc75202aa6a9b57bc40e3796137bd617"
            ))
            .parent_hash(block_hash!(
                "0x6084bda2cd3247aa11364404f7918001e82a7567cfe0b949fa6a7f3d4b4099f"
            ))
            .l1_da_mode(pathfinder_common::L1DataAvailabilityMode::Blob)
            .eth_l1_gas_price(GasPrice(0x34795c87c))
            .strk_l1_gas_price(GasPrice(0x59425e9d6d3c))
            .eth_l1_data_gas_price(GasPrice(0x85257107))
            .strk_l1_data_gas_price(GasPrice(0xe27be612da1))
            .eth_l2_gas_price(GasPrice(0x12345678))
            .strk_l2_gas_price(GasPrice(0x23456789))
            .starknet_version(StarknetVersion::new(0, 13, 3, 0))
            .finalize_with_hash(block_hash!(
                "0x7256dde30ae68f43f3def9ce2a4433dd3de11b630d4f84336891bad8fe4127e"
            ));

        pretty_assertions_sorted::assert_eq!(
            header.serialize(Serializer::new(RpcVersion::V09)).unwrap(),
            json!({
                "block_hash": "0x7256dde30ae68f43f3def9ce2a4433dd3de11b630d4f84336891bad8fe4127e",
                "block_number": 1000000,
                "l1_da_mode": "BLOB",
                "l1_data_gas_price": {
                  "price_in_fri": "0xe27be612da1",
                  "price_in_wei": "0x85257107"
                },
                "l1_gas_price": {
                  "price_in_fri": "0x59425e9d6d3c",
                  "price_in_wei": "0x34795c87c"
                },
                "l2_gas_price": {
                    "price_in_fri": "0x23456789",
                    "price_in_wei": "0x12345678"
                },
                "new_root": "0x7bd9798e3b03e6dfc12db132d48e4a0dc75202aa6a9b57bc40e3796137bd617",
                "parent_hash": "0x6084bda2cd3247aa11364404f7918001e82a7567cfe0b949fa6a7f3d4b4099f",
                "sequencer_address": "0x1176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8",
                "starknet_version": "0.13.3",
                "timestamp": 1734728886,
            })
        );
    }

    #[test]
    fn pending_block() {
        let block_number = BlockNumber::new_or_panic(12345);
        let pending = PreLatestBlock {
            l1_gas_price: GasPrices {
                price_in_wei: GasPrice(0x34795c87c),
                price_in_fri: GasPrice(0x59425e9d6d3c),
            },
            l1_data_gas_price: GasPrices {
                price_in_wei: GasPrice(0x85257107),
                price_in_fri: GasPrice(0xe27be612da1),
            },
            l2_gas_price: GasPrices {
                price_in_wei: GasPrice(0x12345678),
                price_in_fri: GasPrice(0x23456789),
            },
            parent_hash: block_hash!(
                "0x6084bda2cd3247aa11364404f7918001e82a7567cfe0b949fa6a7f3d4b4099f"
            ),
            sequencer_address: sequencer_address!(
                "0x1176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8"
            ),
            timestamp: BlockTimestamp::new_or_panic(1734728886),
            starknet_version: StarknetVersion::new(0, 13, 3, 0),
            l1_da_mode: starknet_gateway_types::reply::L1DataAvailabilityMode::Blob,
            ..Default::default()
        };

        pretty_assertions_sorted::assert_eq!(
            (block_number, &pending)
                .serialize(Serializer::new(RpcVersion::V09))
                .unwrap(),
            json!({
                "l1_da_mode": "BLOB",
                "l1_data_gas_price": {
                  "price_in_fri": "0xe27be612da1",
                  "price_in_wei": "0x85257107"
                },
                "l1_gas_price": {
                  "price_in_fri": "0x59425e9d6d3c",
                  "price_in_wei": "0x34795c87c"
                },
                "l2_gas_price": {
                    "price_in_fri": "0x23456789",
                    "price_in_wei": "0x12345678"
                },
                "block_number": 12345,
                "sequencer_address": "0x1176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8",
                "starknet_version": "0.13.3",
                "timestamp": 1734728886,
            })
        );
    }
}
