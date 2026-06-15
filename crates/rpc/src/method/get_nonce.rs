use anyhow::Context;
use pathfinder_common::{ContractAddress, ContractNonce};

use crate::context::RpcContext;
use crate::types::BlockId;
use crate::RpcVersion;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Input {
    block_id: BlockId,
    contract_address: ContractAddress,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                block_id: value.deserialize("block_id")?,
                contract_address: value.deserialize("contract_address").map(ContractAddress)?,
            })
        })
    }
}

#[derive(Debug)]
pub struct Output(ContractNonce);

crate::error::generate_rpc_error_subset!(Error: BlockNotFound, ContractNotFound);

pub async fn get_nonce(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, Error> {
    let span = tracing::Span::current();
    util::task::spawn_blocking(move |_| -> Result<_, Error> {
        let _g = span.enter();
        let mut db = context
            .storage
            .connection()
            .context("Opening database connection")?;
        let tx = db.transaction().context("Creating database transaction")?;

        if input.block_id.is_pending() {
            let nonce = context
                .pending_data
                .get(&tx, rpc_version)?
                .find_nonce(input.contract_address);

            if let Some(nonce) = nonce {
                return Ok(Output(nonce));
            }
        }

        // Check that block exists. This should occur first as the block number
        // isn't checked explicitly (i.e. nonce fetch just uses <= number).
        let block_id = input
            .block_id
            .to_common_coerced(&tx)
            .map_err(|_| Error::BlockNotFound)?;
        if !tx.block_exists(block_id)? {
            return Err(Error::BlockNotFound);
        }

        let nonce = tx
            .contract_nonce(input.contract_address, block_id)
            .context("Querying contract nonce from database")?;

        if let Some(nonce) = nonce {
            return Ok(Output(nonce));
        };

        // Early starknet contracts had no nonces, so its possible for a contract to
        // exist without having the nonce explicitly set to zero on deployment.
        let contract_exists = tx
            .contract_exists(input.contract_address, block_id)
            .context("Checking contract exists")?;

        if contract_exists {
            Ok(Output(ContractNonce::ZERO))
        } else {
            Err(Error::ContractNotFound)
        }
    })
    .await
    .context("Joining blocking task")?
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::{BlockNumber, ContractNonce};

    use super::{get_nonce, Error, Input};
    use crate::context::RpcContext;
    use crate::types::BlockId;
    use crate::RpcVersion;

    const RPC_VERSION: RpcVersion = RpcVersion::V09;

    #[tokio::test]
    async fn contract_not_found() {
        let context = RpcContext::for_tests();

        let input = Input {
            block_id: BlockId::Latest,
            contract_address: contract_address_bytes!(b"invalid"),
        };

        let result = get_nonce(context, input, RPC_VERSION).await;

        assert_matches!(result, Err(Error::ContractNotFound));
    }

    #[tokio::test]
    async fn block_not_found() {
        let context = RpcContext::for_tests();

        let input = Input {
            block_id: BlockId::Hash(block_hash_bytes!(b"invalid")),
            // This contract does exist and is added in block 0.
            contract_address: contract_address_bytes!(b"contract 0"),
        };

        let result = get_nonce(context, input, RPC_VERSION).await;

        assert_matches!(result, Err(Error::BlockNotFound));
    }

    #[tokio::test]
    async fn l1_accepted() {
        let context = RpcContext::for_tests();

        // This contract is created in `setup_storage` at block 1,
        // so we expect the nonce to be 0x0 at block 1 (L1 accepted)
        let input = Input {
            block_id: BlockId::L1Accepted,
            contract_address: contract_address_bytes!(b"contract 1"),
        };
        let nonce = get_nonce(context, input, RPC_VERSION).await.unwrap();
        assert_eq!(nonce.0, contract_nonce!("0x0"));
    }

    #[tokio::test]
    async fn latest() {
        let context = RpcContext::for_tests();

        // This contract is created in `setup_storage` and has a nonce set to 0x1.
        let input = Input {
            block_id: BlockId::Latest,
            contract_address: contract_address_bytes!(b"contract 0"),
        };
        let nonce = get_nonce(context, input, RPC_VERSION).await.unwrap();
        assert_eq!(nonce.0, contract_nonce!("0x1"));
    }

    #[tokio::test]
    async fn at_block() {
        let context = RpcContext::for_tests();

        // This contract is created in `setup_storage` at block 1,
        // but only gets nonce=0x10 explicitly set in block 2.
        let input = Input {
            block_id: BlockNumber::new_or_panic(2).into(),
            contract_address: contract_address_bytes!(b"contract 1"),
        };
        let nonce = get_nonce(context, input, RPC_VERSION).await.unwrap();
        assert_eq!(nonce.0, contract_nonce!("0x10"));
    }

    #[tokio::test]
    async fn pre_confirmed() {
        let context = RpcContext::for_tests_with_pre_confirmed().await;

        // This contract is created in `setup_storage` and has a nonce set in the
        // pending block.
        let input = Input {
            block_id: BlockId::PreConfirmed,
            contract_address: contract_address_bytes!(b"contract 1"),
        };
        let nonce = get_nonce(context.clone(), input.clone(), RpcVersion::V09)
            .await
            .unwrap();
        assert_eq!(nonce.0, contract_nonce_bytes!(b"preconfirmed nonce"));
    }

    #[tokio::test]
    async fn pre_latest() {
        let context = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;

        // This contract is created during storage setup and has a nonce set in the
        // pre-latest block.
        let input = Input {
            block_id: BlockId::PreConfirmed,
            contract_address: contract_address_bytes!(b"prelatest contract 1 address"),
        };
        let nonce = get_nonce(context.clone(), input.clone(), RpcVersion::V09)
            .await
            .unwrap();
        assert_eq!(nonce.0, contract_nonce_bytes!(b"prelatest nonce"));
    }

    #[tokio::test]
    async fn pre_confirmed_defaults_to_latest() {
        let context = RpcContext::for_tests();

        // This contract is created in `setup_storage` and has a nonce set to 0x1, and
        // is not overwritten in pre confirmed (since this test does not specify any
        // pending data).
        let input = Input {
            block_id: BlockId::PreConfirmed,
            contract_address: contract_address_bytes!(b"contract 0"),
        };
        let nonce = get_nonce(context, input, RPC_VERSION).await.unwrap();
        assert_eq!(nonce.0, contract_nonce!("0x1"));
    }

    #[tokio::test]
    async fn defaults_to_zero() {
        let context = RpcContext::for_tests();

        // This contract is created in `setup_storage` at block 1,
        // but only gets a nonce explicitly set in block 2.
        let input = Input {
            block_id: BlockNumber::new_or_panic(1).into(),
            contract_address: contract_address_bytes!(b"contract 1"),
        };
        let nonce = get_nonce(context, input, RPC_VERSION).await.unwrap();

        assert_eq!(nonce.0, ContractNonce::ZERO);
    }

    #[tokio::test]
    async fn contract_deployed_in_pre_confirmed_defaults_to_zero() {
        let context = RpcContext::for_tests_with_pre_confirmed().await;

        // This contract is deployed in the pre-confirmed block but does not have a
        // nonce update.
        let input = Input {
            block_id: BlockId::PreConfirmed,
            contract_address: contract_address_bytes!(b"preconfirmed contract 0 address"),
        };
        let nonce = get_nonce(context, input, RPC_VERSION).await.unwrap();
        assert_eq!(nonce.0, ContractNonce::ZERO);
    }
}
