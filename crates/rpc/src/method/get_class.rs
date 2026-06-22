use anyhow::Context;
use pathfinder_common::ClassHash;

use crate::context::RpcContext;
use crate::dto::SerializeForVersion;
use crate::types::class::cairo::CairoContractClass;
use crate::types::class::sierra::SierraContractClass;
use crate::types::{BlockId, ContractClass};
use crate::{dto, RpcVersion};

crate::error::generate_rpc_error_subset!(Error: BlockNotFound, ClassHashNotFound);

#[derive(Debug, PartialEq, Eq)]
pub struct Input {
    block_id: BlockId,
    class_hash: pathfinder_common::ClassHash,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                block_id: value.deserialize("block_id")?,
                class_hash: ClassHash(value.deserialize("class_hash")?),
            })
        })
    }
}

#[derive(Debug)]
pub enum Output {
    DeprecatedClass(CairoContractClass),
    Class(SierraContractClass),
}

impl From<ContractClass> for Output {
    fn from(value: ContractClass) -> Self {
        match value {
            ContractClass::Cairo(x) => Self::DeprecatedClass(x),
            ContractClass::Sierra(x) => Self::Class(x),
        }
    }
}

/// Get a contract class.
pub async fn get_class(
    context: RpcContext,
    input: Input,
    _rpc_version: RpcVersion,
) -> Result<Output, Error> {
    let span = tracing::Span::current();
    let jh = util::task::spawn_blocking(move |_| -> Result<Output, Error> {
        let _g = span.enter();
        let mut db = context
            .storage
            .connection()
            .context("Opening database connection")?;
        let tx = db.transaction().context("Creating database transaction")?;

        let is_pending = input.block_id.is_pending()
            && context
                .pending_data
                .get(&tx)?
                .class_is_declared(input.class_hash);

        let block_id = input
            .block_id
            .to_common_coerced(&tx)
            .map_err(|_| Error::BlockNotFound)?;
        if !tx.block_exists(block_id)? {
            return Err(Error::BlockNotFound);
        }

        // If the class is declared in the pending block, then we shouldn't check the
        // class's declaration point.
        let definition = if is_pending {
            tx.class_definition(input.class_hash)
        } else {
            tx.class_definition_at(block_id, input.class_hash)
        }
        .context("Fetching class definition")?;

        let Some(definition) = definition else {
            return Err(Error::ClassHashNotFound);
        };

        let class = ContractClass::from_serialized_def(&definition)
            .context("Parsing class definition")?
            .into();

        Ok(class)
    });

    jh.await.context("Reading class from database")?
}

impl SerializeForVersion for Output {
    fn serialize(&self, serializer: dto::Serializer) -> Result<dto::Ok, dto::Error> {
        match self {
            Output::DeprecatedClass(cairo) => cairo.serialize(serializer),
            Output::Class(sierra) => sierra.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use pathfinder_common::macro_prelude::*;

    use super::*;

    mod parsing {
        use dto::DeserializeForVersion;
        use serde_json::json;

        use super::*;
        use crate::RpcVersion;

        #[test]
        fn positional_args() {
            let positional = json!([
                { "block_hash": "0xabcde" },
                "0x12345"
            ]);

            let input =
                Input::deserialize(crate::dto::Value::new(positional, RpcVersion::V09)).unwrap();
            let expected = Input {
                block_id: block_hash!("0xabcde").into(),
                class_hash: class_hash!("0x12345"),
            };
            assert_eq!(input, expected);
        }

        #[test]
        fn named_args() {
            let named = json!({
                "block_id": { "block_hash": "0xabcde" },
                "class_hash": "0x12345"
            });

            let input = Input::deserialize(crate::dto::Value::new(named, RpcVersion::V09)).unwrap();
            let expected = Input {
                block_id: block_hash!("0xabcde").into(),
                class_hash: class_hash!("0x12345"),
            };
            assert_eq!(input, expected);
        }
    }

    const RPC_VERSION: RpcVersion = RpcVersion::V09;

    #[tokio::test]
    async fn pre_confirmed() {
        let context = RpcContext::for_tests_with_pre_confirmed().await;

        // Cairo v0.x class
        let valid_v0 = class_hash_bytes!(b"class 0 hash");
        super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::PreConfirmed,
                class_hash: valid_v0,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();
        // Cairo v1.x class (Sierra)
        let valid_v1 = class_hash_bytes!(b"class 2 hash (sierra)");
        super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::PreConfirmed,
                class_hash: valid_v1,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();
        let valid_pre_confirmed = class_hash_bytes!(b"preconfirmed class 0 hash");
        super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::PreConfirmed,
                class_hash: valid_pre_confirmed,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        let invalid = class_hash_bytes!(b"invalid");
        let error = super::get_class(
            context,
            Input {
                block_id: BlockId::PreConfirmed,
                class_hash: invalid,
            },
            RPC_VERSION,
        )
        .await
        .unwrap_err();

        assert_matches!(error, Error::ClassHashNotFound);
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[tokio::test]
    async fn pre_latest_and_pre_confirmed(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;

        let valid_pre_latest = class_hash_bytes!(b"prelatest class 0 hash");
        let r = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::PreConfirmed,
                class_hash: valid_pre_latest,
            },
            version,
        )
        .await;
        r.unwrap();

        let valid_pre_confirmed = class_hash_bytes!(b"preconfirmed class 0 hash");
        let r = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::PreConfirmed,
                class_hash: valid_pre_confirmed,
            },
            version,
        )
        .await;
        r.unwrap();
    }

    #[tokio::test]
    async fn latest() {
        let context = RpcContext::for_tests();

        // Cairo v0.x class
        let valid_v0 = class_hash_bytes!(b"class 0 hash");
        super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Latest,
                class_hash: valid_v0,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        // Cairo v1.x class (Sierra)
        let valid_v1 = class_hash_bytes!(b"class 2 hash (sierra)");
        super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Latest,
                class_hash: valid_v1,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        let invalid = class_hash_bytes!(b"invalid");
        let error = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Latest,
                class_hash: invalid,
            },
            RPC_VERSION,
        )
        .await
        .unwrap_err();
        assert_matches!(error, Error::ClassHashNotFound);

        // This class is defined, but is not declared in any canonical block, such
        // as what may occur for a pending class declaration.
        let undeclared = class_hash_bytes!(b"class pending hash");
        let error = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Latest,
                class_hash: undeclared,
            },
            RPC_VERSION,
        )
        .await
        .unwrap_err();
        assert_matches!(error, Error::ClassHashNotFound);
    }

    #[tokio::test]
    async fn at_number() {
        use pathfinder_common::BlockNumber;

        let context = RpcContext::for_tests();

        // Cairo v0.x class
        // This class is declared in block 0.
        let valid_v0 = class_hash_bytes!(b"class 0 hash");
        super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Number(BlockNumber::new_or_panic(1)),
                class_hash: valid_v0,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        // Cairo v1.x class (Sierra)
        // This class is declared in block 2.
        let valid_v1 = class_hash_bytes!(b"class 2 hash (sierra)");
        super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Number(BlockNumber::new_or_panic(2)),
                class_hash: valid_v1,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        let error = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Number(BlockNumber::GENESIS),
                class_hash: valid_v1,
            },
            RPC_VERSION,
        )
        .await
        .unwrap_err();
        assert_matches!(error, Error::ClassHashNotFound);

        let invalid = class_hash_bytes!(b"invalid");
        let error = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Number(BlockNumber::new_or_panic(2)),
                class_hash: invalid,
            },
            RPC_VERSION,
        )
        .await
        .unwrap_err();
        assert_matches!(error, Error::ClassHashNotFound);

        // This class is defined, but is not declared in any canonical block, such
        // as what may occur for a pending class declaration.
        let undeclared = class_hash_bytes!(b"class pending hash");
        let error = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Number(BlockNumber::new_or_panic(2)),
                class_hash: undeclared,
            },
            RPC_VERSION,
        )
        .await
        .unwrap_err();
        assert_matches!(error, Error::ClassHashNotFound);

        // Class exists, but block number does not.
        let valid = class_hash_bytes!(b"class 0 hash");
        let error = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Number(BlockNumber::MAX),
                class_hash: valid,
            },
            RPC_VERSION,
        )
        .await
        .unwrap_err();
        assert_matches!(error, Error::BlockNotFound);
    }

    #[tokio::test]
    async fn read_at_hash() {
        let context = RpcContext::for_tests();

        // Cairo v0.x class
        // This class is declared in block 1.
        let valid_v0 = class_hash_bytes!(b"class 0 hash");
        let block1_hash = block_hash_bytes!(b"block 1");
        super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Hash(block1_hash),
                class_hash: valid_v0,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        // Cairo v1.x class
        // This class is declared in block 2.
        let valid_v1 = class_hash_bytes!(b"class 2 hash (sierra)");
        let block2_hash = block_hash_bytes!(b"latest");
        super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Hash(block2_hash),
                class_hash: valid_v1,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        let block0_hash = block_hash_bytes!(b"genesis");
        let error = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Hash(block0_hash),
                class_hash: valid_v1,
            },
            RPC_VERSION,
        )
        .await
        .unwrap_err();
        assert_matches!(error, Error::ClassHashNotFound);

        let invalid = class_hash_bytes!(b"invalid");
        let latest_hash = block_hash_bytes!(b"latest");
        let error = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Hash(latest_hash),
                class_hash: invalid,
            },
            RPC_VERSION,
        )
        .await
        .unwrap_err();
        assert_matches!(error, Error::ClassHashNotFound);

        // This class is defined, but is not declared in any canonical block.
        let undeclared = class_hash_bytes!(b"class pending hash");
        let latest_hash = block_hash_bytes!(b"latest");
        let error = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Hash(latest_hash),
                class_hash: undeclared,
            },
            RPC_VERSION,
        )
        .await
        .unwrap_err();
        assert_matches!(error, Error::ClassHashNotFound);

        // Class exists, but block hash does not.
        let valid = class_hash_bytes!(b"class 0 hash");
        let invalid_block = block_hash_bytes!(b"invalid");
        let error = super::get_class(
            context.clone(),
            Input {
                block_id: BlockId::Hash(invalid_block),
                class_hash: valid,
            },
            RPC_VERSION,
        )
        .await
        .unwrap_err();
        assert_matches!(error, Error::BlockNotFound);
    }
}
