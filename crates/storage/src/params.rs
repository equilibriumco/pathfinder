use anyhow::Result;
use pathfinder_common::prelude::*;
use pathfinder_common::{BlockCommitmentSignatureElem, L1BlockNumber};
use pathfinder_crypto::Felt;
use rusqlite::types::{FromSqlError, ToSqlOutput};
use rusqlite::RowIndex;

use crate::TrieStorageIndex;

pub trait ToSql {
    fn to_sql(&self) -> ToSqlOutput<'_>;
}

pub trait TryIntoSql {
    #[allow(dead_code)]
    fn try_into_sql(&self) -> Result<ToSqlOutput<'_>>;
}

pub trait TryIntoSqlInt {
    fn try_into_sql_int(&self) -> Result<i64>;
}

impl<Inner: ToSql> ToSql for Option<Inner> {
    fn to_sql(&self) -> ToSqlOutput<'_> {
        use rusqlite::types::Value;

        match self {
            Some(value) => value.to_sql(),
            None => ToSqlOutput::Owned(Value::Null),
        }
    }
}

impl ToSql for TrieStorageIndex {
    fn to_sql(&self) -> ToSqlOutput<'_> {
        use rusqlite::types::Value;
        ToSqlOutput::Owned(Value::Integer(self.0 as i64))
    }
}

impl ToSql for StarknetVersion {
    fn to_sql(&self) -> ToSqlOutput<'_> {
        use rusqlite::types::Value;
        ToSqlOutput::Owned(Value::Text(self.to_string()))
    }
}

impl ToSql for L1DataAvailabilityMode {
    fn to_sql(&self) -> ToSqlOutput<'_> {
        let value = match self {
            L1DataAvailabilityMode::Calldata => 0,
            L1DataAvailabilityMode::Blob => 1,
        };
        ToSqlOutput::Owned(rusqlite::types::Value::Integer(value))
    }
}

to_sql_felt!(
    BlockHash,
    BlockCommitmentSignatureElem,
    ByteCodeOffset,
    CallParam,
    CallResultValue,
    CasmHash,
    ClassCommitment,
    ClassCommitmentLeafHash,
    ClassHash,
    ConstructorParam,
    ContractAddress,
    ContractAddressSalt,
    ContractStateHash,
    ContractRoot,
    EntryPoint,
    EventCommitment,
    EventData,
    EventKey,
    Fee,
    L1ToL2MessageNonce,
    L1ToL2MessagePayloadElem,
    L2ToL1MessagePayloadElem,
    ReceiptCommitment,
    SequencerAddress,
    SierraHash,
    TransactionHash,
    StateCommitment,
    StateDiffCommitment,
    StorageAddress,
    StorageCommitment,
    TransactionCommitment,
    TransactionSignatureElem,
);

to_sql_compressed_felt!(ContractNonce, StorageValue, TransactionNonce);

to_sql_int!(BlockNumber, BlockTimestamp);
to_sql_int!(L1BlockNumber);

to_sql_builtin!(
    String,
    &str,
    Vec<u8>,
    &[u8],
    isize,
    usize,
    i64,
    i32,
    i16,
    i8,
    u64,
    u32,
    u16,
    u8
);

try_into_sql!(usize, u64);

try_into_sql_int!(usize, u64);

/// Extends [rusqlite::Row] to provide getters for our own foreign types. This
/// is a work-around for the orphan rule -- our types live in a separate crate
/// and can therefore not implement the rusqlite traits.
pub trait RowExt {
    fn get_blob<I: RowIndex>(&self, index: I) -> rusqlite::Result<&[u8]>;

    fn get_i64<I: RowIndex>(&self, index: I) -> rusqlite::Result<i64>;

    fn get_optional_i64<I: RowIndex>(&self, index: I) -> rusqlite::Result<Option<i64>>;

    fn get_optional_str<I: RowIndex>(&self, index: I) -> rusqlite::Result<Option<&str>>;

    fn get_optional_blob<I: RowIndex>(&self, index: I) -> rusqlite::Result<Option<&[u8]>>;

    fn get_felt<Index: RowIndex>(&self, index: Index) -> rusqlite::Result<Felt> {
        let blob = self.get_blob(index)?;
        let felt = Felt::from_be_slice(blob)
            .map_err(|e| rusqlite::types::FromSqlError::Other(e.into()))?;
        Ok(felt)
    }

    fn get_optional_felt<Index: RowIndex>(&self, index: Index) -> rusqlite::Result<Option<Felt>> {
        let Some(blob) = self.get_optional_blob(index)? else {
            return Ok(None);
        };

        let felt = Felt::from_be_slice(blob)
            .map_err(|e| rusqlite::types::FromSqlError::Other(e.into()))?;
        Ok(Some(felt))
    }

    fn get_optional_block_number<Index: RowIndex>(
        &self,
        index: Index,
    ) -> rusqlite::Result<Option<BlockNumber>> {
        let num = self
            .get_optional_i64(index)?
            // Always safe since we are fetching an i64
            .map(|x| BlockNumber::new_or_panic(x as u64));
        Ok(num)
    }

    fn get_optional_casm_hash<Index: RowIndex>(
        &self,
        index: Index,
    ) -> rusqlite::Result<Option<CasmHash>> {
        Ok(self.get_optional_felt(index)?.map(CasmHash))
    }

    fn get_optional_state_commitment<Index: RowIndex>(
        &self,
        index: Index,
    ) -> rusqlite::Result<Option<StateCommitment>> {
        Ok(self.get_optional_felt(index)?.map(StateCommitment))
    }

    fn get_optional_class_commitment<Index: RowIndex>(
        &self,
        index: Index,
    ) -> rusqlite::Result<Option<ClassCommitment>> {
        Ok(self.get_optional_felt(index)?.map(ClassCommitment))
    }

    fn get_block_number<Index: RowIndex>(&self, index: Index) -> rusqlite::Result<BlockNumber> {
        let num = self.get_i64(index)?;
        // Always safe since we are fetching an i64
        Ok(BlockNumber::new_or_panic(num as u64))
    }

    fn get_gas_price<Index: RowIndex>(&self, index: Index) -> rusqlite::Result<GasPrice> {
        let blob = self.get_blob(index)?;
        let gas_price = GasPrice::from_be_slice(blob).map_err(|e| FromSqlError::Other(e.into()))?;
        Ok(gas_price)
    }

    fn get_optional_gas_price<Index: RowIndex>(
        &self,
        index: Index,
    ) -> rusqlite::Result<Option<GasPrice>> {
        let Some(blob) = self.get_optional_blob(index)? else {
            return Ok(None);
        };

        let gas_price = GasPrice::from_be_slice(blob).map_err(|e| FromSqlError::Other(e.into()))?;
        Ok(Some(gas_price))
    }

    fn get_timestamp<Index: RowIndex>(&self, index: Index) -> rusqlite::Result<BlockTimestamp> {
        let num = self.get_i64(index)?;
        // Always safe since we are fetching an i64
        Ok(BlockTimestamp::new_or_panic(num as u64))
    }

    fn get_starknet_version<Index: RowIndex>(
        &self,
        index: Index,
    ) -> rusqlite::Result<StarknetVersion> {
        let v: u32 = self.get_i64(index)?.try_into().unwrap();
        Ok(StarknetVersion::from_u32(v))
    }

    fn get_transaction_commitment<Index: RowIndex>(
        &self,
        index: Index,
    ) -> rusqlite::Result<TransactionCommitment> {
        Ok(self
            .get_optional_felt(index)?
            .map(TransactionCommitment)
            .unwrap_or_default())
    }

    fn get_event_commitment<Index: RowIndex>(
        &self,
        index: Index,
    ) -> rusqlite::Result<EventCommitment> {
        Ok(self
            .get_optional_felt(index)?
            .map(EventCommitment)
            .unwrap_or_default())
    }

    fn get_contract_address<Index: RowIndex>(
        &self,
        index: Index,
    ) -> rusqlite::Result<ContractAddress> {
        let felt = self.get_felt(index)?;

        let addr = ContractAddress::new(felt).ok_or(rusqlite::types::FromSqlError::Other(
            anyhow::anyhow!("contract address out of range").into(),
        ))?;

        Ok(addr)
    }

    fn get_optional_class_hash<Index: RowIndex>(
        &self,
        index: Index,
    ) -> rusqlite::Result<Option<ClassHash>> {
        Ok(self.get_optional_felt(index)?.map(ClassHash))
    }

    fn get_l1_da_mode<Index: RowIndex>(
        &self,
        index: Index,
    ) -> rusqlite::Result<L1DataAvailabilityMode> {
        let num = self.get_i64(index)?;
        let mode = match num {
            0 => L1DataAvailabilityMode::Calldata,
            1 => L1DataAvailabilityMode::Blob,
            _ => {
                return Err(rusqlite::types::FromSqlError::Other(
                    anyhow::anyhow!("invalid L1 data availability mode {num}").into(),
                )
                .into())
            }
        };
        Ok(mode)
    }

    row_felt_wrapper!(get_block_hash, BlockHash);
    row_felt_wrapper!(get_casm_hash, CasmHash);
    row_felt_wrapper!(get_class_hash, ClassHash);
    row_felt_wrapper!(get_state_commitment, StateCommitment);
    row_felt_wrapper!(get_state_diff_commitment, StateDiffCommitment);
    row_felt_wrapper!(get_sequencer_address, SequencerAddress);
    row_felt_wrapper!(get_transaction_hash, TransactionHash);
    row_felt_wrapper!(get_class_commitment_leaf, ClassCommitmentLeafHash);
    row_felt_wrapper!(
        get_block_commitment_signature_elem,
        BlockCommitmentSignatureElem
    );
    row_felt_wrapper!(get_receipt_commitment, ReceiptCommitment);
}

impl RowExt for &rusqlite::Row<'_> {
    fn get_blob<I: RowIndex>(&self, index: I) -> rusqlite::Result<&[u8]> {
        self.get_ref(index)?.as_blob().map_err(|e| e.into())
    }

    fn get_optional_blob<I: RowIndex>(&self, index: I) -> rusqlite::Result<Option<&[u8]>> {
        self.get_ref(index)?.as_blob_or_null().map_err(|e| e.into())
    }

    fn get_i64<I: RowIndex>(&self, index: I) -> rusqlite::Result<i64> {
        self.get_ref(index)?.as_i64().map_err(|e| e.into())
    }

    fn get_optional_i64<I: RowIndex>(&self, index: I) -> rusqlite::Result<Option<i64>> {
        self.get_ref(index)?.as_i64_or_null().map_err(|e| e.into())
    }

    fn get_optional_str<I: RowIndex>(&self, index: I) -> rusqlite::Result<Option<&str>> {
        self.get_ref(index)?.as_str_or_null().map_err(|e| e.into())
    }
}

/// Implements [ToSql] for the target [Felt] newtype.
///
/// Writes the full underlying bytes (no compression).
macro_rules! to_sql_felt {
    ($target:ty) => {
        impl ToSql for $target {
            fn to_sql(&self) -> rusqlite::types::ToSqlOutput<'_> {
                use rusqlite::types::{ToSqlOutput, ValueRef};
                ToSqlOutput::Borrowed(ValueRef::Blob(self.as_inner().as_be_bytes()))
            }
        }
    };
    ($head:ty, $($rest:ty),+  $(,)?) => {
        to_sql_felt!($head);
        to_sql_felt!($($rest),+);
    }
}

/// Implements [ToSql] for the target [Felt] newtype.
///
/// Same as [to_sql_felt!] except it compresses the [Felt] by skipping leading
/// zeros.
///
/// [Felt]: pathfinder_crypto::Felt
macro_rules! to_sql_compressed_felt {
    ($target:ty) => {
        impl ToSql for $target {
            fn to_sql(&self) -> rusqlite::types::ToSqlOutput<'_> {
                use rusqlite::types::{ToSqlOutput, ValueRef};
                let bytes = self.0.as_be_bytes();
                let num_zeroes = bytes.iter().take_while(|v| **v == 0).count();
                ToSqlOutput::Borrowed(ValueRef::Blob(&bytes[num_zeroes..]))
            }
        }
    };
    ($head:ty, $($rest:ty),+  $(,)?) => {
        to_sql_compressed_felt!($head);
        to_sql_compressed_felt!($($rest),+);
    }
}

/// Implements [ToSql] for the target integer newtype.
macro_rules! to_sql_int {
    ($target:ty) => {
        impl ToSql for $target {
            fn to_sql(&self) -> rusqlite::types::ToSqlOutput<'_> {
                use rusqlite::types::{ToSqlOutput, Value};
                ToSqlOutput::Owned(Value::Integer(self.get() as i64))
            }
        }
    };
    ($head:ty, $($rest:ty),+  $(,)?) => {
        to_sql_int!($head);
        to_sql_int!($($rest),+);
    }
}

macro_rules! to_sql_builtin {
    ($target:ty) => {
        impl ToSql for $target {
            fn to_sql(&self) -> rusqlite::types::ToSqlOutput<'_> {
                rusqlite::ToSql::to_sql(self).unwrap()
            }
        }
    };
    ($head:ty, $($rest:ty),+  $(,)?) => {
        to_sql_builtin!($head);
        to_sql_builtin!($($rest),+);
    }
}

macro_rules! try_into_sql {
    ($target:ty) => {
        impl TryIntoSql for $target {
            fn try_into_sql(&self) -> anyhow::Result<rusqlite::types::ToSqlOutput<'_>> {
                use rusqlite::types::{ToSqlOutput, Value};
                Ok(ToSqlOutput::Owned(Value::Integer(i64::try_from(*self)?)))
            }
        }
    };
    ($head:ty, $($rest:ty),+  $(,)?) => {
        try_into_sql!($head);
        try_into_sql!($($rest),+);
    }
}

macro_rules! try_into_sql_int {
    ($target:ty) => {
        impl TryIntoSqlInt for $target {
            fn try_into_sql_int(&self) -> anyhow::Result<i64> {
                Ok(i64::try_from(*self)?)
            }
        }
    };
    ($head:ty, $($rest:ty),+  $(,)?) => {
        try_into_sql_int!($head);
        try_into_sql_int!($($rest),+);
    }
}

macro_rules! row_felt_wrapper {
    ($fn_name:ident, $Type:ident) => {
        fn $fn_name<I: RowIndex>(&self, index: I) -> rusqlite::Result<$Type> {
            let felt = self.get_felt(index)?;
            Ok($Type(felt))
        }
    };
}

use row_felt_wrapper;
use to_sql_builtin;
use to_sql_compressed_felt;
use to_sql_felt;
use to_sql_int;
use try_into_sql;
use try_into_sql_int;

/// Used in combination with our own [ToSql] trait to provide functionality
/// equivalent to [rusqlite::params!] for our own foreign types.
macro_rules! params {
    [] => {
        rusqlite::params![]
    };
    [$($param:expr),+ $(,)?] => {
        rusqlite::params![$(&$crate::params::ToSql::to_sql($param)),+]
    };
}

macro_rules! named_params {
    () => {
        rusqlite::named_params![]
    };
    // Note: It's a lot more work to support this as part of the same macro as
    // `params!`, unfortunately.
    ($($param_name:literal: $param_val:expr),+ $(,)?) => {
        rusqlite::named_params![$($param_name: $crate::params::ToSql::to_sql($param_val)),+]
    };
}

pub(crate) use named_params;
pub(crate) use params;

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;

    use super::*;

    #[test]
    fn to_sql() {
        // Exercises to_sql! and params! in a roundtrip to and from storage trip.

        let original = class_hash!("0xdeadbeef");

        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute("CREATE TABLE test (data BLOB)", []).unwrap();
        db.execute("INSERT INTO test VALUES(?)", params![&original])
            .unwrap();

        let result = db
            .query_row("SELECT data FROM test", [], |row| row.get_class_hash(0))
            .unwrap();

        assert_eq!(result, original);
    }
}
