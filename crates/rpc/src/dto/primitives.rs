pub use boolean::*;
pub use numerics::*;
pub use pathfinder_common_types::*;
pub use pathfinder_crypto::*;
pub use pathfinder_primitives::*;
pub use strings::*;

pub mod hex_str {
    use std::borrow::Cow;

    use anyhow::anyhow;

    const LUT: [u8; 16] = *b"0123456789abcdef";

    pub fn bytes_to_hex_str_stripped(data: &[u8]) -> Cow<'static, str> {
        // Skip empty bytes
        let zero_count = data.iter().take_while(|b| **b == 0).count();
        let data = &data[zero_count..];

        if data.is_empty() {
            return Cow::from("0x0");
        }

        // Is the most significant nibble zero? We need to skip it if it is.
        // Index is safe since we just checked that it is non-empty.
        let zero_nibble = (0xF0 & data[0]) == 0;
        to_hex_str(data, zero_nibble).into()
    }

    pub fn bytes_to_hex_str_full(data: &[u8]) -> String {
        to_hex_str(data, false)
    }

    #[inline]
    fn to_hex_str(data: &[u8], skip_first_nibble: bool) -> String {
        let count = if skip_first_nibble {
            data.len() * 2 - 1
        } else {
            data.len() * 2
        };
        let mut buf = vec![0; 2 + count];
        buf[0] = b'0';
        buf[1] = b'x';

        // Handle the first byte separately as we may need to skip the first nibble.
        let (offset, data) = if skip_first_nibble {
            buf[2] = LUT[data[0] as usize & 0x0f];
            (3, &data[1..])
        } else {
            (2, data)
        };

        // Same small lookup table is ~25% faster than hex::encode_from_slice 🤷
        for (i, b) in data.iter().enumerate() {
            let idx = *b as usize;
            let pos = offset + i * 2;
            buf[pos] = LUT[(idx & 0xf0) >> 4];
            buf[pos + 1] = LUT[idx & 0x0f];
        }

        // SAFETY: we only insert hex digits.
        unsafe { String::from_utf8_unchecked(buf) }
    }

    pub fn bytes_from_hex_str_stripped<const N: usize>(hex_str: &str) -> anyhow::Result<[u8; N]> {
        from_hex_str(hex_str, true)
    }

    #[inline]
    fn from_hex_str<const N: usize>(hex_str: &str, stripped: bool) -> anyhow::Result<[u8; N]> {
        fn parse_hex_digit(digit: u8) -> anyhow::Result<u8> {
            match digit {
                b'0'..=b'9' => Ok(digit - b'0'),
                b'A'..=b'F' => Ok(digit - b'A' + 10),
                b'a'..=b'f' => Ok(digit - b'a' + 10),
                other => Err(anyhow!("invalid hex digit: {other}")),
            }
        }

        let bytes = hex_str.as_bytes();
        let start = if bytes.len() >= 2 && bytes[0] == b'0' && bytes[1] == b'x' {
            2
        } else {
            0
        };
        let len = bytes.len() - start;

        if len > 2 * N {
            return Err(anyhow!(
                "hex string too long: expected at most 64 characters, got {len}",
            ));
        }

        let mut buf = [0u8; N];

        if stripped {
            // Handle a possible odd nibble remaining nibble.
            if len % 2 == 1 {
                let idx = len / 2;
                buf[N - 1 - idx] = parse_hex_digit(bytes[start])?;
            }
        } else if len != 2 * N {
            return Err(anyhow!(
                "hex string too short: expected 64 characters, got {len}"
            ));
        }

        let chunks = len / 2;
        let mut chunk = 0;

        while chunk < chunks {
            let lower = parse_hex_digit(bytes[bytes.len() - chunk * 2 - 1])?;
            let upper = parse_hex_digit(bytes[bytes.len() - chunk * 2 - 2])?;
            buf[N - 1 - chunk] = (upper << 4) | lower;
            chunk += 1;
        }

        Ok(buf)
    }
}

mod boolean {
    use serde::de::Error;

    use crate::dto::{self, DeserializeForVersion, SerializeForVersion, Serializer, Value};

    impl SerializeForVersion for bool {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_bool(*self)
        }
    }

    impl DeserializeForVersion for bool {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::Bool(b) => Ok(*b),
                _ => Err(serde_json::Error::custom("expected boolean")),
            }
        }
    }
}

mod numerics {
    use std::num::NonZeroU64;

    use serde::de::Error;

    use crate::dto::{self, DeserializeForVersion, SerializeForVersion, Serializer, Value};

    impl SerializeForVersion for u128 {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_u128(*self)
        }
    }

    impl SerializeForVersion for u64 {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_u64(*self)
        }
    }

    impl SerializeForVersion for &u64 {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_u64(**self)
        }
    }

    impl DeserializeForVersion for u64 {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::Number(n) => n
                    .as_u64()
                    .ok_or_else(|| serde_json::Error::custom("invalid u64 value")),
                _ => Err(serde_json::Error::custom("expected number")),
            }
        }
    }

    impl SerializeForVersion for u32 {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_u32(*self)
        }
    }

    impl DeserializeForVersion for u32 {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::Number(n) => n
                    .as_u64()
                    .and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(|| serde_json::Error::custom("value is too large for u32")),
                _ => Err(serde_json::Error::custom("expected number")),
            }
        }
    }

    impl SerializeForVersion for i64 {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_i64(*self)
        }
    }

    impl SerializeForVersion for i32 {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_i32(*self)
        }
    }

    impl DeserializeForVersion for i32 {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::Number(n) => n
                    .as_i64()
                    .and_then(|n| i32::try_from(n).ok())
                    .ok_or_else(|| serde_json::Error::custom("value is outside i32 range")),
                _ => Err(serde_json::Error::custom("expected number")),
            }
        }
    }

    impl SerializeForVersion for usize {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_u64(*self as u64)
        }
    }

    impl DeserializeForVersion for usize {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::Number(n) => n
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| serde_json::Error::custom("value is outside usize range")),
                _ => Err(serde_json::Error::custom("expected number")),
            }
        }
    }

    impl SerializeForVersion for NonZeroU64 {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_u64(self.get())
        }
    }

    impl SerializeForVersion for f32 {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_f32(*self)
        }
    }

    impl SerializeForVersion for f64 {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_f64(*self)
        }
    }
}

mod strings {
    use serde::de::Error;

    use crate::dto::{self, DeserializeForVersion, SerializeForVersion, Serializer, Value};

    impl SerializeForVersion for String {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_str(self)
        }
    }

    impl SerializeForVersion for &String {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_str(self)
        }
    }

    impl DeserializeForVersion for String {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::String(s) => Ok(s.clone()),
                _ => Err(serde_json::Error::custom("expected string")),
            }
        }
    }

    impl SerializeForVersion for &str {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_str(self)
        }
    }
}

mod pathfinder_primitives {
    use primitive_types::H256;
    use serde::de::Error;

    use super::hex_str;
    use crate::dto::{self, DeserializeForVersion, SerializeForVersion, Serializer, Value};

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct U64Hex(pub u64);

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct U128Hex(pub u128);

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct H256Hex(pub primitive_types::H256);

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct U256Hex(pub primitive_types::U256);

    impl SerializeForVersion for U64Hex {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(&self.0.to_be_bytes()))
        }
    }

    impl DeserializeForVersion for U64Hex {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::String(s) => {
                    let s = s.strip_prefix("0x").unwrap_or(s);
                    u64::from_str_radix(s, 16).map(Self).map_err(|e| {
                        serde_json::Error::custom(format!("invalid hex value for u64: {e}"))
                    })
                }
                _ => Err(serde_json::Error::custom("expected hex string")),
            }
        }
    }

    impl SerializeForVersion for U128Hex {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(&self.0.to_be_bytes()))
        }
    }

    impl DeserializeForVersion for U128Hex {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::String(s) => {
                    let bytes = hex_str::bytes_from_hex_str_stripped::<16>(s).map_err(|e| {
                        serde_json::Error::custom(format!(
                            "failed to parse hex string as u128: {e}"
                        ))
                    })?;
                    Ok(Self(u128::from_be_bytes(bytes)))
                }
                _ => Err(serde_json::Error::custom("expected hex string")),
            }
        }
    }

    impl SerializeForVersion for H256Hex {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_bytes()))
        }
    }

    impl SerializeForVersion for U256Hex {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(&self.0.to_big_endian()))
        }
    }

    impl DeserializeForVersion for H256 {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::String(hex_str) => {
                    let bytes =
                        hex_str::bytes_from_hex_str_stripped::<32>(hex_str).map_err(|e| {
                            serde_json::Error::custom(format!(
                                "failed to parse hex string as u256: {e}"
                            ))
                        })?;
                    Ok(H256(bytes))
                }
                _ => Err(serde_json::Error::custom("expected hex string")),
            }
        }
    }
}

mod pathfinder_crypto {
    use serde::de::Error;

    use super::hex_str;
    use crate::dto::{DeserializeForVersion, SerializeForVersion, Serializer, Value};

    impl SerializeForVersion for pathfinder_crypto::Felt {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for &pathfinder_crypto::Felt {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl DeserializeForVersion for pathfinder_crypto::Felt {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::String(hex_str) => {
                    let bytes =
                        hex_str::bytes_from_hex_str_stripped::<32>(hex_str).map_err(|e| {
                            serde_json::Error::custom(format!("failed to parse hex string: {e}"))
                        })?;
                    Self::from_be_bytes(bytes)
                        .map_err(|_| serde_json::Error::custom("felt overflow"))
                }
                _ => Err(serde_json::Error::custom("expected hex string")),
            }
        }
    }

    impl SerializeForVersion for crate::felt::RpcFelt {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            // StarkHash has a leading "0x" and at most 64 digits
            let mut buf = [0u8; 2 + 64];
            let s = self.0.as_hex_str(&mut buf);
            serializer.serialize_str(s)
        }
    }

    impl DeserializeForVersion for crate::felt::RpcFelt {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::String(s) => {
                    // Enforce 0x prefix
                    match s.as_bytes() {
                        &[b'0', b'x', ..] => pathfinder_crypto::Felt::from_hex_str(s)
                            .map_err(serde_json::Error::custom)
                            .map(crate::felt::RpcFelt),
                        _missing_prefix => Err(serde_json::Error::custom("Missing '0x' prefix")),
                    }
                }
                _ => Err(serde_json::Error::custom("expected hex string")),
            }
        }
    }

    impl SerializeForVersion for crate::felt::RpcFelt251 {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            // Delegate to inner RpcFelt's serialization
            self.0.serialize(serializer)
        }
    }

    impl DeserializeForVersion for crate::felt::RpcFelt251 {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            let felt: crate::felt::RpcFelt = DeserializeForVersion::deserialize(value)?;

            if felt.0.has_more_than_251_bits() {
                return Err(serde_json::Error::custom("Value exceeded 251 bits"));
            }

            Ok(crate::felt::RpcFelt251(felt))
        }
    }
}

mod pathfinder_common_types {
    use pathfinder_serde::bytes_as_hex_str;
    use primitive_types::H160;
    use serde::de::Error;

    use super::hex_str;
    use crate::dto::{self, DeserializeForVersion, SerializeForVersion, Serializer, Value};

    impl SerializeForVersion for &pathfinder_common::AccountDeploymentDataElem {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::BlockNumber {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_u64(self.get())
        }
    }

    impl SerializeForVersion for pathfinder_common::BlockHash {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::BlockTimestamp {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_u64(self.get())
        }
    }

    impl SerializeForVersion for pathfinder_common::ChainId {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for &pathfinder_common::CallParam {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for &pathfinder_common::CallResultValue {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::CasmHash {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for &pathfinder_common::CasmHash {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::ClassCommitment {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::ClassHash {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for &pathfinder_common::ClassHash {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::ContractAddress {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for &pathfinder_common::ContractAddress {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::ContractAddressSalt {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::ContractNonce {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::ContractRoot {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for &pathfinder_common::ConstructorParam {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::EntryPoint {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::EthereumAddress {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            let hex_str = hex_str::bytes_to_hex_str_full(self.0.as_bytes());
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::EventCommitment {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for &pathfinder_common::EventData {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for &pathfinder_common::EventKey {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl DeserializeForVersion for pathfinder_common::EthereumAddress {
        fn deserialize(value: Value) -> Result<Self, serde_json::Error> {
            match &value.data {
                serde_json::Value::String(hex_str) => {
                    let bytes =
                        hex_str::bytes_from_hex_str_stripped::<20>(hex_str).map_err(|e| {
                            serde_json::Error::custom(format!(
                                "failed to parse hex string as ethereum address: {e}"
                            ))
                        })?;
                    Ok(Self(H160::from(bytes)))
                }
                _ => Err(serde_json::Error::custom("expected hex string")),
            }
        }
    }

    impl SerializeForVersion for pathfinder_common::Fee {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::GasPrice {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_u128(self.0)
        }
    }

    impl SerializeForVersion for pathfinder_common::GasPriceHex {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let mut buf = [0u8; 2 + 32];
            let bytes = self.0.to_be_bytes();
            let s = bytes_as_hex_str(&bytes, &mut buf);
            serializer.serialize_str(s)
        }
    }

    impl SerializeForVersion for &pathfinder_common::L2ToL1MessagePayloadElem {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::receipt::L2Gas {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_u128(self.0)
        }
    }

    impl SerializeForVersion for pathfinder_common::L1DataAvailabilityMode {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            match self {
                pathfinder_common::L1DataAvailabilityMode::Calldata => {
                    serializer.serialize_str("CALLDATA")
                }
                pathfinder_common::L1DataAvailabilityMode::Blob => serializer.serialize_str("BLOB"),
            }
        }
    }

    impl SerializeForVersion for pathfinder_common::ProposalCommitment {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::ReceiptCommitment {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::ResourceAmount {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let bytes = self.0.to_be_bytes();
            let hex_str = pathfinder_serde::bytes_to_hex_str(&bytes);
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::ResourcePricePerUnit {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let bytes = self.0.to_be_bytes();
            let hex_str = pathfinder_serde::bytes_to_hex_str(&bytes);
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::SierraHash {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::StarknetVersion {
        fn serialize(
            &self,
            serializer: crate::dto::Serializer,
        ) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&self.to_string())
        }
    }

    impl SerializeForVersion for pathfinder_common::StateCommitment {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::StateDiffCommitment {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::StorageAddress {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::StorageCommitment {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::StorageValue {
        fn serialize(&self, serializer: Serializer) -> Result<dto::Ok, dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::SequencerAddress {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::Tip {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_u64(self.0)
        }
    }

    impl SerializeForVersion for pathfinder_common::TipHex {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            let bytes = self.0 .0.to_be_bytes();
            let hex_str = pathfinder_serde::bytes_to_hex_str(&bytes);
            serializer.serialize_str(&hex_str)
        }
    }

    impl SerializeForVersion for pathfinder_common::TransactionCommitment {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::TransactionHash {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for &pathfinder_common::TransactionHash {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::TransactionNonce {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for &pathfinder_common::TransactionSignatureElem {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for pathfinder_common::TransactionVersion {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for &pathfinder_common::PaymasterDataElem {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for &pathfinder_common::ProofFactElem {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            serializer.serialize_str(&hex_str::bytes_to_hex_str_stripped(self.0.as_be_bytes()))
        }
    }

    impl SerializeForVersion for &pathfinder_common::Proof {
        fn serialize(&self, serializer: Serializer) -> Result<crate::dto::Ok, crate::dto::Error> {
            use base64::Engine;

            let encoded = base64::engine::general_purpose::STANDARD.encode(&self.0);
            serializer.serialize_str(&encoded)
        }
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::{BlockNumber, ChainId, EthereumAddress};
    use pretty_assertions_sorted::assert_eq;
    use primitive_types::H160;
    use serde_json::json;

    use super::*;
    use crate::dto::{SerializeForVersion, Serializer};

    #[test]
    fn felt() {
        let uut = &felt!("0x1234");
        let expected = json!("0x1234");
        let encoded = uut.serialize(Default::default()).unwrap();

        assert_eq!(encoded, expected);
    }

    #[test]
    fn block_hash() {
        let hash = block_hash!("0x1234");
        let expected = &hash.0.serialize(Default::default()).unwrap();
        let encoded = &hash.serialize(Default::default()).unwrap();

        assert_eq!(encoded, expected);
    }

    #[test]
    fn block_number() {
        let number = BlockNumber::new_or_panic(1234);
        let expected = json!(1234);
        let encoded = number.serialize(Default::default()).unwrap();

        assert_eq!(encoded, expected);
    }

    #[test]
    fn sync_status() {
        use crate::types::syncing::NumberedBlock;
        let status = crate::types::syncing::Status {
            starting: NumberedBlock {
                number: pathfinder_common::BlockNumber::GENESIS,
                hash: block_hash!("0x123"),
            },
            current: NumberedBlock {
                number: pathfinder_common::BlockNumber::GENESIS + 20,
                hash: block_hash!("0x456"),
            },
            highest: NumberedBlock {
                number: pathfinder_common::BlockNumber::GENESIS + 300,
                hash: block_hash!("0x789"),
            },
        };

        let s = Serializer::default();

        let expected = json!({
           "starting_block_hash": &status.starting.hash.serialize(s).unwrap(),
           "current_block_hash": &status.current.hash.serialize(s).unwrap(),
           "highest_block_hash": &status.highest.hash.serialize(s).unwrap(),
           "starting_block_num": status.starting.number.serialize(s).unwrap(),
           "current_block_num": status.current.number.serialize(s).unwrap(),
           "highest_block_num": status.highest.number.serialize(s).unwrap(),
        });

        let encoded = status.serialize(s).unwrap();

        assert_eq!(encoded, expected);
    }

    #[test]
    fn chain_id() {
        let uut = ChainId(felt!("0x1234"));
        let expected = json!("0x1234");
        let encoded = uut.serialize(Default::default()).unwrap();

        assert_eq!(encoded, expected);
    }

    #[test]
    fn num_as_hex_h256() {
        let mut bytes = [0u8; 32];
        bytes[30] = 0x12;
        bytes[31] = 0x34;
        let uut = primitive_types::H256(bytes);
        let uut = H256Hex(uut);
        let expected = json!("0x1234");
        let encoded = uut.serialize(Default::default()).unwrap();

        assert_eq!(encoded, expected);
    }

    #[test]
    fn num_as_hex_u64() {
        let uut = U64Hex(0x1234);
        let expected = json!("0x1234");
        let encoded = uut.serialize(Default::default()).unwrap();

        assert_eq!(encoded, expected);
    }

    #[test]
    fn eth_address() {
        let mut bytes = [0u8; 20];
        bytes[18] = 0x12;
        bytes[19] = 0x34;

        let uut = EthereumAddress(H160(bytes));
        let expected = json!("0x0000000000000000000000000000000000001234");
        let encoded = uut.serialize(Default::default()).unwrap();

        assert_eq!(encoded, expected);
    }

    mod to_hex {
        use super::super::hex_str::*;

        #[test]
        fn zero() {
            let data = 0x0u16.to_be_bytes();
            assert_eq!(&bytes_to_hex_str_full(&data), "0x0000");
            assert_eq!(&bytes_to_hex_str_stripped(&data), "0x0");
        }

        #[test]
        fn leading_zeros_even() {
            let data = 0x12u16.to_be_bytes();
            assert_eq!(&bytes_to_hex_str_full(&data), "0x0012");
            assert_eq!(&bytes_to_hex_str_stripped(&data), "0x12");
        }

        #[test]
        fn leading_zeros_odd() {
            let data = 0x1u16.to_be_bytes();
            assert_eq!(&bytes_to_hex_str_full(&data), "0x0001");
            assert_eq!(&bytes_to_hex_str_stripped(&data), "0x1");
        }

        #[test]
        fn multibyte_odd() {
            let data = 0x12345u32.to_be_bytes();
            assert_eq!(&bytes_to_hex_str_full(&data), "0x00012345");
            assert_eq!(&bytes_to_hex_str_stripped(&data), "0x12345");
        }

        #[test]
        fn multibyte_even() {
            let data = 0x123456u32.to_be_bytes();
            assert_eq!(&bytes_to_hex_str_full(&data), "0x00123456");
            assert_eq!(&bytes_to_hex_str_stripped(&data), "0x123456");
        }
    }

    mod from_hex {
        use super::super::hex_str::*;

        #[test]
        fn zero() {
            let bytes = bytes_from_hex_str_stripped::<2>("0x0").unwrap();
            assert_eq!(bytes, [0; 2]);

            let bytes = bytes_from_hex_str_stripped::<2>("0x").unwrap();
            assert_eq!(bytes, [0; 2]);
        }

        #[test]
        fn leading_zeros_even() {
            let bytes = bytes_from_hex_str_stripped::<2>("0x12").unwrap();
            assert_eq!(bytes, [0, 0x12]);
            let bytes = bytes_from_hex_str_stripped::<2>("0x0012").unwrap();
            assert_eq!(bytes, [0, 0x12]);
        }

        #[test]
        fn leading_zeros_odd() {
            let bytes = bytes_from_hex_str_stripped::<2>("0x1").unwrap();
            assert_eq!(bytes, [0, 0x1]);
        }

        #[test]
        fn multibyte_odd() {
            let bytes = bytes_from_hex_str_stripped::<4>("0x12345").unwrap();
            assert_eq!(bytes, [0, 0x01, 0x23, 0x45]);
        }

        #[test]
        fn multibyte_even() {
            let bytes = bytes_from_hex_str_stripped::<4>("0x123456").unwrap();
            assert_eq!(bytes, [0, 0x12, 0x34, 0x56]);
        }
    }
}
