//! Utilities used for serializing/deserializing sequencer REST API related
//! data.

use std::borrow::Cow;

use pathfinder_common::prelude::*;
use pathfinder_crypto::{Felt, HexParseError};
use primitive_types::{H160, H256, U256};
use serde::de::Visitor;
use serde_with::{serde_conv, DeserializeAs, SerializeAs};

serde_conv!(
    pub CallParamAsDecimalStr,
    CallParam,
    |serialize_me: &CallParam| serialize_me.0.to_dec_str(),
    |s: &str| Felt::from_dec_str(s).map(CallParam)
);

serde_conv!(
    pub ConstructorParamAsDecimalStr,
    ConstructorParam,
    |serialize_me: &ConstructorParam| serialize_me.0.to_dec_str(),
    |s: &str| Felt::from_dec_str(s).map(ConstructorParam)
);

serde_conv!(
    pub L1ToL2MessagePayloadElemAsDecimalStr,
    L1ToL2MessagePayloadElem,
    |serialize_me: &L1ToL2MessagePayloadElem| serialize_me.0.to_dec_str(),
    |s: &str| Felt::from_dec_str(s).map(L1ToL2MessagePayloadElem)
);

serde_conv!(
    pub L2ToL1MessagePayloadElemAsDecimalStr,
    L2ToL1MessagePayloadElem,
    |serialize_me: &L2ToL1MessagePayloadElem| serialize_me.0.to_dec_str(),
    |s: &str| Felt::from_dec_str(s).map(L2ToL1MessagePayloadElem)
);

serde_conv!(
    pub TransactionSignatureElemAsDecimalStr,
    TransactionSignatureElem,
    |serialize_me: &TransactionSignatureElem| serialize_me.0.to_dec_str(),
    |s: &str| Felt::from_dec_str(s).map(TransactionSignatureElem)
);

pub struct EthereumAddressAsHexStr;

impl SerializeAs<EthereumAddress> for EthereumAddressAsHexStr {
    fn serialize_as<S>(source: &EthereumAddress, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // EthereumAddress is "0x" + 40 digits at most
        let mut buf = [0u8; 2 + 40];
        let s = bytes_as_hex_str(source.0.as_bytes(), &mut buf);
        serializer.serialize_str(s)
    }
}

impl<'de> DeserializeAs<'de, EthereumAddress> for EthereumAddressAsHexStr {
    fn deserialize_as<D>(deserializer: D) -> Result<EthereumAddress, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct EthereumAddressVisitor;

        impl Visitor<'_> for EthereumAddressVisitor {
            type Value = EthereumAddress;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a hex string of up to 40 digits with an optional '0x' prefix")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                bytes_from_hex_str::<{ H160::len_bytes() }>(v)
                    .map_err(serde::de::Error::custom)
                    .map(|b| EthereumAddress(H160::from(b)))
            }
        }

        deserializer.deserialize_str(EthereumAddressVisitor)
    }
}

pub struct H256AsNoLeadingZerosHexStr;

impl SerializeAs<H256> for H256AsNoLeadingZerosHexStr {
    fn serialize_as<S>(source: &H256, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // H256 is "0x" + 64 digits at most
        let mut buf = [0u8; 2 + 64];
        let s = bytes_as_hex_str(source.as_bytes(), &mut buf);
        serializer.serialize_str(s)
    }
}

impl<'de> DeserializeAs<'de, H256> for H256AsNoLeadingZerosHexStr {
    fn deserialize_as<D>(deserializer: D) -> Result<H256, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct H256Visitor;

        impl Visitor<'_> for H256Visitor {
            type Value = H256;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a hex string of up to 64 digits with an optional '0x' prefix")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                bytes_from_hex_str::<{ H256::len_bytes() }>(v)
                    .map_err(serde::de::Error::custom)
                    .map(H256::from)
            }
        }

        deserializer.deserialize_str(H256Visitor)
    }
}

pub struct GasPriceAsHexStr;

impl SerializeAs<GasPrice> for GasPriceAsHexStr {
    fn serialize_as<S>(source: &GasPrice, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // GasPrice is "0x" + 32 digits at most
        let mut buf = [0u8; 2 + 32];
        let bytes = source.0.to_be_bytes();
        let s = bytes_as_hex_str(&bytes, &mut buf);
        serializer.serialize_str(s)
    }
}

impl<'de> DeserializeAs<'de, GasPrice> for GasPriceAsHexStr {
    fn deserialize_as<D>(deserializer: D) -> Result<GasPrice, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct GasPriceVisitor;

        impl Visitor<'_> for GasPriceVisitor {
            type Value = GasPrice;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a hex string of up to 32 digits with an optional '0x' prefix")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                bytes_from_hex_str::<16>(v)
                    .map_err(serde::de::Error::custom)
                    .map(GasPrice::from_be_bytes)
            }
        }

        deserializer.deserialize_str(GasPriceVisitor)
    }
}

pub struct StarknetBlockNumberAsHexStr;

impl SerializeAs<BlockNumber> for StarknetBlockNumberAsHexStr {
    fn serialize_as<S>(source: &BlockNumber, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = source.get().to_be_bytes();
        // BlockNumber is "0x" + 16 digits at most
        let mut buf = [0u8; 2 + 16];
        let s = bytes_as_hex_str(&bytes, &mut buf);
        serializer.serialize_str(s)
    }
}

impl<'de> DeserializeAs<'de, BlockNumber> for StarknetBlockNumberAsHexStr {
    fn deserialize_as<D>(deserializer: D) -> Result<BlockNumber, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StarknetBlockNumberVisitor;

        impl Visitor<'_> for StarknetBlockNumberVisitor {
            type Value = BlockNumber;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a hex string of up to 16 digits with an optional '0x' prefix")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let stripped = v.strip_prefix("0x").unwrap_or(v);
                let raw = u64::from_str_radix(stripped, 16).map_err(serde::de::Error::custom)?;
                BlockNumber::deserialize_value::<E>(raw)
            }
        }

        deserializer.deserialize_str(StarknetBlockNumberVisitor)
    }
}

serde_with::serde_conv!(
    pub U256AsHexStr,
    primitive_types::U256,
    |u: &U256| { let b = u.to_big_endian(); bytes_to_hex_str(&b) },
    |s: &str| bytes_from_hex_str::<32>(s).map(|b| U256::from_big_endian(&b))
);

serde_with::serde_conv!(
    pub ResourceAmountAsHexStr,
    ResourceAmount,
    |serialize_me: &ResourceAmount| { let b = serialize_me.0.to_be_bytes(); bytes_to_hex_str(&b) },
    |s: &str| bytes_from_hex_str::<8>(s).map(|b| ResourceAmount(u64::from_be_bytes(b)))
);

serde_with::serde_conv!(
    pub ResourcePricePerUnitAsHexStr,
    ResourcePricePerUnit,
    |serialize_me: &ResourcePricePerUnit| { let b = serialize_me.0.to_be_bytes(); bytes_to_hex_str(&b) },
    |s: &str| bytes_from_hex_str::<16>(s).map(|b| ResourcePricePerUnit(u128::from_be_bytes(b)))
);

serde_with::serde_conv!(
    pub TipAsHexStr,
    Tip,
    |serialize_me: &Tip| { let b = serialize_me.0.to_be_bytes(); bytes_to_hex_str(&b) },
    |s: &str| bytes_from_hex_str::<8>(s).map(|b| Tip(u64::from_be_bytes(b)))
);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct U64AsHexStr(pub u64);

impl serde::Serialize for U64AsHexStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&bytes_to_hex_str(&self.0.to_be_bytes()))
    }
}

impl<'de> serde::Deserialize<'de> for U64AsHexStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = U64AsHexStr;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("A u64 encoded as a hex string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let value = bytes_from_hex_str::<8>(v)
                    .map(u64::from_be_bytes)
                    .map_err(E::custom)?;

                Ok(U64AsHexStr(value))
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

/// A Serde helper module to be used as `#[serde(with = "u64_as_hex_str")]`
/// Helps us keep primitive types in struct fields.
pub mod u64_as_hex_str {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::U64AsHexStr;

    pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        U64AsHexStr(*value).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wrapped = U64AsHexStr::deserialize(deserializer)?;
        Ok(wrapped.0)
    }
}

/// A convenience function which parses a hex string into a byte array.
///
/// Supports both upper and lower case hex strings. The '0x' prefix is
/// mandatory.
fn bytes_from_hex_str<const N: usize>(hex_str: &str) -> Result<[u8; N], HexParseError> {
    fn parse_hex_digit(digit: u8) -> Result<u8, HexParseError> {
        match digit {
            b'0'..=b'9' => Ok(digit - b'0'),
            b'A'..=b'F' => Ok(digit - b'A' + 10),
            b'a'..=b'f' => Ok(digit - b'a' + 10),
            other => Err(HexParseError::InvalidNibble(other)),
        }
    }

    let Some(hex_str) = hex_str.strip_prefix("0x") else {
        return Err(HexParseError::MissingPrefix);
    };
    if hex_str.len() > N * 2 {
        return Err(HexParseError::InvalidLength {
            max: N * 2,
            actual: hex_str.len(),
        });
    }

    let mut buf = [0u8; N];

    // We want the result in big-endian so reverse iterate over each pair of
    // nibbles.
    let chunks = hex_str.as_bytes().rchunks_exact(2);

    // Handle a possible odd nibble remaining nibble.
    let odd_nibble = chunks.remainder();
    if !odd_nibble.is_empty() {
        let full_bytes = hex_str.len() / 2;
        buf[N - 1 - full_bytes] = parse_hex_digit(odd_nibble[0])?;
    }

    for (i, c) in chunks.enumerate() {
        // Indexing c[0] and c[1] are safe since chunk-size is 2.
        buf[N - 1 - i] = (parse_hex_digit(c[0])? << 4) | parse_hex_digit(c[1])?;
    }

    Ok(buf)
}

/// The first stage of conversion - skip leading zeros
fn skip_zeros(bytes: &[u8]) -> (impl Iterator<Item = &u8>, usize, usize) {
    // Skip all leading zero bytes
    let it = bytes.iter().skip_while(|&&b| b == 0);
    let num_bytes = it.clone().count();
    let skipped = bytes.len() - num_bytes;
    // The first high nibble can be 0
    let start = if bytes[skipped] < 0x10 { 1 } else { 2 };
    // Number of characters to display
    let len = start + num_bytes * 2;
    (it, start, len)
}

/// The second stage of conversion - map bytes to hex str
fn it_to_hex_str<'a>(
    it: impl Iterator<Item = &'a u8>,
    start: usize,
    len: usize,
    buf: &'a mut [u8],
) -> &'a [u8] {
    const LUT: [u8; 16] = *b"0123456789abcdef";
    buf[0] = b'0';
    // Same small lookup table is ~25% faster than hex::encode_from_slice 🤷
    it.enumerate().for_each(|(i, &b)| {
        let idx = b as usize;
        let pos = start + i * 2;
        let x = [LUT[(idx & 0xf0) >> 4], LUT[idx & 0x0f]];
        buf[pos..pos + 2].copy_from_slice(&x);
    });
    buf[1] = b'x';
    &buf[..len]
}

/// Converts a [BlockNumber] to a RPC-compatible hex string.
pub fn block_number_as_hex_str(block_number: &BlockNumber) -> String {
    let bytes = block_number.get().to_be_bytes();
    // BlockNumber is "0x" + 16 digits at most
    let mut buf = [0u8; 2 + 16];
    let s = bytes_as_hex_str(&bytes, &mut buf);
    s.to_string()
}

/// A convenience function which produces a "0x" prefixed hex str slice in a
/// given buffer `buf` from an array of bytes.
/// Panics if `bytes.len() * 2 + 2 > buf.len()`
pub fn bytes_as_hex_str<'a>(bytes: &'a [u8], buf: &'a mut [u8]) -> &'a str {
    let expected_buf_len = bytes.len() * 2 + 2;
    assert!(
        buf.len() >= expected_buf_len,
        "buffer size is {}, expected at least {}",
        buf.len(),
        expected_buf_len
    );

    if !bytes.iter().any(|b| *b != 0) {
        return "0x0";
    }

    let (it, start, len) = skip_zeros(bytes);
    let res = it_to_hex_str(it, start, len, buf);
    // Unwrap is safe because `buf` holds valid UTF8 characters.
    std::str::from_utf8(res).unwrap()
}

/// A convenience function which produces a "0x" prefixed hex string from a
/// [Felt].
pub fn bytes_to_hex_str(bytes: &[u8]) -> Cow<'static, str> {
    if !bytes.iter().any(|b| *b != 0) {
        return Cow::from("0x0");
    }
    let (it, start, len) = skip_zeros(bytes);
    let mut buf = vec![0u8; len];
    it_to_hex_str(it, start, len, &mut buf);
    // Unwrap is safe as the buffer contains valid utf8
    String::from_utf8(buf).unwrap().into()
}

/// A convenience function which produces a "0x" prefixed hex string from a
/// [H256].
pub fn h256_as_no_leading_zeros_hex_str(h: &H256) -> String {
    let mut buf = [0u8; 2 + 64];
    let s = bytes_as_hex_str(h.as_bytes(), &mut buf);
    s.to_string()
}

/// Extract JSON representation of program and entry points from the contract
/// definition.
pub fn extract_program_and_entry_points_by_type(
    contract_definition_dump: &[u8],
) -> anyhow::Result<(serde_json::Value, serde_json::Value)> {
    use anyhow::Context;

    #[derive(serde::Deserialize)]
    struct ContractDefinition {
        pub program: serde_json::Value,
        pub entry_points_by_type: serde_json::Value,
    }

    let contract_definition =
        serde_json::from_slice::<ContractDefinition>(contract_definition_dump)
            .context("Failed to parse contract_definition")?;

    Ok((
        contract_definition.program,
        contract_definition.entry_points_by_type,
    ))
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use num_bigint::BigUint;
    use pathfinder_crypto::{DecParseError, OverflowError};
    use pretty_assertions_sorted::assert_eq;

    use super::*;

    #[test]
    fn zero() {
        const ZERO_HEX_STR: &str = "0x0";
        const ZERO_DEC_STR: &str = "0";
        const ZERO_BYTES: [u8; 1] = [0];

        let a = Felt::from_biguint(BigUint::from_bytes_be(&ZERO_BYTES)).unwrap();
        let b = Felt::from_dec_str(ZERO_DEC_STR).unwrap();
        let expected = Felt::ZERO;
        assert_eq!(expected, a);
        assert_eq!(expected, b);
        assert_eq!(expected.to_dec_str(), ZERO_DEC_STR);

        let c: [u8; 1] = bytes_from_hex_str(ZERO_HEX_STR).unwrap();
        assert!(c.iter().all(|x| *x == 0));
        assert_eq!(bytes_to_hex_str(&c[..]), ZERO_HEX_STR);
        let mut buf = [0u8; 2 + 2];
        assert_eq!(bytes_as_hex_str(&c[..], &mut buf), ZERO_HEX_STR);
    }

    #[test]
    fn odd() {
        const ODD_HEX_STR: &str = "0x1234567890abcde";
        const ODD_DEC_STR: &str = "81985529205931230";
        const ODD_BYTES: [u8; 8] = [1, 0x23, 0x45, 0x67, 0x89, 0x0a, 0xbc, 0xde];

        let a = Felt::from_biguint(BigUint::from_bytes_be(&ODD_BYTES)).unwrap();
        let b = Felt::from_dec_str(ODD_DEC_STR).unwrap();
        let expected = Felt::from_hex_str(ODD_HEX_STR).unwrap();
        assert_eq!(expected, a);
        assert_eq!(expected, b);
        assert_eq!(expected.to_dec_str(), ODD_DEC_STR);

        let c: [u8; 8] = bytes_from_hex_str(ODD_HEX_STR).unwrap();
        assert_eq!(c, ODD_BYTES);
        assert_eq!(bytes_to_hex_str(&c[..]), ODD_HEX_STR);
        let mut buf = [0u8; 2 + 16];
        assert_eq!(bytes_as_hex_str(&c[..], &mut buf), ODD_HEX_STR);
    }

    #[test]
    fn even() {
        const EVEN_HEX_STR: &str = "0x1234567890abcdef";
        const EVEN_DEC_STR: &str = "1311768467294899695";
        const EVEN_BYTES: [u8; 8] = [0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef];

        let a = Felt::from_biguint(BigUint::from_bytes_be(&EVEN_BYTES)).unwrap();
        let b = Felt::from_dec_str(EVEN_DEC_STR).unwrap();
        let expected = Felt::from_hex_str(EVEN_HEX_STR).unwrap();
        assert_eq!(expected, a);
        assert_eq!(expected, b);
        assert_eq!(expected.to_dec_str(), EVEN_DEC_STR);

        let c: [u8; 8] = bytes_from_hex_str(EVEN_HEX_STR).unwrap();
        assert_eq!(c, EVEN_BYTES);
        assert_eq!(bytes_to_hex_str(&c[..]), EVEN_HEX_STR);
        let mut buf = [0u8; 2 + 16];
        assert_eq!(bytes_as_hex_str(&c[..], &mut buf), EVEN_HEX_STR);
    }

    #[test]
    fn max() {
        const MAX_HEX_STR: &str =
            "0x800000000000011000000000000000000000000000000000000000000000000";
        const MAX_DEC_STR: &str =
            "3618502788666131213697322783095070105623107215331596699973092056135872020480";
        const MAX_BYTES: [u8; 32] = [
            8, 0, 0, 0, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];

        let a = Felt::from_biguint(BigUint::from_bytes_be(&MAX_BYTES)).unwrap();
        let b = Felt::from_dec_str(MAX_DEC_STR).unwrap();
        let expected = Felt::from_hex_str(MAX_HEX_STR).unwrap();
        assert_eq!(expected, a);
        assert_eq!(expected, b);
        assert_eq!(expected.to_dec_str(), MAX_DEC_STR);

        let c: [u8; 32] = bytes_from_hex_str(MAX_HEX_STR).unwrap();
        assert_eq!(c, MAX_BYTES);
        assert_eq!(bytes_to_hex_str(&c[..]), MAX_HEX_STR);
        let mut buf = [0u8; 2 + 64];
        assert_eq!(bytes_as_hex_str(&c[..], &mut buf), MAX_HEX_STR);
    }

    #[test]
    #[should_panic]
    fn buffer_too_small() {
        let mut buf = [0u8; 2 + 1];
        bytes_as_hex_str(&[0u8], &mut buf);
    }

    #[test]
    fn overflow() {
        const OVERFLOW_DEC_STR: &str =
            "3618502788666131213697322783095070105623107215331596699973092056135872020481";
        const OVERFLOW_BYTES: [u8; 32] = [
            8, 0, 0, 0, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 1,
        ];

        assert_eq!(
            Felt::from_biguint(BigUint::from_bytes_be(&OVERFLOW_BYTES)),
            Err(OverflowError)
        );
        assert_matches!(
            Felt::from_dec_str(OVERFLOW_DEC_STR),
            Err(DecParseError::Overflow)
        );
    }

    #[test]
    fn too_long() {
        const TOO_LONG_HEX_STR: &str =
            "0x80000000000001100000000000000000000000000000000000000000000000100";
        const TOO_LONG_DEC_STR: &str =
            "926336713898529590706514632472337947039515447124888755193111566370783237243136";
        const TOO_LONG_BYTES: [u8; 33] = [
            8, 0, 0, 0, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 1, 0,
        ];

        use pathfinder_crypto::HexParseError;
        assert_eq!(
            Felt::from_biguint(BigUint::from_bytes_be(&TOO_LONG_BYTES)),
            Err(OverflowError)
        );
        assert_matches!(
            Felt::from_dec_str(TOO_LONG_DEC_STR),
            Err(DecParseError::Overflow)
        );
        assert_eq!(
            bytes_from_hex_str::<32>(TOO_LONG_HEX_STR),
            Err(HexParseError::InvalidLength {
                max: 64,
                actual: 65
            })
        );
        // Regression: previously max in the error message was hard-coded at 64,
        // so try another buf size to make sure it is not anymore
        assert_eq!(
            &format!("{}", bytes_from_hex_str::<1>("0xabc").unwrap_err()),
            "More than 2 digits found: 3"
        );
    }

    #[test]
    fn invalid_digit() {
        Felt::from_dec_str("0x123a").unwrap();
        assert_matches!(
            Felt::from_dec_str("123z"),
            Err(DecParseError::ParseBigInt(e)) if e.to_string() == "invalid digit found in string"
        );
        assert_eq!(
            bytes_from_hex_str::<32>("0x123z"),
            Err(HexParseError::InvalidNibble(b'z'))
        );
    }

    mod block_number_as_hex_str {
        #[serde_with::serde_as]
        #[derive(Debug, Copy, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
        struct BlockNum(
            #[serde_as(as = "super::StarknetBlockNumberAsHexStr")] pathfinder_common::BlockNumber,
        );

        impl BlockNum {
            pub const fn new_or_panic(v: u64) -> Self {
                Self(pathfinder_common::BlockNumber::new_or_panic(v))
            }
        }

        #[test]
        fn deserialize() {
            // u64::from_str_radix does not accept the `0x` prefix, so also make sure it is
            // stripped
            ["", "0x"].into_iter().for_each(|prefix| {
                assert_eq!(
                    serde_json::from_str::<BlockNum>(&format!("\"{prefix}0\"")).unwrap(),
                    BlockNum::new_or_panic(0)
                );
                assert_eq!(
                    serde_json::from_str::<BlockNum>(&format!("\"{prefix}123\"")).unwrap(),
                    BlockNum::new_or_panic(0x123)
                );
                assert_eq!(
                    serde_json::from_str::<BlockNum>(&format!("\"{prefix}1234\"")).unwrap(),
                    BlockNum::new_or_panic(0x1234)
                );
                let e = serde_json::from_str::<BlockNum>(&format!("\"{prefix}ffffffffffffffff\""))
                    .unwrap_err();
                assert!(e.is_data(), "{e:?}");
            });
        }
    }
}
