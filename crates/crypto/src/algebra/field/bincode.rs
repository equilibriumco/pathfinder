use bincode::{BorrowDecode, Decode, Encode};

use super::Felt;

impl Encode for Felt {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> core::result::Result<(), bincode::error::EncodeError> {
        bincode::Encode::encode(self.as_be_bytes(), encoder)?;
        Ok(())
    }
}

impl<Context> Decode<Context> for Felt {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        let bytes = <[u8; 32]>::decode(decoder)?;
        Ok(Self::from_be_bytes(bytes)
            .map_err(|_| bincode::error::DecodeError::Other("Overflow when decoding felt value"))?)
    }
}

impl<'de, Context> BorrowDecode<'de, Context> for Felt {
    fn borrow_decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        Self::decode(decoder)
    }
}
