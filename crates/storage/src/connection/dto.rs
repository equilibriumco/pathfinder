use std::fmt;

use fake::Dummy;
use pathfinder_crypto::Felt;

/// Minimally encoded Felt value.
#[derive(Clone, Debug, PartialEq, Eq, Default, Hash)]
pub struct MinimalFelt(pub Felt);

impl serde::Serialize for MinimalFelt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = self.0.as_be_bytes();
        let zeros = bytes.iter().take_while(|&&x| x == 0).count();
        bytes[zeros..].serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for MinimalFelt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = MinimalFelt;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a sequence")
            }

            fn visit_seq<B>(self, mut seq: B) -> Result<Self::Value, B::Error>
            where
                B: serde::de::SeqAccess<'de>,
            {
                let len = seq.size_hint().unwrap();
                let mut bytes = [0; 32];
                let num_zeros = bytes.len() - len;
                let mut i = num_zeros;
                while let Some(value) = seq.next_element()? {
                    bytes[i] = value;
                    i += 1;
                }
                Ok(MinimalFelt(Felt::from_be_bytes(bytes).unwrap()))
            }
        }

        deserializer.deserialize_seq(Visitor)
    }
}

impl From<Felt> for MinimalFelt {
    fn from(value: Felt) -> Self {
        Self(value)
    }
}

impl From<MinimalFelt> for Felt {
    fn from(value: MinimalFelt) -> Self {
        value.0
    }
}

impl<T> Dummy<T> for MinimalFelt {
    fn dummy_with_rng<R: rand::prelude::Rng + ?Sized>(config: &T, rng: &mut R) -> Self {
        let felt: Felt = Dummy::dummy_with_rng(config, rng);
        felt.into()
    }
}
