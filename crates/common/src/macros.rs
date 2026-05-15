/// Macros for newtypes stored with an sqlite INTEGER column.
pub(super) mod i64_backed_u64 {

    /// Generates `new`, `new_or_panic` and `get` methods, `PartialEq` against
    /// `i64` and `u64`, and `fake::Dummy`.
    macro_rules! new_get_partialeq {
        ($target:ty) => {
            impl $target {
                pub const fn new(val: u64) -> Option<Self> {
                    let max = i64::MAX as u64;
                    // Range::contains is not const
                    if val <= max {
                        Some(Self(val))
                    } else {
                        None
                    }
                }

                pub const fn new_or_panic(val: u64) -> Self {
                    match Self::new(val) {
                        Some(x) => x,
                        None => panic!("Invalid constant"),
                    }
                }

                pub const fn get(&self) -> u64 {
                    self.0
                }
            }

            impl PartialEq<u64> for $target {
                fn eq(&self, other: &u64) -> bool {
                    self.0 == *other
                }
            }

            impl PartialEq<i64> for $target {
                fn eq(&self, other: &i64) -> bool {
                    u64::try_from(*other).map(|x| self == &x).unwrap_or(false)
                }
            }

            impl PartialEq<u32> for $target {
                fn eq(&self, other: &u32) -> bool {
                    self.0 == *other as u64
                }
            }

            impl PartialEq<i32> for $target {
                fn eq(&self, other: &i32) -> bool {
                    u64::try_from(*other).map(|x| self == &x).unwrap_or(false)
                }
            }

            impl<T> fake::Dummy<T> for $target {
                fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
                    Self(rng.gen_range(0..i64::MAX as u64))
                }
            }
        };
    }

    /// Generates a u64 alike serialization and deserialization.
    macro_rules! serdes {
        ($target:ty) => {
            impl serde::Serialize for $target {
                fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: serde::Serializer,
                {
                    serializer.serialize_u64(self.0)
                }
            }

            impl<'de> serde::Deserialize<'de> for $target {
                fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: serde::Deserializer<'de>,
                {
                    let raw = u64::deserialize(deserializer)?;
                    <$target>::deserialize_value::<D::Error>(raw)
                }
            }

            impl $target {
                pub fn deserialize_value<E>(raw: u64) -> Result<Self, E>
                where
                    E: serde::de::Error,
                {
                    <$target>::new(raw).ok_or_else(|| {
                        serde::de::Error::invalid_value(
                            serde::de::Unexpected::Unsigned(raw),
                            &"i64::MAX unsigned integer",
                        )
                    })
                }
            }
        };
    }

    pub(crate) use new_get_partialeq;
    pub(crate) use serdes;
}

/// Generates felt newtype-wrappers and the `macro_prelude` module.
///
/// Note that this is a single-use macro as it generates a module.
///
/// Usage:
///     `felt_newtypes!([x1, x2, ..]; [y1, y2, ..])`
/// where `x` is the set of `Felt` wrapper types and `y` the `Felt251` wrappers.
macro_rules! felt_newtypes {
    ([$($felt:ident),* $(,)?]; [$($felt251:ident),* $(,)?]) => {
        crate::macros::felt_newtypes!(@define_felt $($felt),*);
        crate::macros::felt_newtypes!(@define_felt251 $($felt251),*);

        pub mod macro_prelude {
            pub use super::felt;
            pub use super::felt_bytes;

            crate::macros::felt_newtypes!(@generate_felt_macro $($felt),*);
            crate::macros::felt_newtypes!(@generate_felt251_macro $($felt251),*);

            crate::macros::felt_newtypes!(@generate_use $($felt),*);
            crate::macros::felt_newtypes!(@generate_use $($felt251),*);
        }
    };

    (@define_felt $head:ident, $($tail:ident),+ $(,)?) => {
        crate::macros::felt_newtypes!(@define_felt $head);
        crate::macros::felt_newtypes!(@define_felt $($tail),+);
    };

    (@define_felt $target:ident) => {
        paste::paste! {
            #[derive(Copy, Clone, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, PartialOrd, Ord, Dummy)]
            pub struct $target(pub pathfinder_crypto::Felt);

            #[allow(unused)]
            impl $target {
                pub const ZERO: Self = Self(pathfinder_crypto::Felt::ZERO);

                pub fn as_inner(&self) -> &pathfinder_crypto::Felt {
                    &self.0
                }
            }

            $crate::macros::fmt::thin_debug!($target);
            $crate::macros::fmt::thin_display!($target);
        }
    };

    (@define_felt251 $head:ident, $($tail:ident),+ $(,)?) => {
        crate::macros::felt_newtypes!(@define_felt251 $head);
        crate::macros::felt_newtypes!(@define_felt251 $($tail),+);
    };

    (@define_felt251 $target:ident) => {
        paste::paste! {
            #[derive(Copy, Clone, Default, PartialEq, Eq, Hash, serde::Serialize, PartialOrd, Ord, Dummy)]
            pub struct $target(pub pathfinder_crypto::Felt);

            $crate::macros::fmt::thin_debug!($target);
            $crate::macros::fmt::thin_display!($target);

            impl $target {
                pub const ZERO: Self = Self(pathfinder_crypto::Felt::ZERO);

                pub fn as_inner(&self) -> &pathfinder_crypto::Felt {
                    &self.0
                }

                pub const fn new(hash: Felt) -> Option<Self> {
                    if hash.has_more_than_251_bits() {
                        None
                    } else {
                        Some(Self(hash))
                    }
                }

                pub const fn new_or_panic(hash: Felt) -> Self {
                    match Self::new(hash) {
                        Some(key) => key,
                        None => panic!("Too many bits, need less for MPT keys"),
                    }
                }

                pub const fn get(&self) -> &Felt {
                    &self.0
                }

                pub fn view_bits(&self) -> &bitvec::slice::BitSlice<u8, bitvec::order::Msb0> {
                    self.0.view_bits()
                }
            }

            impl<'de> serde::Deserialize<'de> for $target {
                fn deserialize<D>(de: D) -> Result<Self, D::Error>
                where
                    D: serde::Deserializer<'de>,
                {
                    let felt = Felt::deserialize(de)?;
                    $target::new(felt).context("Felt251 overflow").map_err(serde::de::Error::custom)
                }
            }
        }
    };

    (@generate_use $head:ident, $($tail:ident),+ $(,)?) => {
        crate::macros::felt_newtypes!(@generate_use $head);
        crate::macros::felt_newtypes!(@generate_use $($tail),+);
    };

    (@generate_use $target:ident) => {
        paste::paste! {
            pub use [<$target:snake>];
            pub use [<$target:snake _bytes>];
        }
    };

    (@generate_felt_macro $head:ident, $($tail:ident),+ $(,)?) => {
        crate::macros::felt_newtypes!(@generate_felt_macro $head);
        crate::macros::felt_newtypes!(@generate_felt_macro $($tail),+);
    };

    (@generate_felt_macro $target:ident) => {
        paste::paste! {
            #[macro_export]
            macro_rules! [<$target:snake>] {
                ($hex:expr) => {
                    $crate::$target($crate::felt!($hex))
                };
            }

            #[macro_export]
            macro_rules! [<$target:snake _bytes>] {
                ($bytes:expr) => {
                    $crate::$target($crate::felt_bytes!($bytes))
                };
            }
        }
    };

    (@generate_felt251_macro $head:ident, $($tail:ident),+ $(,)?) => {
        crate::macros::felt_newtypes!(@generate_felt251_macro $head);
        crate::macros::felt_newtypes!(@generate_felt251_macro $($tail),+);
    };

    (@generate_felt251_macro $target:ident) => {
        paste::paste! {
            #[macro_export]
            macro_rules! [<$target:snake>] {
                ($hex:expr) => {
                    $crate::$target::new_or_panic($crate::felt!($hex))
                };
            }

            #[macro_export]
            macro_rules! [<$target:snake _bytes>] {
                ($bytes:expr) => {
                    $crate::$target::new_or_panic($crate::felt_bytes!($bytes))
                };
            }
        }
    };
}
pub(super) use felt_newtypes;

pub(super) mod fmt {

    /// Adds a thin display implementation which uses the inner fields Display.
    macro_rules! thin_display {
        ($target:ty) => {
            impl std::fmt::Display for $target {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    std::fmt::Display::fmt(&self.0, f)
                }
            }
        };
    }

    /// Adds a thin Debug implementation, which skips `X(StarkHash(debug))` as
    /// `X(debug)`.
    ///
    /// The implementation uses Display of the wrapped value to produce smallest
    /// possible string, but still wraps it in a default Debug derive style
    /// `TypeName(hash)`.
    macro_rules! thin_debug {
        ($target:ty) => {
            impl std::fmt::Debug for $target {
                fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(fmt, "{}({})", stringify!($target), self.0)
                }
            }
        };
    }

    pub(crate) use thin_debug;
    pub(crate) use thin_display;
}

/// Creates a [Felt](pathfinder_crypto::Felt) from a hex string literal verified
/// at compile time.
#[macro_export]
macro_rules! felt {
    ($hex:expr) => {{
        // This forces const evaluation of the macro call. Without this the invocation
        // will only be evaluated at runtime.
        use ::pathfinder_crypto;
        const CONST_FELT: pathfinder_crypto::Felt =
            match pathfinder_crypto::Felt::from_hex_str($hex) {
                Ok(f) => f,
                Err(pathfinder_crypto::HexParseError::InvalidNibble(_)) => {
                    panic!("Invalid hex digit")
                }
                Err(pathfinder_crypto::HexParseError::InvalidLength { .. }) => {
                    panic!("Too many hex digits")
                }
                Err(pathfinder_crypto::HexParseError::MissingPrefix) => {
                    panic!("Missing '0x' prefix")
                }
                Err(pathfinder_crypto::HexParseError::Overflow) => panic!("Felt overflow"),
            };
        CONST_FELT
    }};
}

/// Creates a [`pathfinder_crypto::Felt`] from a byte slice, resulting in
/// compile-time error when invalid.
#[macro_export]
macro_rules! felt_bytes {
    ($bytes:expr) => {{
        match pathfinder_crypto::Felt::from_be_slice($bytes) {
            Ok(sh) => sh,
            Err(pathfinder_crypto::OverflowError) => panic!("Invalid constant: OverflowError"),
        }
    }};
}
