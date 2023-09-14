use std::{fmt::Display, net::Ipv6Addr};

use anyhow::Context as _;
use hex::FromHex as _;
use sats::{impl_deserialize, impl_serialize, impl_st};

use crate::hex::HexString;
use crate::sats;

/// This is the address for a SpacetimeDB database. It is a unique identifier
/// for a particular database and once set for a database, does not change.
///
/// TODO: Evaluate other possible names: `DatabaseAddress`, `SPAddress`
/// TODO: Evaluate replacing this with a literal Ipv6Address which is assigned
/// permanently to a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Address(u128);

impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(&self.to_hex())
    }
}

impl Address {
    pub fn from_arr(arr: &[u8; 16]) -> Self {
        Self(u128::from_be_bytes(*arr))
    }

    pub fn zero() -> Self {
        Self(0)
    }

    pub fn from_hex(hex: &str) -> Result<Self, anyhow::Error> {
        <[u8; 16]>::from_hex(hex)
            .context("Addresses must be 32 hex characters (16 bytes) in length.")
            .map(u128::from_be_bytes)
            .map(Self)
    }

    pub fn to_hex(self) -> HexString<16> {
        crate::hex::encode(&self.as_slice())
    }

    pub fn abbreviate(&self) -> [u8; 8] {
        self.as_slice()[..8].try_into().unwrap()
    }

    pub fn to_abbreviated_hex(self) -> HexString<8> {
        crate::hex::encode(&self.abbreviate())
    }

    pub fn from_slice(slice: impl AsRef<[u8]>) -> Self {
        let slice = slice.as_ref();
        let mut dst = [0u8; 16];
        dst.copy_from_slice(slice);
        Self(u128::from_be_bytes(dst))
    }

    pub fn as_slice(&self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    pub fn to_ipv6(self) -> Ipv6Addr {
        Ipv6Addr::from(self.0)
    }

    #[allow(dead_code)]
    pub fn to_ipv6_string(self) -> String {
        self.to_ipv6().to_string()
    }
}

impl_serialize!([] Address, (self, ser) => self.0.to_be_bytes().serialize(ser));
impl_deserialize!([] Address, de => <[u8; 16]>::deserialize(de).map(|v| Self(u128::from_be_bytes(v))));

#[cfg(feature = "serde")]
impl serde::Serialize for Address {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        spacetimedb_sats::ser::serde::serialize_to(&self.as_slice(), serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Address {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let arr = spacetimedb_sats::de::serde::deserialize_from(deserializer)?;
        Ok(Address::from_arr(&arr))
    }
}

impl_st!([] Address, _ts => sats::AlgebraicType::bytes());

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bsatn_roundtrip() {
        let addr = Address(rand::random());
        let ser = sats::bsatn::to_vec(&addr).unwrap();
        let de = sats::bsatn::from_slice(&ser).unwrap();
        assert_eq!(addr, de);
    }

    #[cfg(feature = "serde")]
    mod serde {
        use super::*;

        #[test]
        fn test_serde_roundtrip() {
            let addr = Address(rand::random());
            let ser = serde_json::to_vec(&addr).unwrap();
            let de = serde_json::from_slice(&ser).unwrap();
            assert_eq!(addr, de);
        }
    }
}
