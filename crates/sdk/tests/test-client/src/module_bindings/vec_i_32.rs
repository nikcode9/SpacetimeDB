// THIS FILE IS AUTOMATICALLY GENERATED BY SPACETIMEDB. EDITS TO THIS FILE
// WILL NOT BE SAVED. MODIFY TABLES IN RUST INSTEAD.

#[allow(unused)]
use spacetimedb_sdk::{
    anyhow::{anyhow, Result},
    identity::Identity,
    reducer::{Reducer, ReducerCallbackId, Status},
    sats::{de::Deserialize, ser::Serialize},
    spacetimedb_lib,
    table::{TableIter, TableType, TableWithPrimaryKey},
    Address,
};

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct VecI32 {
    pub n: Vec<i32>,
}

impl TableType for VecI32 {
    const TABLE_NAME: &'static str = "VecI32";
    type ReducerEvent = super::ReducerEvent;
}

impl VecI32 {
    #[allow(unused)]
    pub fn filter_by_n(n: Vec<i32>) -> TableIter<Self> {
        Self::filter(|row| row.n == n)
    }
}