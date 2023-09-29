#![allow(dead_code)]

use nohash_hasher::IsEnabled;

use super::{FlatProductValue, FixedSizeOf};
use super::page::{Page, RowIndex};
use super::offset_map::OffsetMap;
use crate::ProductType;

/// The content hash of a row.
///
/// Uses fxhash for fast hashing.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowHash(u64);

/// `RowHash` is already a hash, so no need to hash again.
impl IsEnabled for RowHash {}

/// Computes the row hash for the product value.
fn hash_of(fpv: FlatProductValue<'_>) -> RowHash {
    todo!()
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(packed)]
pub struct RowOffset {
    /// An index in `pages` of a table where the page is located.
    page_index: u32,
    /// The offset to where the data begins in the [`Page`].
    offset_in_page: RowIndex,
}

pub struct Table {
    /// The type of each row in the table.
    ///
    /// This is the schema without indices and constraints.
    row_type: ProductType,
    /// Fixed row size in bytes.
    ///
    /// This is a memoized version of `self.row_type.fixed_size_of()`.
    fixed_row_size: usize,
    /// The rows of the table.
    ///
    /// The fixed-size parts of a row must fit within a page.
    pages: Vec<Page>,
    /// Maps `RowHash -> [RowOffset]` where the offsets point into `pages`.
    offset_map: OffsetMap,
}

impl Table {
    /// Creates a new empty table with the given `row_type`.
    pub fn new(row_type: ProductType) -> Self {
        Table {
            fixed_row_size: row_type.fixed_size_of(),
            row_type,
            pages: <_>::default(),
            offset_map: <_>::default(),
        }
    }

    /// Returns the row at `offset`.
    fn get_row(&self, offset: RowOffset) -> FlatProductValue<'_> {
        self.pages[offset.page_index as usize].read(offset.offset_in_page)
    }

    /// Returns whether the table contains the `row`.
    fn contains(&self, row: FlatProductValue<'_>) -> bool {
        let row_hash = hash_of(row);
        let offsets = self.offset_map.offsets_for(row_hash);

        offsets.iter().any(|offset| self.pages)

        match self.offset_map.get(row_hash) {
            None => false, // wohoo!
            Some(offset) if pages[offset] == fpv => true,
            Some(_) => false,
        }
    }
}

/*
fm contains(table, fpv) -> bool {
    let row_hash = hash_of(fpv);
    match table.offset_map.get(row_hash) {
        None => false, // wohoo!
        Some(offset) if pages[offset] == fpv => true,
        Some(_) => false,
    }
}

fn insert(table, fpv) -> Option<RowOffset> {
    if contains(table, fpv) {
        return None;
    }

    let row_hash = hash_of(fpv);
    table.offset_map.insert(row_hash, )

    table.write(fpv)
}

fn delete_fpv(table, fpv) -> bool {
    let row_hash = hash_of(fpv);
    if contains(table, fpv) {
        table.offset_map.remove(row_hash);
    }
}

fn delete(table, row_hash, row_offset) -> bool {
    table.pages.delete(row_offset)
    table.offset_map.remove(row_hash)
}
*/



/*
#[derive(Copy, Clone)]
#[repr(packed)]
struct Heap<T> {
    ptr: NonNull<T>,
    len: u8,
}

union SmallVecData<T, const N: usize> {
    inline: ManuallyDrop<MaybeUninit<[T; N]>>,
    heap: ManuallyDrop<Heap<T>>,
}
*/
