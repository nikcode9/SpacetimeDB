#![allow(dead_code)]

use nohash_hasher::IsEnabled;

use super::offset_map::OffsetMap;
use super::raw_page::{BufferOffset, Pages};
use super::{FixedSizeOf, FlatProductValue};
use crate::ProductType;

/// The content hash of a row.
///
/// Notes:
/// - The hash is not cryptographically secure.
///
/// - The hash is valid only for the lifetime of a `Table`.
///   This entails that it should not be persisted to disk
///   or used as a stable identifier over the network.
///   For example, the hashing algorithm could be different
///   on different machines based on availability of hardware instructions.
///   Moreover, due to random seeds, when restarting from disk,
///   the hashes may be different for the same rows.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowHash(pub u64);

/// `RowHash` is already a hash, so no need to hash again.
impl IsEnabled for RowHash {}

/// Computes the row hash for the product value.
fn hash_of(_fpv: FlatProductValue<'_>) -> RowHash {
    todo!()
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
    pages: Pages,
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
    fn get_row(&self, offset: BufferOffset) -> FlatProductValue<'_> {
        let buffer = self.pages.slice(offset, self.fixed_row_size);
        FlatProductValue { buffer }
    }

    /// Returns whether the table contains the `row`.
    fn contains(&self, hash: RowHash, row: FlatProductValue<'_>) -> bool {
        self.offset_map
            .offsets_for(hash)
            .iter()
            .any(|offset| row == self.get_row(*offset))
    }

    /// Inserts `row` into the table, or `None` if it was already there.
    fn insert(&mut self, row: FlatProductValue<'_>) -> Option<BufferOffset> {
        // Ensure row isn't already there.
        let hash = hash_of(row);
        if self.contains(hash, row) {
            return None;
        }

        // Add row data to pages.
        let offset = self.pages.append(row.buffer).expect("overflowed u32::MAX pages");

        // Add row to offset map.
        self.offset_map.insert(hash, offset);

        Some(offset)
    }

    /// Deletes a row with the given `hash` and at `offset`.
    fn delete(&mut self, hash: RowHash, offset: BufferOffset) -> bool {
        // Remove from offset map.
        if !self.offset_map.remove(hash, offset) {
            return false;
        }

        // Remove row data.
        let swap_offset = self.pages.swap_remove(offset, self.fixed_row_size).unwrap();

        if offset != swap_offset {
            // We've moved another row (let's call it `B`)
            // into the place of the one we removed (`A`).
            // However, `B` uses its old offset in the offset map.
            // Now the map must be adjusted
            // so that `hash_of(B) -> [.., offset ,..]`
            // and not `hash_of(B) -> [.., swap_offset ,..]`.
            let swap_row = self.get_row(offset);
            let swap_hash = hash_of(swap_row);
            *self
                .offset_map
                .offsets_for_mut(swap_hash)
                .iter_mut()
                .find(|o| **o == swap_offset)
                .unwrap() = offset;
        }

        true
    }
}
