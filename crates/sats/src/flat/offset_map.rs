use nohash_hasher::IntMap;
use std::{collections::hash_map::Entry, slice};

use super::raw_page::BufferOffset;
use super::table::RowHash;
use OffsetOrCollider::*;

/// An index to the outer layer of `colliders` in `OffsetMap`.
#[derive(Clone, Copy, PartialEq, Eq)]
struct ColliderSlotIndex(u32);

impl ColliderSlotIndex {
    /// Returns a new slot index based on `idx`.
    fn new(idx: usize) -> Self {
        Self(idx as u32)
    }

    /// Returns the index as a `usize`.
    fn idx(self) -> usize {
        self.0 as usize
    }
}

/// An offset into the `pages` of a table
/// or, for any `RowHash` collisions in `offset_map`,
/// the index in `colliders` to a list of `RowOffset`s.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OffsetOrCollider {
    /// No row hash collisions; this is the only row offset for the hash.
    Offset(BufferOffset),
    /// There are row hash collisions; there are many row offsets for this hash.
    Collider(ColliderSlotIndex),
}

/// An offset map `RowHash -> [RowOffset]`.
#[derive(Default)]
pub struct OffsetMap {
    /// The offset map from row hashes to row offset(s).
    offset_map: IntMap<RowHash, OffsetOrCollider>,
    /// The inner vector is a list ("slot") of row offsets that share a row hash.
    /// The outer is indexed by `ColliderSlotIndex`.
    ///
    /// This indirect approach is used,
    /// rather than storing a list of `RowOffset`,
    /// to reduce the cost for the more common case (fewer collisions).
    ///
    /// This list is append-only as `ColliderSlotIndex` have to be stable.
    /// When removing a row offset causes a slot to become empty,
    /// the index is added to `emptied_collider_slots` and it can be reused.
    /// This is done to avoid a linear scan of `colliders` for the first empty slot.
    // TODO(centril): Use a `SatsBuffer<T>` with `len/capacity: u32` to reduce size.
    colliders: Vec<Vec<BufferOffset>>,
    /// Stack of emptied collider slots.
    // TODO(centril): Use a `SatsBuffer<T>` with `len/capacity: u32` to reduce size.
    emptied_collider_slots: Vec<ColliderSlotIndex>,
}

impl OffsetMap {
    /// Returns the row offsets associated with the given row `hash`.
    pub fn offsets_for(&self, hash: RowHash) -> &[BufferOffset] {
        match self.offset_map.get(&hash) {
            None => &[],
            Some(Offset(ro)) => slice::from_ref(ro),
            Some(Collider(ci)) => &self.colliders[ci.idx()],
        }
    }

    /// Returns the row offsets associated with the given row `hash`.
    pub fn offsets_for_mut(&mut self, hash: RowHash) -> &mut [BufferOffset] {
        match self.offset_map.get_mut(&hash) {
            None => &mut [],
            Some(Offset(ro)) => slice::from_mut(ro),
            Some(Collider(ci)) => &mut self.colliders[ci.idx()],
        }
    }

    /// Associates row `hash` with row `offset`.
    ///
    /// Handles any hash conflicts for `hash`.
    pub fn insert(&mut self, hash: RowHash, offset: BufferOffset) {
        self.offset_map
            .entry(hash)
            .and_modify(|v| match *v {
                // Stored inline => colliders list.
                Offset(existing) => match self.emptied_collider_slots.pop() {
                    // Allocate a new colliders slot.
                    None => {
                        let ci = ColliderSlotIndex::new(self.colliders.len());
                        self.colliders.push(vec![existing, offset]);
                        *v = Collider(ci);
                    }
                    // Reuse an empty slot.
                    Some(ci) => {
                        self.colliders[ci.idx()].push(offset);
                        *v = Collider(ci);
                    }
                },
                // Already using a list; add to it.
                Collider(ci) => {
                    self.colliders[ci.idx()].push(offset);
                }
            })
            // 0 hashes so far.
            .or_insert(Offset(offset));
    }

    /// Removes the association `hash -> offset`.
    ///
    /// Returns whether the association was deleted.
    pub fn remove(&mut self, hash: RowHash, offset: BufferOffset) -> bool {
        let Entry::Occupied(mut entry) = self.offset_map.entry(hash) else {
            return false;
        };

        match *entry.get() {
            // Remove entry on `hash -> [offset]`.
            Offset(o) if o == offset => drop(entry.remove()),
            Offset(_) => return false,
            Collider(ci) => {
                // Find `offset` in slot and remove.
                let slot = &mut self.colliders[ci.idx()];
                let Some(idx) = slot.iter().position(|o| *o == offset) else {
                    return false;
                };
                slot.swap_remove(idx);

                match slot.len() {
                    // Remove entry due to `hash -> []`.
                    0 => drop(entry.remove()),
                    // Simplify; don't use collider list since `hash -> [an_offset]`.
                    1 => *entry.get_mut() = Offset(slot.pop().unwrap()),
                    _ => return true,
                }

                // Slot is now empty; reuse later.
                self.emptied_collider_slots.push(ci);
            }
        }

        true
    }
}
