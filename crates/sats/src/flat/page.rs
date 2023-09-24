use std::collections::{BTreeMap, HashMap};

use crate::ProductType;

use super::FlatProductValue;

pub const PAGE_SIZE: usize = 16 * 1024;

pub struct Page {
    buffer: Vec<u8>,
    row_size: usize,
    num_rows: usize,
}

#[derive(Debug)]
pub struct WriteError;

pub struct RowIndex(usize);

impl Page {
    pub fn new(row_size: usize) -> Self {
        let buffer = [0].repeat(PAGE_SIZE);
        Self {
            buffer,
            row_size,
            num_rows: 0,
        }
    }

    fn used_bytes(&self) -> usize {
        self.row_size * self.num_rows
    }

    pub fn num_free_rows(&self) -> usize {
        let remaining_bytes = self.buffer.len() - self.used_bytes();
        remaining_bytes / self.row_size
    }

    pub fn write(&mut self, product: FlatProductValue) -> Result<RowIndex, WriteError> {
        if self.num_free_rows() == 0 {
            return Err(WriteError);
        }

        let bytes = &product.buffer;
        let size = bytes.len();
        let start = self.used_bytes();
        self.buffer[start..start + size].copy_from_slice(bytes);
        self.num_rows += 1;

        Ok(RowIndex(self.num_rows - 1))
    }

    // NOTE: This function is possible because we store rows contigously.
    // If we stored in a columnar fashion,
    // with all values of a particular column contigously,
    // we could only provide a "cell" API.
    pub fn read(&self, index: RowIndex) -> FlatProductValue<'_> {
        let start = index.0 * self.row_size;
        let buffer = &self.buffer[start..start + self.row_size];
        FlatProductValue { buffer }
    }
}

struct RowOffset {
    which_page: u16,
    offset_in_page: u16,
}

struct RowHash(u64);

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

#[repr(packed)]
#[derive(Copy, Clone)]
struct RowOffset {
    which_page: u32,
    offset_in_page: u16,
}

#[derive(Copy, Clone)]
union Foobar {
    collider_idx: u32,
    offset: RowOffset,
}
*/

pub(crate) struct Table {
    row_type: ProductType,
    pages: Vec<Page>,
    colliders: Vec<SmallVec<RowOffset>,
    offset_map: HashMap<RowHash, Foobar>,
    //pub(crate) rows: BTreeMap<RowId, ProductValue>,
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

#[cfg(test)]
mod tests {
    use crate::{product, AlgebraicType, ProductType, flat::FixedSizeOf};
    use crate::flat::SerializeFlat;

    use super::*;

    #[test]
    fn it_works() {
        let product = product![42u8, 24u8];
        let product_ty: ProductType = [("x", AlgebraicType::U8), ("y", AlgebraicType::U8)]
            .into_iter()
            .collect();
        let fixed_size = product_ty.fixed_size_of();
        assert_eq!(fixed_size, 2);

        let mut buffer = Vec::with_capacity(fixed_size);
        let flat = product.serialize(&mut buffer);
        dbg!(flat.buffer);

        let mut page = Page::new(fixed_size);
        assert_eq!(page.num_rows, 0);

        let row_idx = page.write(flat).unwrap();

        assert_eq!(page.num_rows, 1);

        let flat2 = page.read(row_idx);
        dbg!(flat2.get_element(&product_ty, 0).as_u8_unchecked());
        dbg!(flat2.get_element(&product_ty, 1).as_u8_unchecked());
    }
}
