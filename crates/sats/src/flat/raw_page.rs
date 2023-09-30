use core::{
    mem,
    mem::MaybeUninit,
    ops::{Deref, Index},
    ptr, slice,
};
use std::alloc::{alloc, handle_alloc_error, Layout};

/// The size of a page.
///
/// Currently 64KiB - 8 bytes.
/// The 8 bytes are used for `heapless::Vec.len`.
pub const PAGE_SIZE: usize = u16::MAX as usize - mem::size_of::<usize>();

/// A page of raw bytes.
pub struct Page {
    // The number of written bytes to the page.
    len: usize,
    // The bytes in the page.
    buffer: [MaybeUninit<u8>; PAGE_SIZE],
}

/// An offset into a `Page`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PageOffset(u16);

impl PageOffset {
    /// Returns the offset as a `usize` index.
    pub fn idx(self) -> usize {
        self.0 as usize
    }
}

/// Could not append bytes to a `Page` due to limited space.
#[derive(Debug)]
pub struct PageAppendError;

impl Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY: We have initialized `self.len` bytes, so the slice is valid.
        unsafe { slice::from_raw_parts(self.buffer.as_ptr().cast(), self.len) }
    }
}

impl Page {
    /// Allocates a page directly into the global allocator,
    /// avoiding the stack in the process to ensure no stack overflow.
    pub fn allocate() -> Box<Self> {
        let layout = Layout::new::<Page>();
        // SAFETY: The layout's size is non-zero.
        let raw: *mut Page = unsafe { alloc(layout) }.cast();

        if raw.is_null() {
            handle_alloc_error(layout);
        }

        // We need to initialize `Page::len`
        // without materializing a `&mut` as that is instant UB.
        // SAFETY: `raw` isn't NULL.
        let len = unsafe { ptr::addr_of_mut!((*raw).len) };
        // SAFETY: `len` is valid for writes as only we have exclusive access.
        //          The pointer is also aligned.
        unsafe { len.write(0) };

        // SAFETY: We used the global allocator with a layout for `Page`.
        //         We have initialized the `len`
        //         making the pointee a `Page` valid for reads and writes.
        unsafe { Box::from_raw(raw) }
    }

    /// Returns the number of used bytes in the page.
    pub fn used_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the number of free bytes in the page.
    pub fn free_bytes(&self) -> usize {
        PAGE_SIZE - self.len
    }

    /// Writes `bytes` to the page
    /// and returns the starting offset of `bytes` in the page.
    ///
    /// Errors if `bytes` does not fit in the page.
    pub fn append(&mut self, bytes: &[u8]) -> Result<PageOffset, PageAppendError> {
        let count = bytes.len();
        if count <= self.free_bytes() {
            return Err(PageAppendError);
        }

        let len = self.len;
        // SAFETY (for `.add(len)`):
        // - `bytes` fits in the allocation, so it's in bounds.
        // - `len <= PAGE_LEN <= isize::MAX`.
        // - There's no wrap around.
        let dst: *mut MaybeUninit<u8> = unsafe { self.buffer.as_mut_ptr().add(len).cast() };
        let src: *const MaybeUninit<u8> = bytes.as_ptr().cast();

        // SAFETY:
        // - `src` is valid for reads for `count` by virtue of `&[u8]`.
        // - `dst` is valid for writes for `count` as we have `&mut` + there's room.
        // - `&[u8]` implies proper alignment and `dst` is aligned at a word boundary.
        // - By having `&mut self` we know `bytes: &[u8] cannot overlap.
        unsafe { ptr::copy_nonoverlapping(src, dst, count) };

        self.len += count;

        Ok(PageOffset(len as u16))
    }

    /// Returns a slice starting from `offset` and lasting `count` bytes.
    pub fn slice(&self, offset: PageOffset, count: usize) -> &[u8] {
        let offset = offset.idx();
        &self[offset..offset + count]
    }

    /// Returns a mutable pointer to the buffer.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buffer.as_mut_ptr().cast()
    }

    /// Sets the length to `len`.
    ///
    /// # Safety
    ///
    /// Safe to call if `len` bytes have been initialized.
    pub unsafe fn set_len(&mut self, len: usize) {
        self.len = len;
    }
}

/// The index of a [`Page`] within a [`Pages`].
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PageIndex(u32);

impl PageIndex {
    /// The maximum page index.
    const MAX: Self = PageIndex(u32::MAX);

    /// Returns this index as a `usize`.
    pub fn idx(self) -> usize {
        self.0 as usize
    }
}

impl Index<PageIndex> for Pages {
    type Output = Page;

    fn index(&self, pi: PageIndex) -> &Self::Output {
        &self.pages[pi.idx()]
    }
}

/// Offset to a buffer inside `Pages` referring
/// to the index of a specific page
/// and the offset within the page.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(packed)] // So that `size_of::<OffsetOrCollider>() == 8`.
pub struct BufferOffset {
    /// An index in `pages` of a table where the page is located.
    pub page_index: PageIndex,
    /// The offset to where the data begins in the [`Page`].
    pub offset_in_page: PageOffset,
}

impl BufferOffset {
    /// Returns an offset that is `offset_in_page` bytes into `page`.
    fn new(page: usize, offset_in_page: PageOffset) -> Self {
        Self {
            page_index: PageIndex(page as u32),
            offset_in_page,
        }
    }
}

// Could not allocate a new page as the number would exceed `u32::MAX`.
#[derive(Debug)]
pub struct TooManyPagesError;

/// An error occurred when appending data to the page manager.
#[derive(Debug)]
pub enum PagesAppendError {
    // Could not allocate a new page as the number would exceed `u32::MAX`.
    TooManyPages(TooManyPagesError),
    /// The data attempted to append exceeds `PAGE_SIZE` and will never fit.
    DataWontFit,
}

impl From<TooManyPagesError> for PagesAppendError {
    fn from(value: TooManyPagesError) -> Self {
        Self::TooManyPages(value)
    }
}

/// The page manager on the level of bytes.
#[derive(Default)]
pub struct Pages {
    /// The page buffer.
    ///
    /// Our unit of allocation is a single page rather than `Vec<Page>`.
    pages: Vec<Box<Page>>,
    /// Index to the current working page.
    ///
    /// This is the page to which we are appending.
    curr: usize,
}

impl Deref for Pages {
    type Target = [Box<Page>];

    fn deref(&self) -> &Self::Target {
        &self.pages
    }
}

impl Pages {
    /// Returns a new empty page manager with `capacity` for that many `Page`s.
    pub fn with_capacity(capacity: PageIndex) -> Self {
        Self {
            curr: 0,
            pages: Vec::with_capacity(capacity.idx()),
        }
    }

    /// Returns a slice starting from `offset` and lasting `count` bytes.
    pub fn slice(&self, offset: BufferOffset, count: usize) -> &[u8] {
        self[offset.page_index].slice(offset.offset_in_page, count)
    }

    /// Allocates `count` additional pages,
    /// returning an error if the new number of pages would overflow `u32::MAX`.
    pub fn allocate(&mut self, count: usize) -> Result<(), TooManyPagesError> {
        let new_len = self.len() + count;
        if new_len >= PageIndex::MAX.idx() {
            return Err(TooManyPagesError);
        }

        self.pages.resize_with(new_len, Page::allocate);
        Ok(())
    }

    /// Appends `bytes` to the working page.
    ///
    /// Makes a new page either if there isn't one
    /// or the working page is too full
    /// and there isn't an empty next page.
    ///
    /// Returns an error when `bytes.len() > PAGE_SIZE`
    /// or if more pages could not be allocated when needed.
    pub fn append(&mut self, bytes: &[u8]) -> Result<BufferOffset, PagesAppendError> {
        // Ensure `bytes` can be appended.
        if bytes.len() > PAGE_SIZE {
            return Err(PagesAppendError::DataWontFit);
        }

        // Add a page if we have none.
        if self.is_empty() {
            self.allocate(1)?;
        }

        // Try appending to the current page.
        let offset = match self.pages[self.curr].append(bytes) {
            Ok(o) => o,
            Err(PageAppendError) => {
                // Try appending to the next existing empty page
                // or make a new one.
                if self.curr + 1 >= self.len() {
                    // No empty pages left. Allocate one.
                    self.allocate(1)?;
                }
                self.curr += 1;
                self.pages[self.curr].append(bytes).expect("next page should be empty")
            }
        };
        Ok(BufferOffset::new(self.curr, offset))
    }

    /// Removes the data lasting `len` bytes at `offset`.
    ///
    /// Moves data of `len` bytes,
    /// from the end of the current page,
    /// to where `offset` was.
    /// This ensures contiguous pages,
    /// that pages before the current are filled to max,
    /// and removal in constant time, i.e., `O(1)`.
    ///
    /// Returns the offset to the *moved* bytes, *not* to the deleted one.
    pub fn swap_remove(&mut self, offset: BufferOffset, data_len: usize) -> Option<BufferOffset> {
        // Compute `dst`, i.e., the start pointer to the data to erase.
        // We'll be copying `data_len` bytes from `src` over to `dst`.
        let dst_page = self.pages.get_mut(offset.page_index.idx())?;
        let dst_page_offset = offset.offset_in_page.idx();
        let dst_page_len = dst_page.used_bytes();
        // Ensure `dst_page_offset` is in bounds of the page.
        (dst_page_offset + data_len <= dst_page_len).then_some(())?;
        // SAFETY: In bounds ^-- + `dst_page_offset <= PAGE_LEN < isize::MAX`.
        let dst = unsafe { dst_page.as_mut_ptr().add(dst_page_offset) };

        // Compute `src`, i.e., the start pointer to the data at the end to move.
        let src_page = &mut self.pages[self.curr];
        let src_page_len = src_page.used_bytes();
        let src_page_offset = src_page_len.checked_sub(data_len)?;
        // SAFETY: In bounds ^-- + `src_page_offset <= PAGE_LEN < isize::MAX`.
        let src = unsafe { src_page.as_mut_ptr().add(src_page_offset) };

        // SAFETY: We've ensured `src`
        // and `dst` don't overlap when taking into account `data_len`
        // so there are no aliasing issues.
        // Both pointers are valid for reads/writes for `data_len`
        // and the alignment requirement is vacuously fulfilled
        // as `align_of::<u8> == 1` which is the minimum alignment.
        unsafe { ptr::copy(src, dst, data_len) };

        // No adjustment needed for `dst_page` as it is still `dst_page_len` long.
        // We've merely overwritten a segment with other data.
        // SAFETY: ^-- moved `data_len` bytes from `src_page` in `copy` above
        // so reduce by that amount.
        unsafe { src_page.set_len(src_page_offset) };

        // The previous offset to the data we just moved from the current page.
        let offset = BufferOffset::new(self.curr, PageOffset(src_page_offset as u16));

        // Empty page? Go back one page as the current one.
        if src_page.is_empty() {
            self.curr = self.curr.saturating_sub(1);
        }

        Some(offset)
    }

    /// Removes any unused pages.
    pub fn shrink_to_fit(&mut self) {
        self.pages
            .truncate(self.curr + self.pages[self.curr].is_empty() as usize);
    }
}
