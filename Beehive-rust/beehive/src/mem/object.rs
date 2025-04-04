use super::entry::Entry;
use crate::utils::bitfield::*;

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct ID(u64);

unsafe impl Sync for ID {}
unsafe impl Send for ID {}
impl ID {
    define_bits!(entry_addr, 0, 47);
    define_bits!(size, 48, 63);
    pub(super) fn from_meta(meta: u64) -> Self {
        Self { 0: meta }
    }

    pub(super) fn new(entry: &Entry, size: usize) -> Self {
        let entry_addr = (entry as *const Entry) as u64;
        let mut id = Self::from_meta(0);
        id.set_entry_addr(entry_addr);
        id.set_size(size as u64);
        id
    }

    pub(super) fn get_entry(&self) -> Option<&Entry> {
        let entry_ptr = self.entry_addr() as *const Entry;
        if entry_ptr.is_null() {
            None
        } else {
            unsafe { Some(&*entry_ptr) }
        }
    }

    pub(super) fn null() -> Self {
        Self::from_meta(0)
    }

    pub(super) fn is_null(&self) -> bool {
        self.0 == 0
    }
}
