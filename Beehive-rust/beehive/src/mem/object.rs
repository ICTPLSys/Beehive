use super::entry::Entry;

#[derive(Debug, Copy, Clone)]
pub struct ID {
    meta: u64,
}

impl ID {
    pub(super) fn from_meta(meta: u64) -> Self {
        Self { meta }
    }

    pub(super) fn new(entry: &Entry, size: usize) -> Self {
        let entry_addr = std::ptr::addr_of!(entry) as u64;
        debug_assert!(entry_addr >> 48 == 0);
        debug_assert!(size <= 0xFFFF);
        Self {
            meta: entry_addr | (size as u64) << 48,
        }
    }

    pub(super) fn get_entry_addr(&self) -> u64 {
        self.meta & 0xFFFF_FFFF_FFFF
    }

    pub(super) fn get_size(&self) -> usize {
        (self.meta >> 48) as usize
    }

    pub(super) fn null() -> Self {
        Self { meta: 0 }
    }

    pub(super) fn is_null(&self) -> bool {
        self.meta == 0
    }
}
