use crate::utils::bitfield::*;
use core::intrinsics::likely;
use std::sync::atomic::{AtomicU64, Ordering};

#[repr(C)]
pub(super) struct Entry(AtomicU64);

#[repr(C)]
#[derive(Copy, Clone)]
pub(super) struct LocalEntry(u64);

#[repr(C)]
#[derive(Copy, Clone)]
pub(super) struct RemoteEntry(u64);

pub(super) enum WrappedEntry {
    Null,
    Local(LocalEntry),
    Remote(RemoteEntry),
}

static_assertions::assert_eq_size!(Entry, u64);

impl LocalEntry {
    define_bit!(present, 0);
    define_bit!(moving, 1);
    define_bit!(accessed, 2);
    define_bit!(dirty, 3);
    define_bits!(ref_count, 4, 11);
    // 12-15: reserved
    define_bits!(addr, 16, 63);

    #[inline(always)]
    pub fn new(addr: u64, dirty: bool) -> Self {
        Self {
            0: Self::init_present(true)
                | Self::init_moving(false)
                | Self::init_accessed(false)
                | Self::init_dirty(dirty)
                | Self::init_ref_count(0)
                | Self::init_addr(addr),
        }
    }

    #[inline(always)]
    pub fn fast_path<const MUT: bool>(&self) -> bool {
        self.present() && !self.moving() && self.accessed() && (!MUT || self.dirty())
    }
}

impl RemoteEntry {
    define_bit!(present, 0);
    define_bit!(moving, 1);
    define_bit!(offloaded, 2);
    // 3: reserved
    define_bits!(size, 4, 15);
    // TODO: confirm remote addr (including server idx) is 48-bit only
    define_bits!(addr, 16, 63);

    #[inline(always)]
    pub fn new(addr: u64, size: usize) -> Self {
        Self {
            0: Self::init_present(false)
                | Self::init_moving(false)
                | Self::init_offloaded(false)
                | Self::init_size(size as u64)
                | Self::init_addr(addr),
        }
    }
}

impl Entry {
    #[inline(always)]
    pub fn is_local(&self) -> bool {
        let local = LocalEntry {
            0: self.0.load(Ordering::Relaxed),
        };
        local.present()
    }

    #[inline(always)]
    pub fn null() -> Self {
        Self(AtomicU64::new(0))
    }

    #[inline(always)]
    pub fn is_null(&self) -> bool {
        self.0.load(Ordering::Relaxed) == 0
    }

    #[inline(always)]
    pub fn load(&self) -> WrappedEntry {
        let value = self.0.load(Ordering::Relaxed);
        if value == 0 {
            WrappedEntry::Null
        } else if (LocalEntry { 0: value }).present() {
            WrappedEntry::Local(LocalEntry { 0: value })
        } else {
            WrappedEntry::Remote(RemoteEntry { 0: value })
        }
    }

    #[inline(always)]
    pub fn set_null(&self) {
        self.0.store(0, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn set_local(&self, entry: LocalEntry) {
        self.0.store(entry.0, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn set_remote(&self, entry: RemoteEntry) {
        self.0.store(entry.0, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn local_addr_fast_path<const MUT: bool>(&self) -> Option<u64> {
        // mov
        let local = LocalEntry {
            0: self.0.load(Ordering::Relaxed),
        };
        // and + cmp
        if likely(local.fast_path::<MUT>()) {
            // shr
            Some(local.addr())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry() {
        let entry = Entry::null();
        let mut local = LocalEntry::new(0x1234567890, false);
        entry.set_local(local);
        assert!(entry.is_local());
        match entry.load() {
            WrappedEntry::Local(l) => {
                assert!(l.0 == local.0);
            }
            _ => panic!("expected local entry"),
        }
        assert!(!local.dirty());
        assert_eq!(local.addr(), 0x1234567890);
        local.set_dirty(true);
        assert!(local.dirty());

        let remote = RemoteEntry::new(0x1234567890, 16);
        entry.set_remote(remote);
        assert!(!entry.is_local());
        match entry.load() {
            WrappedEntry::Remote(r) => {
                assert!(r.0 == remote.0);
            }
            _ => panic!("expected remote entry"),
        }
        assert_eq!(remote.addr(), 0x1234567890);
        assert_eq!(remote.size(), 16);
    }
}
