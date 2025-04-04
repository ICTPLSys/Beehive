use super::local_allocator::BlockHead;
use crate::utils::bitfield::*;
use crate::utils::pointer::*;
use core::intrinsics::likely;
use std::sync::atomic::{AtomicU64, Ordering};

#[repr(C)]
#[derive(Debug)]
pub(crate) struct Entry(AtomicU64);

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct LocalEntry(u64);

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct RemoteEntry(u64);

#[derive(Debug)]
pub(crate) enum WrappedEntry {
    Null, // FREE
    Local(LocalEntry),
    Remote(RemoteEntry),
}

impl WrappedEntry {
    pub fn new(entry_state: u64) -> Self {
        if entry_state == 0 {
            Self::Null
        } else if (LocalEntry { 0: entry_state }).present() {
            Self::Local(LocalEntry { 0: entry_state })
        } else {
            Self::Remote(RemoteEntry { 0: entry_state })
        }
    }
}

impl From<WrappedEntry> for u64 {
    fn from(value: WrappedEntry) -> Self {
        match value {
            WrappedEntry::Null => 0,
            WrappedEntry::Local(local) => local.0,
            WrappedEntry::Remote(remote) => remote.0,
        }
    }
}

static_assertions::assert_eq_size!(Entry, u64);

#[repr(C)]
pub(crate) enum LocalState {
    LOCAL = 0,
    MARKED = 1,
    EVICTING = 2,
    BUSY = 3,
}

impl From<LocalState> for u64 {
    fn from(value: LocalState) -> Self {
        value as u64
    }
}

impl From<u64> for LocalState {
    fn from(value: u64) -> Self {
        match value {
            0 => LocalState::LOCAL,
            1 => LocalState::MARKED,
            2 => LocalState::EVICTING,
            3 => LocalState::BUSY,
            _ => panic!("invalid local state"),
        }
    }
}

impl LocalEntry {
    define_bit!(present, 0);
    define_bits!(state, 1, 2);
    define_bit!(accessed, 3);
    define_bit!(dirty, 4);
    define_bits!(ref_count, 5, 12);
    // 13-15: reserved
    define_bits!(addr, 16, 63);

    #[inline(always)]
    pub fn new(
        local_state: LocalState,
        accessed: bool,
        dirty: bool,
        ref_count: u64,
        addr: u64,
    ) -> Self {
        Self {
            0: Self::init_present(true)
                | Self::init_state(local_state as u64)
                | Self::init_accessed(accessed)
                | Self::init_dirty(dirty)
                | Self::init_ref_count(ref_count)
                | Self::init_addr(addr),
        }
    }

    #[inline(always)]
    pub fn fast_path<const MUT: bool>(&self) -> bool {
        self.present() && self.is_local() && self.accessed() && (!MUT || self.dirty())
    }

    pub fn get(&self) -> u64 {
        self.0
    }

    pub fn dec_ref_count(&mut self) {
        self.set_ref_count(self.ref_count() - 1);
    }

    pub fn inc_ref_count(&mut self) {
        self.set_ref_count(self.ref_count() + 1);
    }

    pub fn is_evicting(&self) -> bool {
        self.state() == LocalState::EVICTING as u64
    }

    pub fn is_marked(&self) -> bool {
        self.state() == LocalState::MARKED as u64
    }

    pub fn is_busy(&self) -> bool {
        self.state() == LocalState::BUSY as u64
    }

    pub fn is_local(&self) -> bool {
        self.state() == LocalState::LOCAL as u64
    }

    pub fn can_evict(&self) -> bool {
        !self.accessed() && self.ref_count() == 0 && self.state() == LocalState::LOCAL as u64
    }
}

impl RemoteEntry {
    define_bit!(present, 0);
    define_bit!(fetching, 1);
    define_bit!(dirty, 2); // dirty bit is set if fetching is dirty
    define_bits!(size, 3, 15);
    // TODO: confirm remote addr (including server idx) is 48-bit only
    define_bits!(addr, 16, 63);

    #[inline(always)]
    pub fn new(addr: u64, size: usize) -> Self {
        Self {
            0: Self::init_present(false)
                | Self::init_fetching(false)
                | Self::init_dirty(false)
                | Self::init_size(size as u64)
                | Self::init_addr(addr),
        }
    }

    pub fn get(&self) -> u64 {
        self.0
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
    pub fn is_remote(&self) -> bool {
        !self.is_local()
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
        WrappedEntry::new(self.0.load(Ordering::Relaxed))
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

    pub fn pin(&self) {
        debug_assert!(self.is_local());
        let mut curr = self.0.load(Ordering::Relaxed);
        loop {
            let mut local = LocalEntry { 0: curr };
            debug_assert!(local.present());
            local.inc_ref_count();
            local.set_state(LocalState::LOCAL as u64);
            if let Err(old) = self.cas_entry_weak(curr, local.0, Ordering::Relaxed) {
                curr = old;
            } else {
                break;
            }
        }
    }

    pub fn unpin(&self) {
        let mut curr = self.0.load(Ordering::Relaxed);
        loop {
            let mut local = LocalEntry { 0: curr };
            debug_assert!(local.present());
            debug_assert!(!local.is_evicting());
            debug_assert!(!local.is_marked());
            local.dec_ref_count();
            if let Err(old) = self.cas_entry_weak(curr, local.0, Ordering::Relaxed) {
                curr = old;
            } else {
                break;
            }
        }
    }

    pub fn cas_entry_weak(&self, current: u64, new: u64, order: Ordering) -> Result<u64, u64> {
        self.0.compare_exchange_weak(current, new, order, order)
    }

    pub fn cas_entry_strong(&self, current: u64, new: u64, order: Ordering) -> Result<u64, u64> {
        self.0.compare_exchange(current, new, order, order)
    }

    #[inline]
    pub fn init<const DIRTY: bool>(&mut self, block: SendNonNull<BlockHead>) {
        let addr = unsafe { block.as_ptr().add(1) as u64 };
        self.set_local(LocalEntry::new(LocalState::LOCAL, true, DIRTY, 0, addr));
    }

    pub fn is_evicting(&self) -> bool {
        match self.load() {
            WrappedEntry::Local(local) => local.is_evicting(),
            _ => false,
        }
    }

    pub fn is_fetching(&self) -> bool {
        match self.load() {
            WrappedEntry::Remote(remote) => remote.fetching(),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_entry() {
        let entry = Entry::null();
        let mut local = LocalEntry::new(LocalState::LOCAL, false, false, 1, 0x1234567890);
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
