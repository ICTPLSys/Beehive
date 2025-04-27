use super::entry::Entry;
use super::entry::WrappedEntry;
use super::manager::check_cq;
use super::manager::deallocate_object;
use crate::mem::entry::LocalState;
use crate::mem::local_allocator::BlockHead;
use std::marker::{PhantomData, PhantomPinned};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
/// remote pointer. should not be copied.
pub struct RemPtr<T> {
    // TODO entry should be a pointer?
    pub(super) entry: Entry,
    _marker: PhantomData<T>,
    _pin: PhantomPinned,
}

/// remoteable reference
pub struct RemRef<'a, T: 'a> {
    reference: &'a T,
}

/// mutable remoteable reference
pub struct RemRefMut<'a, T: 'a> {
    reference: &'a mut T,
}

unsafe impl<'a, T: 'a> Sync for RemRefMut<'a, T> {}
unsafe impl<'a, T: 'a> Send for RemRefMut<'a, T> {}

impl<T> RemPtr<T> {
    pub fn null() -> Self {
        Self {
            entry: Entry::null(),
            _marker: PhantomData,
            _pin: PhantomPinned,
        }
    }

    pub fn is_null(&self) -> bool {
        self.entry.is_null()
    }

    // !!! this is NOT thread safe
    // the src and dst entry should not be accessed concurrently
    //
    // move another entry to this, will rewrite the object header
    // dereferencing unique ptr when moving is UB for mutators
    // only eviction may be concurrent with this
    pub fn move_from(&mut self, other: &mut Self) {
        let mut old_state = other.entry.load();
        let pin_and_move =
            |dst: &mut Self, src: &mut Self, old_state: WrappedEntry| -> (bool, WrappedEntry) {
                match old_state {
                    WrappedEntry::Local(local) => {
                        debug_assert!(
                            !local.is_busy(),
                            "invalid state when moving: try to pin a busy entry"
                        );
                        // 2. pin the buffer at local
                        // for MARKED_CLEAN / MARKED_SYNCING
                        // interrupt the eviction to avoid bugs
                        // TODO: this may be optimized
                        let mut pin_state = local;
                        pin_state.set_state(LocalState::LOCAL.into());
                        pin_state.inc_ref_count();
                        if let Err(current_state) = src.entry.cas_entry_weak(
                            old_state.into(),
                            pin_state.get(),
                            Ordering::Relaxed,
                        ) {
                            return (false, WrappedEntry::new(current_state));
                        }
                        // since the buffer is pinned, no other thread will write on the
                        // entry
                        // 3. copy metadata & addrs
                        // pin_state == local + inc_ref_count
                        // unpin_state == pin_state + dec_ref_count == local
                        let mut unpin_state = pin_state;
                        unpin_state.dec_ref_count();
                        // remote addr is set at the block
                        // so we dont need to set it at the entry again
                        dst.entry.set_local(unpin_state);
                        // 4. rewrite the object header
                        unsafe {
                            let block = (unpin_state.addr() as *mut BlockHead).sub(1);
                            block
                                .as_mut_unchecked()
                                .set_entry_addr(&dst.entry as *const Entry);
                        }
                        (true, WrappedEntry::Local(unpin_state))
                    }
                    _ => panic!("invalid state when moving: try to pin a non-local entry"),
                }
            };
        loop {
            // 1. check the state
            match old_state {
                WrappedEntry::Null => {
                    self.entry.set_null();
                    return;
                }
                WrappedEntry::Local(local) => {
                    if local.is_busy() {
                        panic!("invalid state when moving: local entry is busy");
                    } else {
                        // evicting, mark, local
                        match pin_and_move(self, other, old_state) {
                            (true, _) => break,
                            (false, current_state) => {
                                old_state = current_state;
                                continue;
                            }
                        }
                    }
                }
                WrappedEntry::Remote(remote) => {
                    if remote.fetching() {
                        // wait for fetching to simplify the code
                        // and reduce bugs
                        check_cq();
                        old_state = other.entry.load();
                        continue;
                    } else {
                        // remote
                        // no local buffer used
                        self.entry.set_remote(remote);
                        break;
                    }
                }
            }
        }
        other.entry.set_null();
    }
}

impl<T> Drop for RemPtr<T> {
    fn drop(&mut self) {
        deallocate_object(&mut self.entry);
    }
}

impl<'a, T: 'a> RemRef<'a, T> {
    pub fn new(reference: &'a T) -> Self {
        Self { reference }
    }
}

impl<'a, T: 'a> Deref for RemRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.reference
    }
}

impl<'a, T: 'a> RemRefMut<'a, T> {
    pub fn new(reference: &'a mut T) -> Self {
        Self { reference }
    }
}

impl<'a, T: 'a> Deref for RemRefMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.reference
    }
}

impl<'a, T: 'a> DerefMut for RemRefMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.reference
    }
}

pub trait RemRefTrait<'a> {
    fn get_value_addr(&self) -> u64;
}

impl<'a, T: 'a> RemRefTrait<'a> for RemRef<'a, T> {
    #[inline]
    fn get_value_addr(&self) -> u64 {
        self.reference as *const T as u64
    }
}

impl<'a, T: 'a> RemRefTrait<'a> for RemRefMut<'a, T> {
    #[inline]
    fn get_value_addr(&self) -> u64 {
        self.reference as *const T as u64
    }
}
