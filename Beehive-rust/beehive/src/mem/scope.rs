use std::sync::atomic::Ordering;

use crate::{GLOBAL_MUTATOR_STATE, MUTATOR_STATE_COUNTS, MutatorState};

use super::entry::Entry;
use super::local_allocator::BlockHead;
use super::pointer::RemRefTrait;

pub trait DerefScopeBaseTrait {
    fn pin(&self);
    fn unpin(&self);
    fn parent(&self) -> Option<&dyn DerefScopeTrait>;
    fn push(&self, remref: &dyn RemRefTrait) -> SingleDerefScope;
}

pub trait DerefScopeTrait: DerefScopeBaseTrait {
    // please do not overwrite this function
    // or you should make sure your impl can work
    fn scope_pin(&self) {
        self.pin();
        if let Some(parent) = self.parent() {
            parent.scope_pin();
        }
    }

    // please do not overwrite this function
    // or you should make sure your impl can work
    fn scope_unpin(&self) {
        self.unpin();
        if let Some(parent) = self.parent() {
            parent.scope_unpin();
        }
    }

    // please do not overwrite this function
    // or you should make sure your impl can work
    fn unmark(&self) {
        self.scope_pin();
        self.scope_unpin();
    }

    // please do not overwrite this function
    // or you should make sure your impl can work
    fn begin_evacuate(&self) {
        self.pin();
        let old_state = libfibre_port::get_scope_state();
        debug_assert_ne!(old_state, MutatorState::OutOfScope as i32);
        libfibre_port::set_scope_state(MutatorState::OutOfScope as i32);
        unsafe {
            let previous = MUTATOR_STATE_COUNTS[old_state as usize].fetch_sub(1, Ordering::Relaxed);
            debug_assert!(previous > 0);
        };
    }

    // please do not overwrite this function
    // or you should make sure your impl can work
    fn end_evacuate(&self) {
        debug_assert_eq!(
            libfibre_port::get_scope_state(),
            MutatorState::OutOfScope as i32
        );
        let state = unsafe { GLOBAL_MUTATOR_STATE.load(Ordering::Relaxed) };
        libfibre_port::set_scope_state(state);
        unsafe { MUTATOR_STATE_COUNTS[state as usize].fetch_add(1, Ordering::Relaxed) };
        self.unpin();
    }
}

impl<T: DerefScopeBaseTrait> DerefScopeTrait for T {}
pub struct RootScope {
    // avoid explictly construction
    _pad: (),
}

impl RootScope {
    pub fn root() -> Self {
        debug_assert_eq!(
            libfibre_port::get_scope_state(),
            MutatorState::OutOfScope as i32
        );
        let state = unsafe { GLOBAL_MUTATOR_STATE.load(Ordering::Relaxed) };
        unsafe { MUTATOR_STATE_COUNTS[state as usize].fetch_add(1, Ordering::Relaxed) };
        libfibre_port::set_scope_state(state);
        // make all thread mutator state set ready
        libfibre_port::yield_now();
        Self { _pad: () }
    }
}

impl DerefScopeBaseTrait for RootScope {
    fn pin(&self) {}
    fn unpin(&self) {}
    fn parent(&self) -> Option<&dyn DerefScopeTrait> {
        None
    }

    fn push(&self, remref: &dyn RemRefTrait) -> SingleDerefScope {
        SingleDerefScope::new(Some(self), remref)
    }
}

impl Drop for RootScope {
    fn drop(&mut self) {
        debug_assert_ne!(
            libfibre_port::get_scope_state(),
            MutatorState::OutOfScope as i32
        );
        let old_state = libfibre_port::get_scope_state();
        libfibre_port::set_scope_state(MutatorState::OutOfScope as i32);
        unsafe {
            MUTATOR_STATE_COUNTS[old_state as usize].fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub struct SingleDerefScope<'a> {
    parent: Option<&'a dyn DerefScopeTrait>,
    value_addr: u64,
}

impl<'a> SingleDerefScope<'a> {
    pub fn empty(scope: Option<&'a dyn DerefScopeTrait>) -> Self {
        Self {
            parent: scope,
            value_addr: 0,
        }
    }

    pub(crate) fn new(parent: Option<&'a dyn DerefScopeTrait>, remref: &dyn RemRefTrait) -> Self {
        Self {
            parent,
            value_addr: remref.get_value_addr(),
        }
    }

    #[inline]
    fn get_entry(&self) -> &Entry {
        let addr = self.value_addr;
        let block = unsafe { &*(addr as *mut BlockHead).sub(1) };
        block.get_entry().unwrap()
    }
}

impl<'a> DerefScopeBaseTrait for SingleDerefScope<'a> {
    fn pin(&self) {
        self.get_entry().pin();
    }

    fn unpin(&self) {
        self.get_entry().unpin();
    }

    fn parent(&self) -> Option<&'a dyn DerefScopeTrait> {
        self.parent
    }

    fn push(&self, remref: &dyn RemRefTrait) -> SingleDerefScope {
        SingleDerefScope::new(Some(self), remref)
    }
}

impl<'a> Drop for SingleDerefScope<'a> {
    fn drop(&mut self) {
        debug_assert_ne!(
            libfibre_port::get_scope_state(),
            MutatorState::OutOfScope as i32
        );
    }
}

#[inline]
pub fn pin_remref(remref: &dyn RemRefTrait, _scope: &dyn DerefScopeTrait) {
    let addr = remref.get_value_addr();
    let block = unsafe { &*(addr as *mut BlockHead).sub(1) };
    block.get_entry().unwrap().pin();
}

#[inline]
pub fn unpin_remref(remref: &dyn RemRefTrait, _scope: &dyn DerefScopeTrait) {
    let addr = remref.get_value_addr();
    let block = unsafe { &*(addr as *mut BlockHead).sub(1) };
    block.get_entry().unwrap().unpin();
}
