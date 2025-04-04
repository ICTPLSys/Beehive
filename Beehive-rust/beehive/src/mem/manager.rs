use super::entry::{Entry, LocalState, WrappedEntry};
use super::local_allocator::{BlockHead, GlobalHeap, allocate_block};
use super::object::ID;
use super::pointer::{RemPtr, RemRef, RemRefMut};
use super::remote_allocator::*;
use super::scope::DerefScopeTrait;
use crate::MutatorState;
use crate::Runtime;
use crate::mem::entry::{LocalEntry, RemoteEntry};
use crate::net::Client;
use crate::net::Config;
use log::info;
use std::arch::x86_64::_rdtsc;
use std::intrinsics::unlikely;
use std::sync::atomic::Ordering;

use crate::utils::pointer::*;
#[derive(Debug, Clone, Copy)]
struct Deadline {
    tsc: u64,
}

impl Deadline {
    const INIT_DELAY_CYCLES: u64 = 8000;
    const DELAY_CYCLES: u64 = 1000;
    pub fn new() -> Self {
        unsafe {
            Self {
                tsc: _rdtsc() + Deadline::INIT_DELAY_CYCLES,
            }
        }
    }

    pub fn is_expired(&self) -> bool {
        unsafe { _rdtsc() > self.tsc }
    }

    pub fn delay(&mut self) {
        self.tsc += Deadline::DELAY_CYCLES;
    }
}

pub trait CheckFetch = FnMut() -> bool;
pub trait OnMiss = Fn(&mut dyn FnMut() -> bool);
pub fn initialize(config: &Config) {
    info!("Initializing memory manager");
    // first, initialize the client
    info!("Initializing network client");
    Client::initialize(config);
    // then, initialize the global heap
    info!("Initializing local heap");
    unsafe {
        let heap = Client::get_memory();
        GlobalHeap::initialize(heap, Client::get_heap_size());
    }
    info!("Initializing remote heap");
    init_remote_heap(config);
    info!("Memory manager initialized");
}

pub fn destroy() {
    info!("Destroying memory manager");
    unsafe {
        GlobalHeap::destroy();
    }
    Client::destroy();
    info!("Memory manager destroyed");
}

#[inline]
fn allocate_local_block(
    size: usize,
    id: ID,
    remote_addr: RemoteAddr,
    scope: &dyn DerefScopeTrait,
) -> SendNonNull<BlockHead> {
    const MAX_RETRY_COUNT: i32 = 1024;
    let entry = id.get_entry();
    debug_assert!(entry.unwrap().is_remote());
    debug_assert_ne!(
        libfibre_port::get_scope_state(),
        crate::MutatorState::OutOfScope as i32
    );
    let mut block = unsafe { allocate_block(size, id, scope) };
    if unlikely(block.is_none()) {
        let mut retry_count = 0;
        loop {
            scope.begin_evacuate();
            Runtime::instance().on_demand_evacuate();
            scope.end_evacuate();
            block = unsafe { allocate_block(size, id, scope) };
            if retry_count > MAX_RETRY_COUNT {
                panic!("Retry allocate_local too much!");
            }
            retry_count += 1;
            if block.is_some() {
                break;
            }
        }
    }
    unsafe {
        let mut block = block.unwrap_unchecked();
        block.as_mut().set_remote_addr(remote_addr);
        block
    }
}

/// allocate an object of size `size`
fn allocate_object<const DIRTY: bool>(
    size: usize,
    entry: &mut Entry,
    remote_addr: RemoteAddr,
    scope: &dyn DerefScopeTrait,
) -> SendNonNull<BlockHead> {
    let id = ID::new(entry, size);
    let block = allocate_local_block(size, id, remote_addr, scope);
    entry.init::<DIRTY>(block);
    if let WrappedEntry::Local(e) = entry.load() {
        debug_assert_eq!(unsafe { block.as_ptr().add(1) } as u64, e.addr());
    } else {
        panic!("error entry set");
    }
    block
}

fn deallocate_local(local_addr: u64, entry: &Entry) {
    unsafe {
        let block = (local_addr as *mut BlockHead).sub(1);
        debug_assert_eq!(
            block.as_mut().unwrap().get_entry().unwrap() as *const Entry,
            entry
        );
        (*block).deallocate();
    }
}

/// deallocate an object
/// entry will be set to null after deallocation
pub(super) fn deallocate_object(entry: &mut Entry) {
    let mut entry_state = entry.load();
    loop {
        match entry_state {
            WrappedEntry::Null => return,
            WrappedEntry::Local(local) => {
                // check if this package is on the fly
                if local.ref_count() > 0 {
                    check_cq();
                    entry_state = entry.load();
                    continue;
                }
                if local.is_marked() || local.is_local() {
                    // LOCAL || MARKED
                    let new_state = WrappedEntry::Null;
                    if let Err(old_state) =
                        entry.cas_entry_weak(local.get(), u64::from(new_state), Ordering::Relaxed)
                    {
                        entry_state = WrappedEntry::new(old_state);
                        continue;
                    } else {
                        let remote_addr = unsafe {
                            let block = (local.addr() as *const BlockHead).sub(1);
                            (*block).remote_addr()
                        };
                        deallocate_local(local.addr(), entry);
                        deallocate_remote(remote_addr);
                        // TODO need Dealllocate Entry?
                        break;
                    }
                } else {
                    // EVICTING || BUSY
                    if local.is_evicting() {
                        check_cq();
                    }
                    entry_state = entry.load();
                    continue;
                }
            }
            WrappedEntry::Remote(remote) => {
                // FETCHING || REMOTE
                if remote.fetching() {
                    check_cq();
                    entry_state = entry.load();
                    continue;
                } else {
                    // REMOTE
                    let new_state = WrappedEntry::Null;
                    if let Err(old_state) =
                        entry.cas_entry_weak(remote.get(), u64::from(new_state), Ordering::Relaxed)
                    {
                        entry_state = WrappedEntry::new(old_state);
                        continue;
                    } else {
                        deallocate_remote(RemoteAddr::new_from_addr(remote.addr()));
                        // TODO need Deallocate entry?
                        break;
                    }
                }
            }
        }
    }
    entry.set_null();
}

/// allocate an object on remoteable heap
pub fn allocate<'a, T>(
    ptr: &mut RemPtr<T>,
    value: Option<T>,
    scope: &dyn DerefScopeTrait,
) -> RemRefMut<'a, T> {
    if !ptr.is_null() {
        deallocate_object(&mut ptr.entry);
    }
    debug_assert!(ptr.entry.is_null());
    let entry = &mut ptr.entry;
    let size = std::mem::size_of::<T>();
    let remote_addr = allocate_remote(size).unwrap();
    // entry is null, so we can allocate it without cas
    entry.set_remote(RemoteEntry::new(remote_addr.addr(), size));
    let block = allocate_object::<true>(size, &mut ptr.entry, remote_addr, scope);
    debug_assert!(ptr.entry.is_local());
    unsafe {
        let mut local_ptr = block.add(1).cast::<T>();
        if let Some(value) = value {
            std::ptr::write(local_ptr.as_ptr(), value);
        }
        RemRefMut::new(local_ptr.as_mut())
    }
}

pub fn deallocate<T>(ptr: &mut RemPtr<T>) {
    deallocate_object(&mut ptr.entry);
}

pub fn deref<'a, T>(
    ptr: &RemPtr<T>,
    miss_handler: &dyn OnMiss,
    scope: &dyn DerefScopeTrait,
) -> RemRef<'a, T> {
    if let Some(local_addr) = ptr.entry.local_addr_fast_path::<false>() {
        RemRef::new(unsafe { &*(local_addr as *const T) })
    } else {
        let local_addr = deref_slow_path::<false>(&ptr.entry, miss_handler, scope);
        RemRef::new(unsafe { &*(local_addr as *const T) })
    }
}

pub fn deref_mut<'a, T>(
    ptr: &mut RemPtr<T>,
    miss_handler: &dyn OnMiss,
    scope: &dyn DerefScopeTrait,
) -> RemRefMut<'a, T> {
    if let Some(local_addr) = ptr.entry.local_addr_fast_path::<true>() {
        RemRefMut::new(unsafe { &mut *(local_addr as *mut T) })
    } else {
        let local_addr = deref_slow_path::<true>(&ptr.entry, miss_handler, scope);
        RemRefMut::new(unsafe { &mut *(local_addr as *mut T) })
    }
}

pub fn deref_sync<'a, T>(ptr: &RemPtr<T>, scope: &dyn DerefScopeTrait) -> RemRef<'a, T> {
    deref(ptr, &|_| {}, scope)
}

pub fn deref_mut_sync<'a, T>(ptr: &mut RemPtr<T>, scope: &dyn DerefScopeTrait) -> RemRefMut<'a, T> {
    deref_mut(ptr, &|_| {}, scope)
}

pub(crate) fn check_cq() -> usize {
    let handle_read = |wr_id: u64| {
        let local_addr = wr_id;
        let block = unsafe { (local_addr as *mut BlockHead).sub(1) };
        let entry = unsafe { block.as_mut().unwrap().get_entry().unwrap() };
        let entry_state = entry.load();
        loop {
            match entry_state {
                WrappedEntry::Local(local) => panic!(
                    "check cq: invalid state {} when handle read!",
                    local.state()
                ),
                WrappedEntry::Remote(remote) => {
                    debug_assert!(remote.fetching());
                    // only this part of codes can operate it for now
                    // since entry.state == fetching
                    entry.set_local(LocalEntry::new(
                        LocalState::LOCAL,
                        true,
                        remote.dirty(),
                        0,
                        local_addr,
                    ));
                    break;
                }
                _ => panic!("check cq: invalid state when handle read!"), // FREE
            }
        }
    };
    let handle_write = |wr_id: u64| {
        let local_addr = wr_id;
        let block = unsafe { (local_addr as *mut BlockHead).sub(1) };
        let entry = unsafe { block.as_mut().unwrap().get_entry().unwrap() };
        let mut entry_state = entry.load();
        loop {
            match entry_state {
                WrappedEntry::Local(mut local) => {
                    if local.addr() != local_addr || local.is_busy() {
                        entry_state = entry.load();
                        continue;
                    }
                    let old = local.get();
                    local.dec_ref_count();
                    if local.is_evicting() {
                        // EVICTING
                        if local.ref_count() == 0 {
                            // we can operate it direcly for now
                            // since entry.state == EVICTING
                            unsafe {
                                entry.set_remote(RemoteEntry::new(
                                    block.as_mut().unwrap().remote_addr().into(),
                                    block.as_mut().unwrap().remote_size(),
                                ));
                                block.as_mut().unwrap().deallocate();
                                break;
                            }
                        } else {
                            if let Err(old) =
                                entry.cas_entry_weak(old, local.get(), Ordering::Relaxed)
                            {
                                entry_state = WrappedEntry::new(old);
                            } else {
                                break;
                            }
                        }
                    } else {
                        // LOCAL
                        if let Err(old) = entry.cas_entry_weak(old, local.get(), Ordering::Relaxed)
                        {
                            entry_state = WrappedEntry::new(old);
                        } else {
                            break;
                        }
                    }
                }
                _ => panic!("check cq: invalid state when handle write!"),
            }
        }
    };
    Client::poll(handle_read, handle_write)
}

fn deref_slow_path<const MUT: bool>(
    entry: &Entry,
    miss_handler: &dyn OnMiss,
    scope: &dyn DerefScopeTrait,
) -> u64 {
    let mut entry_state = entry.load();
    let mut ddl = Deadline::new();
    let mut handle = |entry: &Entry| {
        let mut check = || {
            if ddl.is_expired() {
                loop {
                    if entry.is_local() {
                        return true;
                    }
                    if check_cq() == 0 {
                        break;
                    }
                }
            }
            ddl.delay();
            return false;
        };
        miss_handler(&mut check);
        while !entry.is_local() {
            check_cq();
        }
    };
    loop {
        match entry_state {
            WrappedEntry::Null => panic!("deref null object"),
            WrappedEntry::Local(mut local) => {
                let old = local.get();
                if local.is_busy() {
                    // BUSY
                    entry_state = entry.load();
                    continue;
                }
                // LOCAL, MARKED, EVICTING
                if MUT {
                    local.set_dirty(true);
                }
                local.set_accessed(true);
                local.set_state(LocalState::LOCAL as u64);
                if let Err(old) = entry.cas_entry_weak(old, local.get(), Ordering::Relaxed) {
                    entry_state = WrappedEntry::new(old);
                } else {
                    return local.addr();
                }
            }
            WrappedEntry::Remote(mut remote) => {
                if remote.fetching() {
                    // FETCHING
                    if let Err(old) =
                        entry.cas_entry_weak(remote.get(), remote.get(), Ordering::Relaxed)
                    {
                        entry_state = WrappedEntry::new(old);
                        continue;
                    } else {
                        handle(entry);
                        match entry.load() {
                            WrappedEntry::Local(local) => return local.addr(),
                            _ => panic!("entry should be local after fetch"),
                        }
                    }
                } else {
                    // REMOTE
                    let old = remote.get();
                    let local_block_addr = allocate_local_block(
                        remote.size() as usize,
                        ID::new(entry, remote.size() as usize),
                        RemoteAddr::new_from_addr(remote.addr()),
                        scope,
                    );
                    debug_assert_eq!(entry as *const Entry, unsafe {
                        local_block_addr.as_ref().get_entry().unwrap()
                    });
                    remote.set_fetching(true);
                    remote.set_dirty(MUT);
                    if let Err(old) = entry.cas_entry_weak(old, remote.get(), Ordering::Relaxed) {
                        deallocate_local(unsafe { local_block_addr.as_ptr().add(1) as u64 }, entry);
                        entry_state = WrappedEntry::new(old);
                        continue;
                    } else {
                        // transform block_addr to obj_addr
                        let obj_addr = unsafe { local_block_addr.add(1) };
                        loop {
                            if Client::post_read(
                                RemoteAddr::new_from_addr(remote.addr()),
                                obj_addr.as_ptr() as u64,
                                remote.size() as u32,
                                obj_addr.as_ptr() as u64,
                            ) {
                                break;
                            } else {
                                check_cq();
                            }
                        }
                        handle(entry);
                        // entry are set in handle()
                        debug_assert!(entry.is_local());
                        return obj_addr.as_ptr() as u64;
                    }
                }
            }
        }
    }
}

pub fn prefetch<T>(ptr: &RemPtr<T>, scope: &dyn DerefScopeTrait) {
    prefetch_impl::<T, false>(ptr, scope);
}

pub fn prefetch_mut<T>(ptr: &RemPtr<T>, scope: &dyn DerefScopeTrait) {
    prefetch_impl::<T, true>(ptr, scope);
}

fn prefetch_impl<T, const MUT: bool>(ptr: &RemPtr<T>, scope: &dyn DerefScopeTrait) {
    if let None = ptr.entry.local_addr_fast_path::<MUT>() {
        prefetch_slow_path::<false>(&ptr.entry, scope);
    }
}

fn prefetch_slow_path<const MUT: bool>(entry: &Entry, scope: &dyn DerefScopeTrait) {
    let mut entry_state = entry.load();
    loop {
        match entry_state {
            WrappedEntry::Null => panic!("prefetch null object"),
            WrappedEntry::Local(mut local) => {
                let old = local.get();
                if local.is_busy() {
                    break;
                }
                local.set_dirty(MUT);
                local.set_state(LocalState::LOCAL as u64);
                if let Err(old) = entry.cas_entry_weak(old, local.get(), Ordering::Relaxed) {
                    entry_state = WrappedEntry::new(old);
                } else {
                    break;
                }
            }
            WrappedEntry::Remote(mut remote) => {
                if remote.fetching() {
                    // dont need further operation if entry is fetching
                    break;
                } else {
                    // REMOTE
                    let old = remote.get();
                    let local_block_addr = allocate_local_block(
                        remote.size() as usize,
                        ID::new(entry, remote.size() as usize),
                        RemoteAddr::new_from_addr(remote.addr()),
                        scope,
                    );
                    debug_assert_eq!(entry as *const Entry, unsafe {
                        local_block_addr.as_ref().get_entry().unwrap()
                    });
                    remote.set_fetching(true);
                    remote.set_dirty(MUT);
                    if let Err(old) = entry.cas_entry_weak(old, remote.get(), Ordering::Relaxed) {
                        deallocate_local(unsafe { local_block_addr.as_ptr().add(1) as u64 }, entry);
                        entry_state = WrappedEntry::new(old);
                        continue;
                    } else {
                        // transform block_addr to obj_addr
                        let obj_addr = unsafe { local_block_addr.add(1) };
                        loop {
                            if Client::post_read(
                                RemoteAddr::new_from_addr(remote.addr()),
                                obj_addr.as_ptr() as u64,
                                remote.size() as u32,
                                obj_addr.as_ptr() as u64,
                            ) {
                                break;
                            } else {
                                check_cq();
                            }
                        }
                        // entry are set in handle()
                        debug_assert!(entry.is_local());
                        break;
                    }
                }
            }
        }
    }
}
