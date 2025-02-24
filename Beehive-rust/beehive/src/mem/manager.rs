use super::entry::{Entry, LocalEntry, WrappedEntry};
use super::local_allocator::{BlockHead, GlobalHeap, allocate_block};
use super::object::ID;
use super::pointer::{RemPtr, RemRef, RemRefMut};
use super::scope::DerefScope;
use crate::net::Client;
use crate::net::Config;
use log::{info, warn};
use std::intrinsics::unlikely;
use std::ptr::NonNull;

pub fn initialize(config: Config) {
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
    warn!("TODO: initialize remote heap");
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
fn allocate_local(size: usize, id: ID, scope: &DerefScope) -> NonNull<BlockHead> {
    let block = unsafe { allocate_block(size, id, scope) };
    if unlikely(block.is_none()) {
        todo!("evacuate");
    }
    unsafe { block.unwrap_unchecked() }
}

#[inline]
fn reset_entry<const DIRTY: bool>(entry: &mut Entry, block: NonNull<BlockHead>) {
    let addr = unsafe { block.as_ptr().add(1) as u64 };
    entry.set_local(LocalEntry::new(addr, DIRTY));
}

/// allocate an object of size `size`
fn allocate_object<const DIRTY: bool>(
    size: usize,
    entry: &mut Entry,
    scope: &DerefScope,
) -> NonNull<BlockHead> {
    let id = ID::new(entry, size);
    let block = allocate_local(size, id, scope);
    // TODO: should we allocate remote memory here?
    reset_entry::<DIRTY>(entry, block);
    block
}

fn deallocate_local(local: LocalEntry) {
    let addr = local.addr();
    unsafe {
        let block = (addr as *mut BlockHead).sub(1);
        (*block).deallocate();
    }
}

/// deallocate an object
/// entry will be set to null after deallocation
pub(super) fn deallocate_object(entry: &mut Entry) {
    match entry.load() {
        WrappedEntry::Null => return,
        WrappedEntry::Local(local) => {
            if local.moving() || local.ref_count() > 0 {
                todo!("object moving or in use");
            }
            deallocate_local(local);
        }
        WrappedEntry::Remote(_) => {
            todo!("deallocate remote object");
        }
    }
    entry.set_null();
}

/// allocate an object on remoteable heap
pub fn allocate<'a, T>(ptr: &'a mut RemPtr<T>, value: T, scope: &DerefScope) -> RemRefMut<'a, T> {
    if !ptr.is_null() {
        deallocate_object(&mut ptr.entry);
    }
    let size = std::mem::size_of::<T>();
    let block = allocate_object::<true>(size, &mut ptr.entry, scope);
    unsafe {
        let mut local_ptr = block.add(1).cast::<T>();
        std::ptr::write(local_ptr.as_ptr(), value);
        RemRefMut::new(local_ptr.as_mut())
    }
}

pub fn deallocate<T>(ptr: &mut RemPtr<T>) {
    deallocate_object(&mut ptr.entry);
}

pub fn deref<'a, T>(ptr: &RemPtr<T>, _scope: &'a DerefScope) -> RemRef<'a, T> {
    if let Some(local_addr) = ptr.entry.local_addr_fast_path::<false>() {
        RemRef::new(unsafe { &*(local_addr as *const T) })
    } else {
        let local_addr = deref_slow_path(&ptr.entry, false);
        RemRef::new(unsafe { &*(local_addr as *const T) })
    }
}

pub fn deref_mut<'a, T>(ptr: &mut RemPtr<T>, _scope: &'a DerefScope) -> RemRefMut<'a, T> {
    if let Some(local_addr) = ptr.entry.local_addr_fast_path::<true>() {
        RemRefMut::new(unsafe { &mut *(local_addr as *mut T) })
    } else {
        let local_addr = deref_slow_path(&ptr.entry, true);
        RemRefMut::new(unsafe { &mut *(local_addr as *mut T) })
    }
}

fn deref_slow_path(entry: &Entry, mut_flag: bool) -> u64 {
    match entry.load() {
        WrappedEntry::Null => panic!("deref null object"),
        WrappedEntry::Local(mut local) => {
            if mut_flag {
                local.set_dirty(true);
            }
            local.set_accessed(true);
            entry.set_local(local);
            local.addr()
        }
        WrappedEntry::Remote(_) => todo!("deref remote object"),
    }
}
