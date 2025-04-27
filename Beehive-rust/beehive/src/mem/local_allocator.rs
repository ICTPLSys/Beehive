use std::intrinsics::{likely, unlikely};
use std::marker::{Send, Sync};
use std::mem::MaybeUninit;
use std::os::raw::c_void;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::entry::{Entry, WrappedEntry};
use super::evacuator::EvacuateBuffer;
use super::object;
use super::scope::DerefScopeTrait;
use super::{RemoteAddr, allocator_utils::*};
use crate::check_memory_low;
use crate::mem::entry::{LocalState, RemoteEntry};
use crate::utils::spinlock::SpinPollingLock;
use std::ptr::NonNull;
use sync_ptr::SendMutPtr;

#[repr(C)]
#[derive(Debug)]
pub(crate) struct BlockHead {
    next: SendMutPtr<BlockHead>,
    remote_addr: RemoteAddr,
    id: object::ID, // TODO need atomic?
}

unsafe impl Sync for BlockHead {}
unsafe impl Send for BlockHead {}
#[repr(C)]
#[derive(Debug)]
struct ListNode {
    next: Option<NonNull<ListNode>>,
}

#[repr(C)]
#[derive(Debug)]
struct RegionHead {
    link: ListNode,
    state: RegionState,
    free_list: Option<NonNull<BlockHead>>,
    active_list: Option<NonNull<BlockHead>>,
    marked_list: Option<NonNull<BlockHead>>,
    used_count: u32,
    unused_offset: u32,
    bin: u32,
    sweep_time_stamp: u32, // for multithreaded mark & evict
}

unsafe impl Sync for RegionHead {}
unsafe impl Send for RegionHead {}
struct RegionList {
    lock: SpinPollingLock,
    head: ListNode,
    tail: Option<NonNull<ListNode>>,
}

pub(crate) struct GlobalHeap {
    usable_list: [RegionList; REGION_BIN_COUNT],
    full_list: RegionList,
    free_list: RegionList,
    unallocated_offset: AtomicUsize,
    heap_size: usize,
    memory_low_water_mark: usize,
    heap: *mut c_void,
    free_size: AtomicUsize,
}

struct ThreadHeap {
    regions: [SendMutPtr<RegionHead>; REGION_BIN_COUNT],
}

#[inline(never)]
pub(super) unsafe fn allocate_block(
    size: usize,
    id: object::ID,
    scope: &dyn DerefScopeTrait,
) -> Option<NonNull<BlockHead>> {
    let wsize = wsize_from_size(size + size_of::<BlockHead>());
    let bin = bin_from_wsize(wsize);
    debug_assert!(bin_size(bin) >= size + size_of::<BlockHead>());
    debug_assert!(bin == 0 || bin_size(bin - 1) < size + size_of::<BlockHead>());
    unsafe {
        let thread_heap = &raw mut THREAD_HEAP;
        (*thread_heap).allocate_block(bin, id, scope)
    }
}

#[thread_local]
static mut THREAD_HEAP: ThreadHeap = ThreadHeap::new();

pub(crate) static mut GLOBAL_HEAP: MaybeUninit<GlobalHeap> = MaybeUninit::uninit();

static GLOBAL_HEAP_INITIALIZED: AtomicBool = AtomicBool::new(false);

impl BlockHead {
    pub(super) fn deallocate(&mut self) {
        self.id = object::ID::null();
    }

    pub(super) fn get_entry(&self) -> Option<&Entry> {
        self.id.get_entry()
    }

    pub(super) fn set_remote_addr(&mut self, addr: RemoteAddr) {
        self.remote_addr = addr;
    }

    pub(super) fn remote_addr(&self) -> RemoteAddr {
        self.remote_addr
    }

    pub(super) fn remote_size(&self) -> usize {
        self.id.size() as usize
    }

    pub(crate) fn get_object_ptr(&self) -> *const c_void {
        unsafe { (self as *const BlockHead).add(1) as *const c_void }
    }

    pub(crate) fn set_entry_addr(&mut self, addr: *const Entry) {
        self.id.set_entry_addr(addr as u64);
    }
}

impl GlobalHeap {
    const LOW_WATER_MARK_RATIO: f64 = 0.2;

    /// initialize the global heap
    pub unsafe fn initialize(heap_ptr: *mut c_void, heap_size: usize) {
        assert!(!GLOBAL_HEAP_INITIALIZED.load(Ordering::Relaxed));
        GLOBAL_HEAP_INITIALIZED.store(true, Ordering::Relaxed);
        let heap = Self::instance();
        for i in 0..REGION_BIN_COUNT {
            heap.usable_list[i].init();
        }
        heap.full_list.init();
        heap.free_list.init();
        heap.unallocated_offset = AtomicUsize::new(0);
        heap.heap_size = heap_size;
        heap.memory_low_water_mark = (heap_size as f64 * Self::LOW_WATER_MARK_RATIO) as usize;
        heap.heap = heap_ptr;
        heap.free_size = AtomicUsize::new(heap_size);
    }

    pub unsafe fn destroy() {
        assert!(GLOBAL_HEAP_INITIALIZED.load(Ordering::Relaxed));
        GLOBAL_HEAP_INITIALIZED.store(false, Ordering::Relaxed);
    }

    pub fn instance() -> &'static mut GlobalHeap {
        unsafe { &mut *GLOBAL_HEAP.as_mut_ptr() }
    }

    fn return_back_region(self: &mut GlobalHeap, region: SendMutPtr<RegionHead>) {
        let region_ptr = unsafe { NonNull::new_unchecked(region.inner()) };
        let region = unsafe { region.as_mut_unchecked() };
        if unlikely(region.can_allocate()) {
            let bin = region.bin;
            region.state = RegionState::Usable;
            self.usable_list[bin as usize].push(region_ptr);
            let region_free_size = region.free_size();
            let previous = self
                .free_size
                .fetch_add(region_free_size, Ordering::Relaxed);
            debug_assert!(previous + region_free_size <= self.heap_size);
        } else {
            region.state = RegionState::Full;
            self.full_list.push(region_ptr);
        }
    }

    /// allocate a new region with free blocks
    fn allocate_region(self: &mut GlobalHeap, bin: usize) -> SendMutPtr<RegionHead> {
        // TODO maybe we can remove this
        // this load is required because of the update of free size and self.list
        // is not synchronous
        if self.free_size.load(Ordering::Relaxed) < REGION_SIZE {
            return SendMutPtr::null();
        }
        // first, try to allocate from the usable list
        let region = self.usable_list[bin].pop();
        if let Some(region) = region {
            let free_size = unsafe { region.as_ref().free_size() };
            let previous = self.free_size.fetch_sub(free_size, Ordering::Relaxed);
            debug_assert!(previous - free_size < self.heap_size);
            return unsafe { SendMutPtr::new(region.as_ptr()) };
        }
        // then, try to allocate from the free list
        let region = self.free_list.pop();
        if let Some(mut region) = region {
            unsafe { region.as_mut().init(bin) };
            let previous = self
                .free_size
                .fetch_sub(unsafe { region.as_ref().free_size() }, Ordering::Relaxed);
            debug_assert!(previous - unsafe { region.as_ref().free_size() } < self.heap_size);
            return unsafe { SendMutPtr::new(region.as_ptr()) };
        }
        // finally, allocate a new region
        let mut offset = self.unallocated_offset.load(Ordering::Relaxed);
        loop {
            if offset + REGION_SIZE > self.heap_size {
                return SendMutPtr::null();
            }
            match self.unallocated_offset.compare_exchange_weak(
                offset,
                offset + REGION_SIZE,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let mut region = unsafe {
                        NonNull::new_unchecked(self.heap.add(offset).cast::<RegionHead>())
                    };
                    unsafe { region.as_mut().init(bin) };
                    let previous = self
                        .free_size
                        .fetch_sub(unsafe { region.as_ref().free_size() }, Ordering::Relaxed);
                    debug_assert!(
                        previous - unsafe { region.as_ref().free_size() } < self.heap_size
                    );
                    return unsafe { SendMutPtr::new(region.as_ptr()) };
                }
                Err(old) => {
                    offset = old;
                }
            }
        }
    }

    pub fn memory_low(self: &GlobalHeap) -> bool {
        let free_size = self.free_size.load(Ordering::Relaxed);
        // println!("free_size: {}", free_size);
        free_size < self.memory_low_water_mark
    }

    pub(crate) fn mark(&mut self, ts: u32) {
        let mut free_size = 0;
        free_size += self.full_list.mark(ts);
        for ulist in self.usable_list.iter_mut() {
            free_size += ulist.mark(ts);
        }
        let previous = self.free_size.fetch_add(free_size, Ordering::Relaxed);
        debug_assert!(previous + free_size <= self.heap_size);
        // println!("mark free_size: {}", free_size);
    }

    pub(crate) fn evacuate(&mut self, evacuate_buffer: &mut EvacuateBuffer, ts: u32) {
        let mut free_size = 0;
        free_size += self.full_list.evacuate(evacuate_buffer, ts);
        for ulist in self.usable_list.iter_mut() {
            free_size += ulist.evacuate(evacuate_buffer, ts);
        }
        let previous = self.free_size.fetch_add(free_size, Ordering::Relaxed);
        debug_assert!(previous + free_size <= self.heap_size);
        // println!("evacuate free_size: {}", free_size);
    }

    pub(crate) fn gc(&mut self, ts: u32) {
        let mut free_size = 0;
        free_size += self.full_list.gc(ts);
        for ulist in self.usable_list.iter_mut() {
            free_size += ulist.gc(ts);
        }
        let previous = self.free_size.fetch_add(free_size, Ordering::Relaxed);
        debug_assert!(previous + free_size <= self.heap_size);
        // println!("gc free_size: {}", free_size);
    }
}

impl ThreadHeap {
    unsafe fn allocate_block(
        self: &mut ThreadHeap,
        bin: usize,
        id: object::ID,
        scope: &dyn DerefScopeTrait,
    ) -> Option<NonNull<BlockHead>> {
        let region = self.regions[bin];
        if likely(!region.is_null()) {
            let block = unsafe { region.as_mut_unchecked().allocate(id) };
            if let Some(block) = block {
                return Some(block);
            }
            GlobalHeap::instance().return_back_region(region);
        }
        // region is null, try to allocate a new region
        let global_heap = GlobalHeap::instance();
        let region = global_heap.allocate_region(bin);
        check_memory_low(scope);
        self.regions[bin] = region;
        if unlikely(region.is_null()) {
            return None;
        } else {
            unsafe { region.as_mut_unchecked().state = RegionState::InUse };
            unsafe { region.as_mut_unchecked().allocate(id) }
        }
    }

    const fn new() -> ThreadHeap {
        ThreadHeap {
            regions: [SendMutPtr::null(); REGION_BIN_COUNT],
        }
    }
}

impl RegionList {
    fn init(self: &mut RegionList) {
        self.lock = SpinPollingLock::new();
        self.head = ListNode { next: None };
        self.tail = None;
    }

    /// push a node to the tail of the list
    fn push(&mut self, node: NonNull<RegionHead>) {
        unsafe { node.as_ptr().cast::<ListNode>().as_mut_unchecked().next = None };
        self.lock.lock();
        match self.tail {
            Some(tail) => {
                unsafe { (*tail.as_ptr()).next = Some(node.cast()) };
                self.tail = Some(node.cast());
            }
            None => {
                debug_assert!(self.head.next.is_none());
                self.head.next = Some(node.cast());
                self.tail = Some(node.cast());
            }
        }
        self.lock.unlock();
    }

    /// pop a node from the front of the list
    fn pop(&mut self) -> Option<NonNull<RegionHead>> {
        self.lock.lock();
        let front = self.head.next;
        match front {
            Some(node) => {
                self.head.next = unsafe { node.as_ref().next };
                if self.tail == Some(node) {
                    self.tail = None;
                }
                self.lock.unlock();
                Some(node.cast())
            }
            None => {
                debug_assert!(self.tail.is_none());
                self.lock.unlock();
                None
            }
        }
    }

    fn pop_unmatched(&mut self, time_stamp: u32) -> Option<NonNull<RegionHead>> {
        self.lock.lock();
        let node = self.head.next;
        if let Some(node) = node {
            let node = node.as_ptr().cast::<RegionHead>();
            if unsafe { node.as_ref_unchecked().sweep_time_stamp == time_stamp } {
                debug_assert!(
                    unsafe { node.as_ref_unchecked().sweep_time_stamp == time_stamp }
                        || self.tail.is_none()
                );
                // println!("pop_unmatched ts is same: {}", time_stamp);
                self.lock.unlock();
                None
            } else {
                // println!("pop one");
                self.head.next = unsafe { node.as_mut_unchecked().link.next };
                if let Some(tail) = self.tail {
                    // only one in this list is poped
                    if std::ptr::addr_eq(tail.as_ptr(), node) {
                        self.tail = None;
                        debug_assert!(self.head.next.is_none());
                    }
                } else {
                    panic!("pop_unmatched a wrong region list, tail is none!");
                }
                self.lock.unlock();
                unsafe { Some(NonNull::new_unchecked(node)) }
            }
        } else {
            debug_assert!(self.tail.is_none());
            self.lock.unlock();
            None
        }
    }

    pub(crate) fn mark(&mut self, ts: u32) -> usize {
        let mut free_size = 0;
        loop {
            if let Some(mut region) = self.pop_unmatched(ts) {
                let region = unsafe { region.as_mut() };
                let origin_free_size = region.free_size();
                region.mark();
                free_size += region.free_size() - origin_free_size;
                region.sweep_time_stamp = ts;
                self.push(unsafe { NonNull::new_unchecked(region) });
            } else {
                break;
            }
        }
        free_size
    }

    pub(crate) fn evacuate(&mut self, evacuate_buffer: &mut EvacuateBuffer, ts: u32) -> usize {
        let mut free_size = 0;
        loop {
            if let Some(mut region) = self.pop_unmatched(ts) {
                let region = unsafe { region.as_mut() };
                let origin_free_size = region.free_size();
                region.evacuate(evacuate_buffer);
                free_size += region.free_size() - origin_free_size;
                region.sweep_time_stamp = ts;
                if region.is_empty() {
                    region.state = RegionState::Free;
                    unsafe {
                        debug_assert!(region.free_size() < REGION_SIZE);
                        GlobalHeap::instance()
                            .free_list
                            .push(NonNull::new_unchecked(region))
                    };
                } else if region.can_allocate() {
                    region.state = RegionState::Usable;
                    unsafe {
                        debug_assert!(region.free_size() < REGION_SIZE && region.free_size() > 0);
                        GlobalHeap::instance().usable_list[region.bin as usize]
                            .push(NonNull::new_unchecked(region))
                    };
                } else {
                    region.state = RegionState::Full;
                    unsafe {
                        debug_assert_eq!(region.free_size(), 0);
                        GlobalHeap::instance()
                            .full_list
                            .push(NonNull::new_unchecked(region))
                    };
                }
            } else {
                break;
            }
        }
        free_size
    }

    pub(crate) fn gc(&mut self, ts: u32) -> usize {
        let mut free_size = 0;
        loop {
            if let Some(mut region) = self.pop_unmatched(ts) {
                let region = unsafe { region.as_mut() };
                let origin_free_size = region.free_size();
                region.gc();
                free_size += region.free_size() - origin_free_size;
                region.sweep_time_stamp = ts;
                if region.is_empty() {
                    region.state = RegionState::Free;
                    unsafe {
                        GlobalHeap::instance()
                            .free_list
                            .push(NonNull::new_unchecked(region))
                    };
                } else if region.can_allocate() {
                    region.state = RegionState::Usable;
                    unsafe {
                        GlobalHeap::instance().usable_list[region.bin as usize]
                            .push(NonNull::new_unchecked(region))
                    };
                } else {
                    region.state = RegionState::Full;
                    unsafe {
                        GlobalHeap::instance()
                            .full_list
                            .push(NonNull::new_unchecked(region))
                    };
                }
            } else {
                break;
            }
        }
        free_size
    }
}

impl RegionHead {
    fn init(self: &mut RegionHead, bin: usize) {
        self.link = ListNode { next: None };
        self.state = RegionState::Free;
        self.free_list = None;
        self.active_list = None;
        self.marked_list = None;
        self.used_count = 0;
        self.unused_offset = size_of::<RegionHead>() as u32;
        self.bin = bin as u32;
        self.sweep_time_stamp = u32::MAX;
    }

    unsafe fn allocate(self: &mut RegionHead, id: object::ID) -> Option<NonNull<BlockHead>> {
        // first, allocate a block
        if let Some(block) = self.free_list {
            // remove the block from the free list
            unsafe {
                self.free_list = {
                    let next = block.as_ref().next.inner();
                    if next.is_null() {
                        None
                    } else {
                        Some(NonNull::new_unchecked(next))
                    }
                }
            }
            Some(block)
        } else {
            // try to allocate from the unused space
            let bin_size = bin_size(self.bin as usize);
            if self.unused_offset as usize + bin_size <= REGION_SIZE {
                unsafe {
                    let block = ptr::from_mut(self)
                        .byte_add(self.unused_offset as usize)
                        .cast::<BlockHead>();
                    self.unused_offset += bin_size as u32;
                    Some(NonNull::new_unchecked(block))
                }
            } else {
                None
            }
        }
        // then, set up the block
        .map(|mut block| {
            self.used_count += 1;
            // add the block to the active list and set the id
            unsafe {
                block.as_mut().next = self
                    .active_list
                    .map_or(SendMutPtr::null(), |p| SendMutPtr::new(p.as_ptr()));
                self.active_list = Some(block);
                block.as_mut().id = id;
                debug_assert_eq!(block.as_mut().id, id);
            }
            block
        })
    }

    fn can_allocate(self: &mut RegionHead) -> bool {
        self.free_list.is_some()
            || self.unused_offset as usize + bin_size(self.bin as usize) <= REGION_SIZE
    }

    fn free_size(self: &RegionHead) -> usize {
        let bin_size = bin_size(self.bin as usize);
        let max_block_num = (REGION_SIZE - size_of::<RegionHead>()) / bin_size;
        (max_block_num - self.used_count as usize) * bin_size
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.active_list.is_none() && self.marked_list.is_none()
    }

    pub(crate) fn mark(&mut self) {
        let mut new_active_list: Option<NonNull<BlockHead>> = None;
        let mut new_marked_list = self.marked_list;
        let mut new_free_list = self.free_list;
        let mut block_head = match self.active_list {
            Some(block) => block.as_ptr(),
            None => {
                // println!("mark active_list is none");
                std::ptr::null_mut()
            }
        };
        while !block_head.is_null() {
            let next_block = unsafe { block_head.as_ref_unchecked().next };
            let entry = unsafe { block_head.as_ref_unchecked().get_entry() };
            let add_list_head = |block: *mut BlockHead, list: &mut Option<NonNull<BlockHead>>| unsafe {
                block.as_mut_unchecked().next = match list {
                    Some(list) => SendMutPtr::new(list.as_ptr()),
                    None => SendMutPtr::null(),
                };
                *list = Some(NonNull::new_unchecked(block));
            };
            if entry.is_none() {
                // block is free
                add_list_head(block_head, &mut new_free_list);
                self.used_count -= 1;
            } else {
                let entry = entry.unwrap();
                let mut entry_state = entry.load();
                loop {
                    match entry_state {
                        WrappedEntry::Null => break,
                        WrappedEntry::Local(local) => {
                            debug_assert!(entry.is_local());
                            if !std::ptr::addr_eq(local.addr() as *const BlockHead, unsafe {
                                block_head.as_ref_unchecked().get_object_ptr()
                            }) {
                                println!("ptr not matched addr");
                                continue;
                            }
                            let mut new_local = local;
                            new_local.set_accessed(false); // clear hotness
                            if new_local.can_evict() {
                                // mark this object
                                new_local.set_state(LocalState::MARKED as u64);
                                if let Err(old_state) = entry.cas_entry_weak(
                                    local.get(),
                                    new_local.get(),
                                    Ordering::Relaxed,
                                ) {
                                    entry_state = WrappedEntry::new(old_state);
                                    continue;
                                }
                                add_list_head(block_head, &mut new_marked_list);
                            } else {
                                debug_assert_ne!(new_local.state(), LocalState::MARKED as u64);
                                if let Err(old_state) = entry.cas_entry_weak(
                                    local.get(),
                                    new_local.get(),
                                    Ordering::Relaxed,
                                ) {
                                    entry_state = WrappedEntry::new(old_state);
                                    continue;
                                }
                                if new_local.state() == LocalState::EVICTING as u64 {
                                    add_list_head(block_head, &mut new_marked_list);
                                } else {
                                    add_list_head(block_head, &mut new_active_list);
                                }
                            }
                        }
                        WrappedEntry::Remote(_) => {
                            debug_assert!(!entry.is_local());
                            add_list_head(block_head, &mut new_active_list);
                        }
                    }
                    break;
                }
            }
            block_head = next_block.inner();
        }
        self.active_list = new_active_list;
        self.marked_list = new_marked_list;
        self.free_list = new_free_list;
    }

    pub(crate) fn evacuate(&mut self, evacuate_buffer: &mut EvacuateBuffer) {
        let mut new_active_list = self.active_list;
        let mut new_marked_list: Option<NonNull<BlockHead>> = None;
        let mut new_free_list = self.free_list;
        let mut block_head = match self.marked_list {
            Some(block) => block.as_ptr(),
            None => {
                // println!("evacuate marked_list is none");
                std::ptr::null_mut()
            }
        };
        let add_list_head = |block: *mut BlockHead, list: &mut Option<NonNull<BlockHead>>| unsafe {
            block.as_mut_unchecked().next = match list {
                Some(list) => SendMutPtr::new(list.as_ptr()),
                None => SendMutPtr::null(),
            };
            *list = Some(NonNull::new_unchecked(block));
        };
        while !block_head.is_null() {
            let next_block = unsafe { block_head.as_ref_unchecked().next };
            let entry = unsafe { block_head.as_ref_unchecked().get_entry() };
            if entry.is_none() {
                // block is free
                add_list_head(block_head, &mut new_free_list);
                self.used_count -= 1;
            } else {
                let entry = entry.unwrap();
                let mut entry_state = entry.load();
                loop {
                    match entry_state {
                        WrappedEntry::Null => break,
                        WrappedEntry::Local(local) => {
                            if !std::ptr::addr_eq(local.addr() as *const c_void, unsafe {
                                block_head.as_ref_unchecked().get_object_ptr()
                            }) {
                                continue;
                            }
                            if local.state() == LocalState::MARKED as u64 {
                                let mut new_local = local;
                                if local.dirty() {
                                    new_local.set_dirty(false);
                                    new_local.set_state(LocalState::EVICTING as u64);
                                    new_local.inc_ref_count();
                                    if let Err(old_state) = entry.cas_entry_weak(
                                        local.get(),
                                        new_local.get(),
                                        Ordering::Relaxed,
                                    ) {
                                        entry_state = WrappedEntry::new(old_state);
                                        continue;
                                    }
                                    evacuate_buffer.add(
                                        unsafe {
                                            block_head.as_ref_unchecked().get_object_ptr() as u64
                                        },
                                        unsafe { block_head.as_ref_unchecked().remote_addr },
                                        unsafe { block_head.as_ref_unchecked().id.size() as usize },
                                    );
                                    add_list_head(block_head, &mut new_marked_list);
                                } else {
                                    debug_assert_eq!(local.ref_count(), 0);
                                    let new_remote = RemoteEntry::new(
                                        unsafe { block_head.as_ref_unchecked().remote_addr.into() },
                                        unsafe { block_head.as_ref_unchecked().id.size() as usize },
                                    );
                                    if let Err(old_state) = entry.cas_entry_weak(
                                        local.get(),
                                        new_remote.get(),
                                        Ordering::Relaxed,
                                    ) {
                                        entry_state = WrappedEntry::new(old_state);
                                        continue;
                                    }
                                    unsafe {
                                        block_head.as_mut_unchecked().deallocate();
                                    }
                                    add_list_head(block_head, &mut new_free_list);
                                    self.used_count -= 1;
                                }
                            } else {
                                add_list_head(block_head, {
                                    if local.is_evicting() {
                                        &mut new_marked_list
                                    } else {
                                        &mut new_active_list
                                    }
                                });
                            }
                        }
                        WrappedEntry::Remote(_) => {
                            add_list_head(block_head, &mut new_active_list);
                        }
                    }
                    break;
                }
            }
            block_head = next_block.inner();
        }
        self.marked_list = new_marked_list;
        self.active_list = new_active_list;
        self.free_list = new_free_list;
    }

    pub(crate) fn gc(&mut self) {
        let mut new_active_list = self.active_list;
        let mut new_marked_list: Option<NonNull<BlockHead>> = None;
        let mut new_free_list = self.free_list;
        let mut block_head = match self.marked_list {
            Some(block) => block.as_ptr(),
            None => {
                // println!("gc marked_list is none");
                std::ptr::null_mut()
            }
        };
        let add_list_head = |block: *mut BlockHead, list: &mut Option<NonNull<BlockHead>>| unsafe {
            block.as_mut_unchecked().next = match list {
                Some(list) => SendMutPtr::new(list.as_ptr()),
                None => SendMutPtr::null(),
            };
            *list = Some(NonNull::new_unchecked(block));
        };
        while !block_head.is_null() {
            let next_block = unsafe { block_head.as_ref_unchecked().next };
            let entry = unsafe { block_head.as_ref_unchecked().get_entry() };
            if entry.is_none() {
                // block is free
                add_list_head(block_head, &mut new_free_list);
                self.used_count -= 1;
            } else {
                let entry = entry.unwrap();
                let entry_state = entry.load();
                loop {
                    match entry_state {
                        WrappedEntry::Null => {
                            add_list_head(block_head, &mut new_free_list);
                            self.used_count -= 1;
                        }
                        WrappedEntry::Local(local) => {
                            if !std::ptr::addr_eq(local.addr() as *mut c_void, unsafe {
                                block_head.as_ref_unchecked().get_object_ptr()
                            }) {
                                continue;
                            }
                            add_list_head(block_head, {
                                if local.is_marked() || local.is_evicting() {
                                    &mut new_marked_list
                                } else {
                                    &mut new_active_list
                                }
                            });
                        }
                        WrappedEntry::Remote(_) => {
                            add_list_head(block_head, &mut new_active_list);
                        }
                    }
                    break;
                }
            }
            block_head = next_block.inner();
        }
        self.marked_list = new_marked_list;
        self.active_list = new_active_list;
        self.free_list = new_free_list;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::RootScope;
    use serial_test::serial;

    fn value_from_block<T>(block: NonNull<BlockHead>) -> NonNull<T> {
        unsafe { block.add(1).cast::<T>() }
    }

    #[test]
    #[serial]
    fn test_region_allocate() {
        let mut memory = Box::new([0 as u8; REGION_SIZE]);
        let region = unsafe { &mut *memory.as_mut_ptr().cast::<RegionHead>() };
        for test_bin in 0..REGION_BIN_COUNT {
            if bin_size(test_bin) <= size_of::<BlockHead>() + size_of::<usize>() {
                continue;
            }
            region.init(test_bin);
            assert!(region.can_allocate());
            let mut blocks = vec![];
            for i in 0.. {
                let block = unsafe { region.allocate(object::ID::from_meta(i)) };
                if block.is_none() {
                    break;
                }
                blocks.push(block.unwrap());
                let mut value = value_from_block::<u64>(block.unwrap());
                unsafe { *value.as_mut() = i };
            }
            assert_eq!(blocks.len(), region.used_count as usize);
            for (i, block) in blocks.into_iter().enumerate() {
                let value = value_from_block::<u64>(block);
                assert_eq!(unsafe { *value.as_ptr() }, i as u64);
            }
        }
    }

    #[test]
    #[serial]
    fn test_region_list() {
        let mut list = MaybeUninit::<RegionList>::uninit();
        unsafe {
            (*list.as_mut_ptr()).init();
        }
        let mut list = unsafe { list.assume_init() };
        assert!(list.head.next.is_none());
        assert!(list.tail.is_none());
        let regions = MaybeUninit::<[RegionHead; 10]>::uninit();
        let mut regions = unsafe { regions.assume_init() };
        for region in regions.iter_mut() {
            region.init(0);
            let region_ptr = unsafe { NonNull::new_unchecked(region as *mut RegionHead) };
            list.push(region_ptr);
        }
        let region_ptrs = regions
            .iter_mut()
            .map(|r| NonNull::new(r as *mut RegionHead))
            .collect::<Vec<_>>();
        assert_eq!(list.head.next.map(|p| p.cast()), region_ptrs[0]);
        assert_eq!(list.tail.map(|p| p.cast()), region_ptrs[9]);
        // the list should be FIFO
        for i in 0..regions.len() {
            let r = list.pop();
            assert_eq!(r, region_ptrs[i]);
        }
        assert!(list.head.next.is_none());
        assert!(list.tail.is_none());
    }

    #[test]
    #[serial]
    fn test_allocate_region() {
        let heap_size = 1024 * 1024 * 1024;
        assert!(heap_size % REGION_SIZE == 0);
        let heap_layout = std::alloc::Layout::from_size_align(heap_size, 4096).unwrap();
        unsafe {
            let memory = SendMutPtr::new(std::alloc::alloc(heap_layout));
            GlobalHeap::initialize(memory.inner() as *mut c_void, heap_size);
            let global_heap = GlobalHeap::instance();
            assert_eq!(global_heap.heap_size, heap_size);
            assert_eq!(global_heap.heap, memory.inner() as *mut c_void);
            assert_eq!(global_heap.free_size.load(Ordering::Relaxed), heap_size);
            assert!(!global_heap.memory_low());
            let num_regions = heap_size / REGION_SIZE;
            let mut regions = vec![];
            for i in 0..num_regions {
                assert_eq!(
                    global_heap.unallocated_offset.load(Ordering::Relaxed),
                    i * REGION_SIZE
                );
                let region = global_heap.allocate_region(6);
                assert!(!region.is_null());
                regions.push(region);
            }
            assert!(global_heap.memory_low());
            assert!(global_heap.allocate_region(6).is_null());
            assert_eq!(
                global_heap.free_size.load(Ordering::Relaxed),
                64 * num_regions
            );
            for region in regions.into_iter() {
                global_heap.return_back_region(region);
            }
            assert!(!global_heap.memory_low());
            for _ in 0..num_regions {
                let region = global_heap.allocate_region(6);
                assert!(!region.is_null());
            }
            assert!(global_heap.memory_low());
            GlobalHeap::destroy();
            std::alloc::dealloc(memory.inner(), heap_layout);
        }
    }

    #[test]
    #[serial]
    fn test_allocate_block() {
        unsafe { libfibre_port::cfibre_init(1) };
        let heap_size = REGION_SIZE * 4;
        let block_size = 1024;
        let allocate_count = REGION_SIZE * 2 / block_size;
        let heap_layout = std::alloc::Layout::from_size_align(heap_size, 4096).unwrap();
        let memory = unsafe { SendMutPtr::new(std::alloc::alloc(heap_layout)) };
        unsafe {
            GlobalHeap::initialize(memory.inner() as *mut c_void, heap_size);
        }

        let mut blocks = vec![];
        let scope = RootScope::root();
        for i in 0..allocate_count {
            let block =
                unsafe { allocate_block(block_size, object::ID::from_meta(i as u64), &scope) };
            assert!(block.is_some());
            unsafe { *value_from_block(block.unwrap()).as_mut() = i };
            blocks.push(block);
        }
        for (i, block) in blocks.into_iter().enumerate() {
            let value = value_from_block::<u64>(block.unwrap());
            assert_eq!(unsafe { *value.as_ptr() }, i as u64);
        }

        unsafe {
            GlobalHeap::destroy();
            std::alloc::dealloc(memory.inner(), heap_layout);
        }
    }
}
