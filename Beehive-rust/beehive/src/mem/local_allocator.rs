use std::intrinsics::{likely, unlikely};
use std::mem::MaybeUninit;
use std::os::raw::c_void;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::allocator_utils::*;
use super::object;
use super::scope::DerefScope;

#[repr(C)]
pub(crate) struct BlockHead {
    next: *mut BlockHead,
    id: object::ID,
}

#[repr(C)]
struct ListNode {
    next: Option<NonNull<ListNode>>,
}

#[repr(C)]
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

struct RegionList {
    lock: SpinLock,
    head: ListNode,
    tail: Option<NonNull<ListNode>>,
}

pub(super) struct GlobalHeap {
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
    regions: [*mut RegionHead; REGION_BIN_COUNT],
}

#[inline(never)]
pub(super) unsafe fn allocate_block(
    size: usize,
    id: object::ID,
    scope: &DerefScope,
) -> Option<NonNull<BlockHead>> {
    let wsize = wsize_from_size(size);
    let bin = bin_from_wsize(wsize);
    debug_assert!(bin_size(bin) >= size);
    debug_assert!(bin == 0 || bin_size(bin - 1) < size + size_of::<usize>());
    unsafe {
        let thread_heap = &raw mut THREAD_HEAP;
        (*thread_heap).allocate_block(bin, id, scope)
    }
}

#[thread_local]
static mut THREAD_HEAP: ThreadHeap = ThreadHeap::new();

static mut GLOBAL_HEAP: MaybeUninit<GlobalHeap> = MaybeUninit::uninit();

static GLOBAL_HEAP_INITIALIZED: AtomicBool = AtomicBool::new(false);

impl BlockHead {
    pub(super) fn deallocate(self: &mut BlockHead) {
        self.id = object::ID::null();
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

    #[allow(static_mut_refs)]
    fn instance() -> &'static mut GlobalHeap {
        unsafe { &mut *GLOBAL_HEAP.as_mut_ptr() }
    }

    fn return_back_region(self: &mut GlobalHeap, region: *mut RegionHead) {
        let region_ptr = unsafe { NonNull::new_unchecked(region) };
        let region = unsafe { &mut *region };
        if unlikely(region.can_allocate()) {
            let bin = region.bin;
            region.state = RegionState::Usable;
            self.usable_list[bin as usize].push(region_ptr);
            let region_free_size = region.free_size();
            self.free_size
                .fetch_add(region_free_size, Ordering::Relaxed);
        } else {
            region.state = RegionState::Full;
            self.full_list.push(region_ptr);
        }
    }

    /// allocate a new region with free blocks
    fn allocate_region(self: &mut GlobalHeap, bin: usize) -> *mut RegionHead {
        // first, try to allocate from the usable list
        let region = self.usable_list[bin].pop();
        if let Some(region) = region {
            let free_size = unsafe { region.as_ref().free_size() };
            self.free_size.fetch_sub(free_size, Ordering::Relaxed);
            return region.as_ptr();
        }
        // then, try to allocate from the free list
        let region = self.free_list.pop();
        if let Some(mut region) = region {
            unsafe { region.as_mut().init(bin) };
            self.free_size.fetch_sub(REGION_SIZE, Ordering::Relaxed);
            return region.as_ptr();
        }
        // finally, allocate a new region
        let offset = self.unallocated_offset.load(Ordering::Relaxed);
        loop {
            if offset + REGION_SIZE > self.heap_size {
                return ptr::null_mut();
            }
            if self
                .unallocated_offset
                .compare_exchange_weak(
                    offset,
                    offset + REGION_SIZE,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                let mut region =
                    unsafe { NonNull::new_unchecked(self.heap.add(offset).cast::<RegionHead>()) };
                unsafe { region.as_mut().init(bin) };
                self.free_size.fetch_sub(REGION_SIZE, Ordering::Relaxed);
                return region.as_ptr();
            }
        }
    }

    fn check_memory_low(self: &mut GlobalHeap, scope: &DerefScope) {
        if self.memory_low() {
            todo!()
        }
    }

    fn memory_low(self: &GlobalHeap) -> bool {
        self.free_size.load(Ordering::Relaxed) < self.memory_low_water_mark
    }
}

impl ThreadHeap {
    #[allow(static_mut_refs)]
    unsafe fn allocate_block(
        self: &mut ThreadHeap,
        bin: usize,
        id: object::ID,
        scope: &DerefScope,
    ) -> Option<NonNull<BlockHead>> {
        let region = self.regions[bin];
        if likely(!region.is_null()) {
            let block = unsafe { (*region).allocate(id) };
            if let Some(block) = block {
                return Some(block);
            }
            GlobalHeap::instance().return_back_region(region);
        }
        // region is null, try to allocate a new region
        let global_heap = GlobalHeap::instance();
        let region = global_heap.allocate_region(bin);
        global_heap.check_memory_low(scope);
        self.regions[bin] = region;
        if unlikely(region.is_null()) {
            return None;
        } else {
            unsafe { (*region).state = RegionState::InUse };
            unsafe { (*region).allocate(id) }
        }
    }

    const fn new() -> ThreadHeap {
        ThreadHeap {
            regions: [ptr::null_mut(); REGION_BIN_COUNT],
        }
    }
}

impl RegionList {
    fn init(self: &mut RegionList) {
        self.lock = SpinLock::new();
        self.head = ListNode { next: None };
        self.tail = None;
    }

    /// push a node to the tail of the list
    fn push(&mut self, node: NonNull<RegionHead>) {
        self.lock.lock();
        match self.tail {
            Some(tail) => {
                unsafe { (*tail.as_ptr()).next = Some(node.cast()) };
                self.tail = Some(node.cast());
            }
            None => {
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
                self.lock.unlock();
                None
            }
        }
    }

    fn pop_unmatched(&mut self, time_stamp: u32) -> Option<NonNull<RegionHead>> {
        todo!()
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
                self.free_list = Some(NonNull::new_unchecked(block.as_ref().next));
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
                block.as_mut().next = self.active_list.map_or(ptr::null_mut(), |p| p.as_ptr());
                self.active_list = Some(block);
                block.as_mut().id = id;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn value_from_block<T>(block: NonNull<BlockHead>) -> NonNull<T> {
        unsafe { block.add(1).cast::<T>() }
    }

    #[test]
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
            println!(
                "bin: {}, size: {}, count: {}",
                test_bin,
                bin_size(test_bin),
                blocks.len()
            );
            assert_eq!(blocks.len(), region.used_count as usize);
            for (i, block) in blocks.into_iter().enumerate() {
                let value = value_from_block::<u64>(block);
                assert_eq!(unsafe { *value.as_ptr() }, i as u64);
            }
        }
    }

    #[test]
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
            let memory = std::alloc::alloc(heap_layout);
            GlobalHeap::initialize(memory as *mut c_void, heap_size);
            let global_heap = GlobalHeap::instance();
            assert_eq!(global_heap.heap_size, heap_size);
            assert_eq!(global_heap.heap, memory as *mut c_void);
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
            assert!(global_heap.free_size.load(Ordering::Relaxed) == 0);
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
            std::alloc::dealloc(memory, heap_layout);
        }
    }

    #[test]
    #[serial]
    fn test_allocate_block() {
        let heap_size = REGION_SIZE * 4;
        let block_size = 1024;
        let allocate_count = REGION_SIZE * 2 / block_size;
        let heap_layout = std::alloc::Layout::from_size_align(heap_size, 4096).unwrap();
        let memory = unsafe { std::alloc::alloc(heap_layout) };
        unsafe {
            GlobalHeap::initialize(memory as *mut c_void, heap_size);
        }

        let mut blocks = vec![];
        let scope = DerefScope {};
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
            std::alloc::dealloc(memory, heap_layout);
        }
    }
}
