use crate::mem::allocator_utils::*;
use crate::net::Config;
use crate::utils::bitfield::*;
use crate::utils::pointer::*;
use crate::utils::spinlock::SpinPollingLock;
use once_cell::sync::Lazy;
use std::fmt::Display;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{array, intrinsics::unlikely};

static mut REMOTE_GLOBAL_HEAP: Lazy<RemoteGlobalHeap> = Lazy::new(|| RemoteGlobalHeap::new());

#[thread_local]
static mut REMOTE_THREAD_HEAP: Lazy<RemoteThreadHeap> = Lazy::new(|| RemoteThreadHeap::new());

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct RemoteAddr(u64);

impl RemoteAddr {
    const OFFSET_BIT_COUNT: usize = 40; // 1TB for each server
    define_bits!(offset, 0, RemoteAddr::OFFSET_BIT_COUNT - 1);
    define_bits!(server_id, RemoteAddr::OFFSET_BIT_COUNT, 47);
    define_bits!(reserved, 48, 63);

    pub fn new(offset: u64, server_id: u64) -> Self {
        RemoteAddr {
            0: Self::init_offset(offset) | Self::init_server_id(server_id) | Self::init_reserved(0),
        }
    }

    pub fn null() -> Self {
        Self::new_from_addr(0)
    }

    pub fn addr(&self) -> u64 {
        self.offset() | (self.server_id() << RemoteAddr::OFFSET_BIT_COUNT)
    }

    pub fn new_from_addr(addr: u64) -> Self {
        Self { 0: addr }
    }
}

impl From<RemoteAddr> for u64 {
    fn from(raddr: RemoteAddr) -> Self {
        raddr.0
    }
}

impl Display for RemoteAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.server_id(), self.offset())
    }
}

impl Add<u64> for RemoteAddr {
    type Output = Self;
    fn add(self, rhs: u64) -> Self {
        RemoteAddr::new(self.offset() + rhs, self.server_id())
    }
}

impl AddAssign<u64> for RemoteAddr {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl Sub<RemoteAddr> for RemoteAddr {
    type Output = u64;
    fn sub(self, rhs: RemoteAddr) -> u64 {
        debug_assert!(rhs.server_id() == self.server_id());
        self.offset() - rhs.offset()
    }
}

impl Sub<u64> for RemoteAddr {
    type Output = Self;
    fn sub(self, rhs: u64) -> Self {
        RemoteAddr::new(self.offset() - rhs, self.server_id())
    }
}

impl SubAssign<u64> for RemoteAddr {
    fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct DoubleLinkedListHead {
    next: Option<SendNonNull<DoubleLinkedListHead>>,
    prev: Option<SendNonNull<DoubleLinkedListHead>>,
}

unsafe impl Sync for DoubleLinkedListHead {}
unsafe impl Send for DoubleLinkedListHead {}

impl DoubleLinkedListHead {
    pub fn new() -> Self {
        DoubleLinkedListHead {
            next: None,
            prev: None,
        }
    }

    pub unsafe fn insert_front_unsafe(&mut self, mut node: SendNonNull<DoubleLinkedListHead>) {
        unsafe {
            debug_assert!(node.as_ref().next.is_none());
            debug_assert!(node.as_ref().prev.is_none());
            node.as_mut().next = Some(SendNonNull::new_unchecked(self));
            if let Some(mut prev) = self.prev {
                node.as_mut().prev = Some(SendNonNull::new_unchecked(prev.as_ptr()));
                prev.as_mut().next = Some(node);
            } else {
                node.as_mut().prev = None;
            }
        }
        self.prev = Some(node);
    }

    pub unsafe fn insert_back_unsafe(&mut self, mut node: SendNonNull<DoubleLinkedListHead>) {
        unsafe {
            debug_assert!(node.as_ref().next.is_none());
            debug_assert!(node.as_ref().prev.is_none());
            node.as_mut().prev = Some(SendNonNull::new_unchecked(self));
            if let Some(mut next) = self.next {
                node.as_mut().next = Some(SendNonNull::new_unchecked(next.as_ptr()));
                next.as_mut().prev = Some(node);
            } else {
                node.as_mut().next = None;
            }
        }
        self.prev = Some(node);
    }

    pub unsafe fn remove_self_unsafe(&mut self) {
        debug_assert!(self.next.is_some() && self.prev.is_some());
        unsafe {
            self.prev.unwrap().as_mut().next = self.next;
            self.next.unwrap().as_mut().prev = self.prev;
        }
        self.prev = None;
        self.next = None;
    }

    pub fn reset(&mut self) {
        self.next = None;
        self.prev = None;
    }
}

#[derive(Debug)]
#[repr(C)]
struct RemoteRegionHead {
    header: DoubleLinkedListHead, // Must at the head
    lock: SpinPollingLock,
    bin: u32,
    last_map_idx: u32,
    block_bitmap: Vec<u64>,
    entry_size: usize,
    base_addr: RemoteAddr,
    used_count: u64,
}

unsafe impl Sync for RemoteRegionHead {}
unsafe impl Send for RemoteRegionHead {}

impl RemoteRegionHead {
    const MAP_ELEMENT_BIT_COUNT: usize = std::mem::size_of::<u64>() * 8;
    const FREE_BIT_MAP: u64 = u64::MAX;

    pub fn lock(&mut self) {
        self.lock.lock();
    }

    pub fn unlock(&mut self) {
        self.lock.unlock();
    }

    pub fn new() -> Self {
        RemoteRegionHead {
            header: DoubleLinkedListHead::new(),
            lock: SpinPollingLock::new(),
            bin: 0,
            last_map_idx: 0,
            block_bitmap: vec![],
            entry_size: 0,
            base_addr: RemoteAddr::new(0, 0),
            used_count: 0,
        }
    }

    pub fn init(&mut self, server_id: u64, base_addr: u64, bin: u32) {
        self.header = DoubleLinkedListHead::new();
        self.lock = SpinPollingLock::new();
        self.base_addr = RemoteAddr::new(base_addr, server_id);
        self.reset::<true>(bin);
    }

    pub fn reset<const INIT: bool>(&mut self, bin: u32) {
        debug_assert!(!self.lock.is_locked());
        self.entry_size = REGION_SIZE / bin_size(bin as usize);
        let map_size: usize =
            (self.entry_size + Self::MAP_ELEMENT_BIT_COUNT - 1) / Self::MAP_ELEMENT_BIT_COUNT;
        (INIT || self.bin != bin).then(|| {
            self.block_bitmap = vec![Self::FREE_BIT_MAP; map_size];
        });
        // specially handle last bitmap
        if self.entry_size % Self::MAP_ELEMENT_BIT_COUNT != 0 {
            let last_bitmap = self.block_bitmap.last_mut().unwrap();
            *last_bitmap = (1u64 << (self.entry_size % Self::MAP_ELEMENT_BIT_COUNT)) - 1;
        }
        self.last_map_idx = 0;
        self.used_count = 0;
        self.bin = bin;
    }

    pub fn allocate_unsafe(&mut self) -> Option<RemoteAddr> {
        let map_size: usize =
            (self.entry_size + Self::MAP_ELEMENT_BIT_COUNT - 1) / Self::MAP_ELEMENT_BIT_COUNT;
        let mut addr = None;
        for _ in 0..map_size {
            let bit = self.block_bitmap[self.last_map_idx as usize].trailing_zeros();
            if bit != 64 {
                self.block_bitmap[self.last_map_idx as usize] ^= 1 << bit;
                self.used_count += 1;
                addr = Some(
                    self.base_addr
                        + ((Self::MAP_ELEMENT_BIT_COUNT * self.last_map_idx as usize
                            + bit as usize)
                            * bin_size(self.bin as usize)) as u64,
                );
                break;
            } else {
                self.last_map_idx += 1;
                unlikely(self.last_map_idx >= map_size as u32).then(|| {
                    self.last_map_idx = 0;
                });
            }
        }
        addr
    }

    pub fn allocate(&mut self) -> Option<RemoteAddr> {
        self.lock();
        let addr = self.allocate_unsafe();
        self.unlock();
        addr
    }

    pub fn deallocate_unsafe(&mut self, raddr: RemoteAddr) {
        debug_assert!(raddr.server_id() == self.base_addr.server_id());
        let addr = raddr.offset();
        debug_assert!(
            addr >= self.base_addr.offset() && addr < self.base_addr.offset() + REGION_SIZE as u64
        );
        let offset: u64 = (addr - self.base_addr.offset()) / bin_size(self.bin as usize) as u64;
        self.block_bitmap[(offset / Self::MAP_ELEMENT_BIT_COUNT as u64) as usize] |=
            1 << (offset % Self::MAP_ELEMENT_BIT_COUNT as u64);
        self.used_count -= 1;
    }

    pub fn is_free_unsafe(&self) -> bool {
        self.used_count == 0
    }

    pub fn is_full_unsafe(&self) -> bool {
        self.used_count == self.entry_size as u64
    }

    pub fn is_full_just_now_unsafe(&self) -> bool {
        self.used_count == self.entry_size as u64 - 1
    }

    pub fn get_bin(&self) -> u32 {
        self.bin
    }

    pub fn get_used_count(&self) -> u64 {
        self.used_count
    }

    pub fn is_in_thread_heap_unsafe(&self) -> bool {
        debug_assert!(!(self.header.next.is_none() ^ self.header.prev.is_none()));
        self.header.next.is_none() && self.header.prev.is_none()
    }

    pub fn remove_self(&mut self) {
        self.lock();
        unsafe {
            self.header.remove_self_unsafe();
        }
        debug_assert!(self.is_in_thread_heap_unsafe());
        self.unlock();
    }
}

#[derive(Debug)]
struct RemoteRegionList {
    lock: SpinPollingLock,
    dummy_head: DoubleLinkedListHead,
    dummy_tail: DoubleLinkedListHead,
}

unsafe impl Sync for RemoteRegionList {}
unsafe impl Send for RemoteRegionList {}

impl RemoteRegionList {
    fn lock(&mut self) {
        self.lock.lock();
    }

    fn unlock(&mut self) {
        self.lock.unlock();
    }

    pub fn new() -> Self {
        RemoteRegionList {
            lock: SpinPollingLock::new(),
            dummy_head: DoubleLinkedListHead::new(),
            dummy_tail: DoubleLinkedListHead::new(),
        }
    }

    pub fn init(&mut self) {
        // manual init is neccesary
        // for this will be moved during construction
        // TODO maybe we can fix it, make it not ugly
        unsafe {
            self.dummy_head.next = Some(SendNonNull::new_unchecked(&mut self.dummy_tail));
            self.dummy_tail.prev = Some(SendNonNull::new_unchecked(&mut self.dummy_head));
        }
    }

    pub fn insert_tail(&mut self, node: SendNonNull<RemoteRegionHead>) {
        self.lock();
        unsafe {
            debug_assert!(node.as_ref().is_in_thread_heap_unsafe());
            self.dummy_tail.insert_front_unsafe(node.cast());
        }
        self.unlock();
    }

    pub fn insert_head(&mut self, node: SendNonNull<RemoteRegionHead>) {
        self.lock();
        unsafe {
            self.dummy_head.insert_back_unsafe(node.cast());
        }
        self.unlock();
    }

    pub fn empty(&self) -> bool {
        let is_head_next_to_tail = self
            .dummy_head
            .next
            .expect("Region list dummy_head.next should not none!")
            .as_ptr() as *const _
            == &self.dummy_tail;
        let is_tail_prev_to_head = self
            .dummy_tail
            .prev
            .expect("Region list dummy_tail.prev should not none!")
            .as_ptr() as *const _
            == &self.dummy_head;
        debug_assert!(!(is_head_next_to_tail ^ is_tail_prev_to_head));
        is_head_next_to_tail
    }

    pub fn pop_head(&mut self) -> Option<SendNonNull<RemoteRegionHead>> {
        self.lock();
        if self.empty() {
            self.unlock();
            None
        } else {
            let region = self.dummy_head.next.map(|next| unsafe {
                let mut region = next.cast::<RemoteRegionHead>();
                region.as_mut().remove_self();
                region
            });
            debug_assert!(region.is_some());
            self.unlock();
            region
        }
    }

    pub fn pop_tail(&mut self) -> Option<SendNonNull<RemoteRegionHead>> {
        self.lock();
        if self.empty() {
            self.unlock();
            None
        } else {
            let region = self.dummy_tail.prev.map(|prev| unsafe {
                let mut region = prev.cast::<RemoteRegionHead>();
                region.as_mut().remove_self();
                region
            });
            debug_assert!(region.is_some());
            self.unlock();
            region
        }
    }

    pub unsafe fn remove_from_list(&mut self, mut region: SendNonNull<RemoteRegionHead>) {
        self.lock();
        unsafe {
            region.as_mut().header.remove_self_unsafe();
        }
        self.unlock();
    }
}

#[derive(Debug)]
struct RemoteServerRegionHeap {
    server_id: u64,
    used_heap_idx: AtomicUsize,
    regions: Box<[RemoteRegionHead]>,
    regions_size: usize,
}

impl RemoteServerRegionHeap {
    pub fn new(server_id: u64, buffer_size: usize) -> Self {
        let regions_size = (buffer_size + REGION_SIZE - 1) / REGION_SIZE;
        unsafe {
            Self {
                server_id,
                used_heap_idx: AtomicUsize::new(0),
                regions: Box::new_uninit_slice(regions_size).assume_init(),
                regions_size: regions_size,
            }
        }
    }

    pub fn allocate_region_init(&mut self, bin: usize) -> Option<SendNonNull<RemoteRegionHead>> {
        let mut idx = self.used_heap_idx.load(Ordering::Relaxed);
        loop {
            if idx < self.regions_size {
                let new_idx = idx + 1;
                match self.used_heap_idx.compare_exchange_weak(
                    idx,
                    new_idx,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        self.regions[idx].init(
                            self.server_id,
                            (idx * REGION_SIZE) as u64,
                            bin as u32,
                        );
                        unsafe {
                            let region = Some(SendNonNull::new_unchecked(&mut self.regions[idx]));
                            debug_assert!(region.unwrap().as_ref().is_in_thread_heap_unsafe());
                            return region;
                        }
                    }
                    Err(new_val) => idx = new_val,
                }
            } else {
                return None;
            }
        }
    }

    pub fn addr_to_region(&mut self, raddr: RemoteAddr) -> SendNonNull<RemoteRegionHead> {
        debug_assert!(raddr.server_id() == self.server_id);
        unsafe {
            SendNonNull::new_unchecked(&mut self.regions[raddr.offset() as usize / REGION_SIZE])
        }
    }
}
#[derive(Debug)]
pub(crate) struct RemoteGlobalHeap {
    usable_region_list: [RemoteRegionList; REGION_BIN_COUNT],
    full_region_list: RemoteRegionList,
    free_region_list: RemoteRegionList,
    server_heaps: Vec<RemoteServerRegionHeap>,
}

unsafe impl Sync for RemoteGlobalHeap {}
unsafe impl Send for RemoteGlobalHeap {}

impl RemoteGlobalHeap {
    fn addr_to_region(&mut self, raddr: RemoteAddr) -> SendNonNull<RemoteRegionHead> {
        self.server_heaps[raddr.server_id() as usize].addr_to_region(raddr)
    }

    pub fn new() -> Self {
        RemoteGlobalHeap {
            usable_region_list: array::from_fn(|_| RemoteRegionList::new()),
            full_region_list: RemoteRegionList::new(),
            free_region_list: RemoteRegionList::new(),
            server_heaps: vec![],
        }
    }

    pub fn register_remote_vec(&mut self, buffer_sizes: Vec<usize>) {
        if self.server_heaps.len() == 0 {
            for (id, buffer_size) in buffer_sizes.iter().enumerate() {
                self.server_heaps
                    .push(RemoteServerRegionHeap::new(id as u64, *buffer_size));
            }
            self.full_region_list.init();
            self.free_region_list.init();
            for url in self.usable_region_list.iter_mut() {
                url.init();
            }
            debug_assert!(self.full_region_list.empty());
            debug_assert!(self.free_region_list.empty());
            for url in self.usable_region_list.iter() {
                debug_assert!(url.empty());
            }
        } else {
            panic!("duplicate init Remote Global Heap!");
        }
        log::info!(
            "Register remote memory size: {} MB",
            buffer_sizes.iter().sum::<usize>() / (1 << 20)
        );
        for (id, buffer_size) in buffer_sizes.iter().enumerate() {
            log::info!(
                "server {id} remote heap size: {} MB",
                *buffer_size / (1 << 20)
            );
        }
    }

    pub fn register_remote(&mut self, server_num: usize, size: usize) {
        self.register_remote_vec(vec![size; server_num]);
    }

    pub fn deregister_remote(&mut self) {
        for serv_heap in self.server_heaps.iter() {
            log::info!(
                "Deregister remote server {} heap size: {} MB",
                serv_heap.server_id,
                serv_heap.regions_size * REGION_SIZE / (1 << 20)
            );
        }
        self.server_heaps.clear();
    }

    fn allocate_region(&mut self, bin: usize) -> Option<SendNonNull<RemoteRegionHead>> {
        if let Some(region) = self.usable_region_list[bin].pop_head() {
            unsafe {
                debug_assert!(region.as_ref().is_in_thread_heap_unsafe());
            }
            return Some(region);
        }
        if let Some(mut region) = self.free_region_list.pop_head() {
            unsafe {
                region.as_mut().reset::<false>(bin as u32);
                debug_assert!(region.as_ref().is_in_thread_heap_unsafe());
            }
            return Some(region);
        }
        for serv_heap in self.server_heaps.iter_mut() {
            if let Some(region) = serv_heap.allocate_region_init(bin) {
                return Some(region);
            }
        }
        None
    }

    fn return_back_region(&mut self, mut region: SendNonNull<RemoteRegionHead>) {
        unsafe {
            region.as_mut().lock();
        }
        self.return_back_region_unsafe(region);
        unsafe {
            region.as_mut().unlock();
        }
    }

    fn return_back_region_unsafe(&mut self, region: SendNonNull<RemoteRegionHead>) {
        unsafe {
            debug_assert!(region.as_ref().is_in_thread_heap_unsafe());
            if region.as_ref().is_full_unsafe() {
                debug_assert!(region.as_ref().is_in_thread_heap_unsafe());
                self.full_region_list.insert_tail(region);
            } else if region.as_ref().is_free_unsafe() {
                self.free_region_list.insert_tail(region);
            } else {
                self.usable_region_list[region.as_ref().get_bin() as usize].insert_tail(region);
            }
        }
    }

    pub fn deallocate(&mut self, raddr: RemoteAddr) {
        let mut region = self.addr_to_region(raddr);
        unsafe {
            region.as_mut().lock();
            self.deallocate_unsafe(raddr, region);
            region.as_mut().unlock();
        }
    }

    fn deallocate_unsafe(&mut self, raddr: RemoteAddr, mut region: SendNonNull<RemoteRegionHead>) {
        unsafe {
            region.as_mut().deallocate_unsafe(raddr);
            (!region.as_ref().is_in_thread_heap_unsafe()).then(|| {
                if unlikely(region.as_ref().is_free_unsafe()) {
                    self.usable_region_list[region.as_ref().get_bin() as usize]
                        .remove_from_list(region);
                    self.free_region_list.insert_tail(region);
                } else if unlikely(region.as_ref().is_full_just_now_unsafe()) {
                    self.full_region_list.remove_from_list(region);
                    self.usable_region_list[region.as_ref().get_bin() as usize].insert_tail(region);
                }
            });
        }
    }
}

impl Drop for RemoteGlobalHeap {
    fn drop(&mut self) {
        self.deregister_remote();
    }
}

struct RemoteThreadHeap {
    regions: [Option<SendNonNull<RemoteRegionHead>>; REGION_BIN_COUNT],
}

#[allow(static_mut_refs)]
impl RemoteThreadHeap {
    pub fn new() -> Self {
        RemoteThreadHeap {
            regions: [None; REGION_BIN_COUNT],
        }
    }

    pub fn allocate(&mut self, size: usize) -> Option<RemoteAddr> {
        let wsize: usize = wsize_from_size(size);
        let bin: usize = bin_from_wsize(wsize);
        debug_assert!(bin_size(bin) >= size);
        debug_assert!(bin == 0 || bin_size(bin - 1) < size + size_of::<usize>());
        let mut region = self.regions[bin];
        if let Some(mut region) = region {
            unsafe {
                debug_assert!(region.as_ref().is_in_thread_heap_unsafe());
                region.as_mut().lock();
                match region.as_mut().allocate_unsafe() {
                    Some(addr) => {
                        region.as_mut().unlock();
                        return Some(addr);
                    }
                    _ => {
                        REMOTE_GLOBAL_HEAP.return_back_region_unsafe(region);
                        region.as_mut().unlock();
                    }
                }
            }
        }
        unsafe {
            region = REMOTE_GLOBAL_HEAP.allocate_region(bin);
        }
        self.regions[bin] = region;
        match region {
            Some(mut region) => unsafe {
                debug_assert!(region.as_ref().is_in_thread_heap_unsafe());
                region.as_mut().allocate()
            },
            _ => None,
        }
    }

    pub fn deallocate(&mut self, raddr: RemoteAddr) {
        unsafe {
            REMOTE_GLOBAL_HEAP.deallocate(raddr);
        }
    }
}

#[allow(static_mut_refs)]
impl Drop for RemoteThreadHeap {
    fn drop(&mut self) {
        for region in self.regions.iter() {
            if let Some(region) = region {
                unsafe {
                    REMOTE_GLOBAL_HEAP.return_back_region(*region);
                }
            }
        }
    }
}

#[allow(static_mut_refs)]
pub(super) fn allocate_remote(size: usize) -> Option<RemoteAddr> {
    unsafe { REMOTE_THREAD_HEAP.allocate(size) }
}

#[allow(static_mut_refs)]
pub(super) fn deallocate_remote(raddr: RemoteAddr) {
    unsafe {
        REMOTE_THREAD_HEAP.deallocate(raddr);
    }
}

#[allow(static_mut_refs)]
pub(super) fn init_remote_heap(config: &Config) {
    let server_configs = &config.servers;
    let server_memory_sizes = server_configs.iter().map(|cfg| cfg.memory_size).collect();
    unsafe {
        REMOTE_GLOBAL_HEAP.register_remote_vec(server_memory_sizes);
    }
}

#[cfg(test)]
#[allow(static_mut_refs)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        thread,
    };

    use serial_test::serial;

    use crate::mem::remote_allocator::*;
    #[test]
    #[serial]
    pub fn remote_allocator_test_single_thread() {
        const REMOTE_BUF_SIZE: usize = 1 << 30;
        const ALLOC_SIZE: usize = 1 << 10; // 1K
        const SERVER_NUM: usize = 1;
        unsafe {
            REMOTE_GLOBAL_HEAP.register_remote(SERVER_NUM, REMOTE_BUF_SIZE);
        }
        let mut addrs = vec![];
        for _ in 0..REMOTE_BUF_SIZE / ALLOC_SIZE {
            match allocate_remote(ALLOC_SIZE) {
                Some(addr) => addrs.push(addr),
                _ => break,
            }
        }
        let available_cnt = addrs.len();
        debug_assert!(available_cnt > 0);
        for i in 0..available_cnt - 1 {
            assert!(
                addrs[i].server_id() == 0
                    && addrs[i].offset() + ALLOC_SIZE as u64 <= addrs[i + 1].offset()
            );
        }
        assert!(
            addrs.last().unwrap().server_id() == 0
                && addrs.last().unwrap().offset() + ALLOC_SIZE as u64 <= REMOTE_BUF_SIZE as u64
        );
        for addr in addrs.iter() {
            deallocate_remote(*addr);
        }
        // alloc again
        addrs.clear();
        for _ in 0..REMOTE_BUF_SIZE / ALLOC_SIZE {
            match allocate_remote(ALLOC_SIZE) {
                Some(addr) => addrs.push(addr),
                _ => break,
            }
        }
        assert_eq!(addrs.len(), available_cnt);
        unsafe {
            REMOTE_GLOBAL_HEAP.deregister_remote();
        }
    }

    #[test]
    #[serial]
    pub fn remote_allocator_test_multi_thread() {
        const REMOTE_BUF_SIZE: usize = 1 << 30;
        const ALLOC_SIZE: usize = 1 << 10; // 1K
        const NUM_THREADS: usize = 16;
        const SERVER_NUM: usize = 1;
        unsafe {
            REMOTE_GLOBAL_HEAP.register_remote(SERVER_NUM, REMOTE_BUF_SIZE);
        }
        let global_addrs: Arc<Mutex<Vec<RemoteAddr>>> = Arc::new(Mutex::new(vec![]));
        let mut threads = vec![];
        for _ in 0..NUM_THREADS {
            let ga = global_addrs.clone();
            threads.push(thread::spawn(move || {
                let mut addrs = vec![];
                for _ in 0..REMOTE_BUF_SIZE / ALLOC_SIZE {
                    match allocate_remote(ALLOC_SIZE) {
                        Some(addr) => addrs.push(addr),
                        _ => break,
                    }
                }
                {
                    let handle = ga.clone();
                    let mut gaddrs = handle.lock().unwrap();
                    gaddrs.extend(addrs.iter());
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        let mut gaddrs = global_addrs.lock().unwrap();
        gaddrs.sort();
        let available_cnt = gaddrs.len();
        for i in 0..available_cnt - 1 {
            assert!(
                gaddrs[i].server_id() == 0
                    && gaddrs[i].offset() + ALLOC_SIZE as u64 <= gaddrs[i + 1].offset()
            );
        }
        assert!(
            gaddrs.last().unwrap().server_id() == 0
                && gaddrs.last().unwrap().offset() + ALLOC_SIZE as u64 <= REMOTE_BUF_SIZE as u64
        );
        for raddr in gaddrs.iter() {
            deallocate_remote(*raddr);
        }
        unsafe {
            REMOTE_GLOBAL_HEAP.deregister_remote();
        }
    }

    #[test]
    #[serial]
    pub fn remote_allocator_test_multi_server() {
        const REMOTE_BUF_SIZE: usize = 1 << 26;
        const ALLOC_SIZE: usize = 1 << 10; // 1K
        const NUM_THREADS: usize = 32;
        const SERVER_NUM: usize = 8;
        unsafe {
            REMOTE_GLOBAL_HEAP.register_remote(SERVER_NUM, REMOTE_BUF_SIZE);
        }
        let global_addrs: Arc<Mutex<Vec<RemoteAddr>>> = Arc::new(Mutex::new(vec![]));
        let mut threads = vec![];
        for _ in 0..NUM_THREADS {
            let ga = global_addrs.clone();
            threads.push(thread::spawn(move || {
                let mut addrs = vec![];
                for _ in 0..REMOTE_BUF_SIZE / ALLOC_SIZE {
                    match allocate_remote(ALLOC_SIZE) {
                        Some(addr) => addrs.push(addr),
                        _ => break,
                    }
                }
                {
                    let handle = ga.clone();
                    let mut gaddrs = handle.lock().unwrap();
                    gaddrs.extend(addrs.iter());
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        let mut gaddrs = global_addrs.lock().unwrap();
        gaddrs.sort();
        let available_cnt = gaddrs.len();
        assert!(available_cnt > 0);
        for i in 0..available_cnt - 1 {
            let addr_i = &gaddrs[i];
            let addr_i1 = &gaddrs[i + 1];
            (addr_i.server_id() == addr_i1.server_id()).then(|| {
                assert!(addr_i.offset() + ALLOC_SIZE as u64 <= addr_i1.offset());
            });
        }
        assert!(gaddrs.last().unwrap().offset() + ALLOC_SIZE as u64 <= REMOTE_BUF_SIZE as u64);
        for raddr in gaddrs.iter() {
            deallocate_remote(*raddr);
        }
        unsafe {
            REMOTE_GLOBAL_HEAP.deregister_remote();
        }
    }
}
