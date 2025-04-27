use core::array;
use std::alloc::Layout;
use std::intrinsics::unlikely;
use std::ptr::drop_in_place;

#[repr(C)]
struct TaskNode {
    next: *mut TaskNode,
    shift: usize,
}

struct TaskList {
    node: *mut TaskNode,
    shift: usize,
    batch_layout: Layout,
    allocated_ptr: Vec<*mut u8>,
}

impl TaskList {
    const BATCH_SIZE: usize = 8;
    pub(crate) fn new(shift: usize) -> Self {
        let batch_layout = Layout::array::<u8>(Self::BATCH_SIZE * (1usize << shift)).unwrap();
        Self {
            node: std::ptr::null_mut(),
            shift,
            batch_layout,
            allocated_ptr: Vec::new(),
        }
    }

    pub(crate) fn alloc(&mut self) -> *mut TaskNode {
        if unlikely(self.is_empty()) {
            self.alloc_batch();
        }
        debug_assert!(!self.is_empty());
        debug_assert!(
            unsafe { (*self.node).next }.is_null()
                || unsafe { (*self.node).next as u64 } >= 0x5000_0000_0000
        );
        let node = self.node;
        self.node = unsafe { (*node).next };
        node
    }

    pub(crate) fn dealloc(&mut self, node: *mut TaskNode) {
        unsafe { (*node).next = self.node };
        self.node = node;
        debug_assert!(!self.is_empty());
        debug_assert!(
            unsafe { (*self.node).next }.is_null()
                || unsafe { (*self.node).next as u64 } >= 0x5000_0000_0000
        );
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.node.is_null()
    }

    fn alloc_batch(&mut self) {
        debug_assert!(self.is_empty());
        let buffer = unsafe { std::alloc::alloc(self.batch_layout) };
        (0..Self::BATCH_SIZE).for_each(|i| {
            let node = unsafe { buffer.add(i * (1usize << self.shift)) as *mut TaskNode };
            unsafe {
                (*node).next = std::ptr::null_mut();
                (*node).shift = self.shift;
            }
        });
        for i in 0..(Self::BATCH_SIZE - 1) {
            let node = unsafe { buffer.add(i * (1usize << self.shift)) as *mut TaskNode };
            let next_node =
                unsafe { buffer.add((i + 1) * (1usize << self.shift)) as *mut TaskNode };
            unsafe { (*node).next = next_node };
        }
        self.node = buffer as *mut TaskNode;
        let mut node = self.node;
        while !unsafe { (*node).next }.is_null() {
            let next_node = unsafe { (*node).next };
            assert_eq!(
                unsafe { (node as *mut u8).add(1usize << self.shift) },
                next_node as *mut u8
            );
            node = next_node;
        }
        self.allocated_ptr.push(buffer as *mut u8);
    }
}

impl Drop for TaskList {
    fn drop(&mut self) {
        self.allocated_ptr.iter().for_each(|ptr| {
            unsafe { std::alloc::dealloc(*ptr, self.batch_layout) };
        });
    }
}

pub(crate) struct TaskAllocator {
    free_lists: [TaskList; Self::NODE_SIZE_SHIFT_MAX - Self::NODE_SIZE_SHIFT_MIN + 1],
}

impl TaskAllocator {
    const NODE_SIZE_SHIFT_MIN: usize = 6;
    const NODE_SIZE_SHIFT_MAX: usize = 30;
    pub(crate) fn new() -> Self {
        Self {
            free_lists: array::from_fn(|i| TaskList::new(Self::NODE_SIZE_SHIFT_MIN + i)),
        }
    }

    pub(crate) fn alloc<T>(&mut self) -> *mut T {
        let size = std::mem::size_of::<T>() + std::mem::size_of::<TaskNode>();
        let shift =
            (size.next_power_of_two().trailing_zeros() as usize).max(Self::NODE_SIZE_SHIFT_MIN);
        debug_assert!(shift <= Self::NODE_SIZE_SHIFT_MAX);
        let free_list = &mut self.free_lists[shift - Self::NODE_SIZE_SHIFT_MIN];
        let node = free_list.alloc();
        unsafe { node.add(1) as *mut T }
    }

    pub(crate) fn free<T: ?Sized>(&mut self, ptr: *mut T) {
        unsafe {
            drop_in_place(ptr);
        }
        let node = unsafe { (ptr as *mut TaskNode).sub(1) };
        let shift = unsafe { (*node).shift };
        self.free_lists[shift - Self::NODE_SIZE_SHIFT_MIN].dealloc(node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_task_allocator() {
        const NODES_COUNT: usize = 1000;
        let mut allocator = TaskAllocator::new();
        let start = Instant::now();
        let nodes = (0..NODES_COUNT)
            .map(|_| allocator.alloc::<[u8; 512]>())
            .collect::<Vec<_>>();
        let alloc_fresh_time = start.elapsed();
        println!(
            "alloc fresh time per node: {:?}",
            alloc_fresh_time.div_f64(NODES_COUNT as f64)
        );
        let start = Instant::now();
        nodes.iter().for_each(|node| {
            allocator.free(*node);
        });
        let free_time = start.elapsed();
        println!(
            "free time per node: {:?}",
            free_time.div_f64(NODES_COUNT as f64)
        );
        let start = Instant::now();
        (0..NODES_COUNT).for_each(|_| {
            let node = allocator.alloc::<[u8; 512]>();
            allocator.free(node);
        });
        let alloc_free_time = start.elapsed();
        println!(
            "alloc free time per node: {:?}",
            alloc_free_time.div_f64(NODES_COUNT as f64)
        );
        let start = Instant::now();
        let nodes_re = (0..NODES_COUNT)
            .map(|_| allocator.alloc::<[u8; 512]>())
            .collect::<Vec<_>>();
        let alloc_time = start.elapsed();
        println!(
            "alloc old time per node: {:?}",
            alloc_time.div_f64(NODES_COUNT as f64)
        );
        for (node, node_re) in nodes.iter().zip(nodes_re.iter().rev()) {
            assert_eq!(node, node_re);
        }
    }
}
