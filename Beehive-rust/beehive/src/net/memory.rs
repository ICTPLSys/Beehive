use nix::sys::mman::{MapFlags, ProtFlags, mmap_anonymous, munmap};
use std::{ffi::c_void, num::NonZero, ptr::NonNull};

fn mmap_length(size: usize) -> usize {
    const PAGE_SIZE: usize = 1 << 21;
    const REGION_SIZE: usize = 256 * 1024; // TODO
    let alignment = std::cmp::max(REGION_SIZE, PAGE_SIZE);
    if size % alignment == 0 {
        size
    } else {
        (size / alignment + 1) * alignment
    }
}

pub(super) unsafe fn allocate_memory(size: usize) -> *mut c_void {
    let prot = ProtFlags::PROT_READ | ProtFlags::PROT_WRITE;
    let flags =
        MapFlags::MAP_PRIVATE | MapFlags::MAP_ANON | MapFlags::MAP_HUGETLB | MapFlags::MAP_HUGE_2MB;
    let mem = unsafe {
        mmap_anonymous(
            Option::None,
            NonZero::new(mmap_length(size)).unwrap(),
            prot,
            flags,
        )
    };
    mem.unwrap().as_ptr()
}

pub(super) unsafe fn deallocate_memory(ptr: *mut c_void, size: usize) {
    unsafe { munmap(NonNull::new(ptr).unwrap(), mmap_length(size)).unwrap() };
}
