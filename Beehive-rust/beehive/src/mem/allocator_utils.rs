use std::sync::atomic::{AtomicBool, Ordering};

pub(super) const REGION_SIZE: usize = 256 * 1024;
pub(super) const REGION_BIN_COUNT: usize = 48;
const BIN_SIZE: [usize; REGION_BIN_COUNT] = [
    1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160,
    192, 224, 256, 320, 384, 448, 512, 640, 768, 896, 1024, 1280, 1536, 1792, 2048, 2560, 3072,
    3584, 4096, 5120, 6144, 7168, 8192,
];
const MAX_BIN_SIZE: usize = BIN_SIZE[REGION_BIN_COUNT - 1];

#[inline]
pub(super) const fn bin_size(bin: usize) -> usize {
    BIN_SIZE[bin] * size_of::<usize>()
}

/// aligned to size_of::<usize>
#[inline]
pub(super) fn wsize_from_size(size: usize) -> usize {
    (size + size_of::<usize>() - 1) / size_of::<usize>()
}

#[inline]
fn bsr32(value: u32) -> u32 {
    31 - value.leading_zeros()
}

pub(super) fn bin_from_wsize(wsize: usize) -> usize {
    if wsize <= 8 {
        if wsize <= 1 {
            0
        } else {
            // round to double word size
            (wsize - 1) | 1
        }
    } else {
        assert!(wsize <= MAX_BIN_SIZE, "object too large to allocate");
        let wsize = wsize - 1;
        let b = bsr32(wsize as u32);
        // (~16% worst internal fragmentation)
        let bin = ((b << 2) + ((wsize as u32 >> (b - 2)) & 0x03)) - 4;
        bin as usize
    }
}

pub(super) enum RegionState {
    Free,
    InUse,
    Usable,
    Full,
}
#[derive(Debug)]
pub(super) struct SpinLock {
    lock: AtomicBool,
}

impl SpinLock {
    pub fn new() -> Self {
        SpinLock {
            lock: AtomicBool::new(false),
        }
    }

    pub fn lock(&mut self) {
        loop {
            if self
                .lock
                .compare_exchange_weak(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn unlock(&mut self) {
        debug_assert!(self.is_locked());
        self.lock.store(false, Ordering::Relaxed);
    }

    pub fn is_locked(&self) -> bool {
        self.lock.load(Ordering::Relaxed)
    }
}
