use crate::mem::DerefScopeTrait;
use crate::{check_memory_low, pararoutine::yield_now};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub struct SpinLock {
    lock: AtomicBool,
}

impl SpinLock {
    pub fn new() -> Self {
        SpinLock {
            lock: AtomicBool::new(false),
        }
    }

    #[inline]
    fn lock_impl<const YIELD: bool>(&mut self, scope: Option<&dyn DerefScopeTrait>) {
        while self
            .lock
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            if cfg!(test) {
                if YIELD {
                    std::thread::yield_now();
                }
            } else {
                if let Some(scope) = scope {
                    check_memory_low(scope);
                }
                if YIELD {
                    libfibre_port::yield_now();
                }
            }
        }
    }

    pub fn lock(&mut self) {
        self.lock_impl::<true>(None);
    }

    pub fn try_lock(&mut self) -> bool {
        self.lock
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    pub fn lock_with_scope(&mut self, scope: &dyn DerefScopeTrait) {
        self.lock_impl::<true>(Some(scope));
    }

    pub fn lock_polling(&mut self) {
        self.lock_impl::<false>(None);
    }

    pub fn unlock(&mut self) {
        debug_assert!(self.is_locked());
        assert!(
            self.lock
                .compare_exchange(true, false, Ordering::Release, Ordering::Relaxed)
                .is_ok()
        );
    }

    pub fn is_locked(&self) -> bool {
        self.lock.load(Ordering::Relaxed)
    }

    pub async fn lock_async(&mut self) {
        self.lock_async_impl(None).await;
    }

    pub async fn lock_async_with_scope(&mut self, scope: &dyn DerefScopeTrait) {
        self.lock_async_impl(Some(scope)).await;
    }

    #[inline]
    pub async fn lock_async_impl(&mut self, scope: Option<&dyn DerefScopeTrait>) {
        while self
            .lock
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            if let Some(scope) = scope {
                check_memory_low(scope);
            }
            yield_now().await;
        }
    }
}

#[derive(Debug)]
pub struct SpinPollingLock {
    spin: SpinLock,
}

impl SpinPollingLock {
    pub fn new() -> Self {
        Self {
            spin: SpinLock::new(),
        }
    }

    pub fn lock(&mut self) {
        self.spin.lock_polling();
    }

    pub fn unlock(&mut self) {
        self.spin.unlock();
    }

    pub fn is_locked(&self) -> bool {
        self.spin.is_locked()
    }

    pub fn try_lock(&mut self) -> bool {
        self.spin.try_lock()
    }
}
