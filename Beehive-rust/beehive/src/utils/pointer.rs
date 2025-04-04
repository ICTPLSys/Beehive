use std::fmt::Debug;
use std::ptr::NonNull;

#[repr(transparent)]
pub struct SendNonNull<T> {
    inner: NonNull<T>,
}

impl<T> Debug for SendNonNull<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Clone for SendNonNull<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for SendNonNull<T> {}

impl<T> PartialEq for SendNonNull<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

unsafe impl<T> Send for SendNonNull<T> {}
impl<T> SendNonNull<T> {
    pub fn new(ptr: *mut T) -> Option<Self> {
        NonNull::new(ptr).map(|inner| Self { inner })
    }

    pub unsafe fn new_unchecked(ptr: *mut T) -> Self {
        Self {
            inner: unsafe { NonNull::new_unchecked(ptr) },
        }
    }

    pub fn as_ptr(self) -> *mut T {
        self.inner.as_ptr()
    }

    pub unsafe fn as_ref<'a>(&self) -> &'a T {
        unsafe { self.inner.as_ref() }
    }

    pub unsafe fn as_mut<'a>(&mut self) -> &'a mut T {
        unsafe { self.inner.as_mut() }
    }

    pub fn cast<U>(self) -> SendNonNull<U> {
        SendNonNull {
            inner: self.inner.cast(),
        }
    }

    pub unsafe fn add(self, count: usize) -> Self {
        Self {
            inner: unsafe { self.inner.add(count) },
        }
    }
}
