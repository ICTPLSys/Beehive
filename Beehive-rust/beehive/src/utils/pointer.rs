use std::fmt::Debug;

#[derive(Debug)]
pub struct ConstNonNull<T: ?Sized> {
    inner: *const T,
}

impl<T: ?Sized> ConstNonNull<T> {
    pub fn new(inner: *const T) -> Option<Self> {
        if !inner.is_null() {
            unsafe { Some(Self::new_unchecked(inner)) }
        } else {
            None
        }
    }

    pub unsafe fn new_unchecked(inner: *const T) -> Self {
        Self { inner }
    }

    pub fn as_ptr(&self) -> *const T {
        self.inner
    }

    pub unsafe fn as_ref(&self) -> &T {
        unsafe { self.inner.as_ref_unchecked() }
    }
}

impl<T: ?Sized> Clone for ConstNonNull<T> {
    fn clone(&self) -> Self {
        unsafe { Self::new_unchecked(self.inner) }
    }
}

impl<T: ?Sized> Copy for ConstNonNull<T> {}
