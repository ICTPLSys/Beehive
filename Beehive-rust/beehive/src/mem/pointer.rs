use super::entry::Entry;
use super::manager::deallocate_object;
use std::marker::{PhantomData, PhantomPinned};
use std::ops::{Deref, DerefMut};

/// remote pointer. should not be copied.
pub struct RemPtr<T> {
    pub(super) entry: Entry,
    _marker: PhantomData<T>,
    _pin: PhantomPinned,
}

/// remoteable reference
pub struct RemRef<'a, T> {
    reference: &'a T,
}

pub struct RemRefMut<'a, T> {
    reference: &'a mut T,
}

impl<T> RemPtr<T> {
    pub fn null() -> Self {
        Self {
            entry: Entry::null(),
            _marker: PhantomData,
            _pin: PhantomPinned,
        }
    }

    pub fn is_null(&self) -> bool {
        self.entry.is_null()
    }
}

impl<T> Drop for RemPtr<T> {
    fn drop(&mut self) {
        deallocate_object(&mut self.entry);
    }
}

impl<'a, T> RemRef<'a, T> {
    pub fn new(reference: &'a T) -> Self {
        Self { reference }
    }
}

impl<'a, T> Deref for RemRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.reference
    }
}

impl<'a, T> RemRefMut<'a, T> {
    pub fn new(reference: &'a mut T) -> Self {
        Self { reference }
    }
}

impl<'a, T> Deref for RemRefMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.reference
    }
}

impl<'a, T> DerefMut for RemRefMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.reference
    }
}
