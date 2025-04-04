use crate::MutatorState;
use crate::mem::manager::CheckFetch;
use crate::mem::{DerefScopeTrait, RemPtr, local_allocator::BlockHead, manager};
use crate::mem::{RemRef, RemRefMut, RootScope, pin_remref, unpin_remref};
use crate::rem_data_struct::rem_iter::*;
use std::mem::size_of;
use sync_ptr::{SendMutPtr, SyncMutPtr};

use crate::thread::{fork_join_task, fork_join_task_with_scope, fork_join_with_id_with_scope};
const DEFAULT_GROUP_BYTES: usize = 4096 - size_of::<BlockHead>();

pub const fn default_group_size<T>() -> usize {
    DEFAULT_GROUP_BYTES / size_of::<T>()
}
pub trait RemVecIterOnMiss<T, const GROUP_SIZE: usize> =
    FnMut(&RemVec<T, GROUP_SIZE>, usize, usize, usize, &mut dyn CheckFetch, &dyn DerefScopeTrait);
pub struct RemVec<T, const GROUP_SIZE: usize = { default_group_size::<T>() }> {
    groups: Vec<RemPtr<[T; GROUP_SIZE]>>,
    size: usize,
}

impl<T, const GROUP_SIZE: usize> RemVec<T, GROUP_SIZE> {
    pub fn with_size<const MULTITHREAD: bool>(size: usize) -> Self {
        let mut new_vec = Self::with_capacity(size);
        new_vec.resize::<MULTITHREAD>(size, None);
        new_vec
    }

    pub const fn group_size(&self) -> usize {
        GROUP_SIZE
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            groups: Vec::with_capacity((capacity + GROUP_SIZE - 1) / GROUP_SIZE),
            size: 0,
        }
    }

    pub fn resize<const MULTITHREAD: bool>(
        &mut self,
        size: usize,
        scope: Option<&dyn DerefScopeTrait>,
    ) {
        debug_assert!(self.capacity() >= self.size());
        self.reserve(size);
        if size <= self.size() {
            self.resize_shrink(size);
        } else {
            self.resize_enlarge::<MULTITHREAD>(size, scope);
        }
    }

    fn resize_shrink(&mut self, size: usize) {
        let group_count = (size + GROUP_SIZE - 1) / GROUP_SIZE;
        debug_assert!(group_count <= self.groups_count());
        self.groups.resize_with(group_count, || RemPtr::null());
        self.size = size;
    }

    fn resize_enlarge<const MULTITHREAD: bool>(
        &mut self,
        size: usize,
        scope: Option<&dyn DerefScopeTrait>,
    ) {
        let allocate_group_count = (size + GROUP_SIZE - 1) / GROUP_SIZE - self.groups_count();
        if MULTITHREAD {
            debug_assert!(scope.is_none());
            let start_group_idx = self.groups_count();
            self.groups
                .resize_with(start_group_idx + allocate_group_count, || RemPtr::null());
            let self_ptr = unsafe { SendMutPtr::new(self) };
            fork_join_task_with_scope(
                libfibre_port::cfibre_get_worker_count(),
                allocate_group_count,
                move |task_id, scope| {
                    let self_ref = unsafe { self_ptr.as_mut_unchecked() };
                    manager::allocate(&mut self_ref.groups[start_group_idx + task_id], None, scope);
                },
            );
        } else {
            let mut resize_allocate = |scope: &dyn DerefScopeTrait| {
                for _ in 0..allocate_group_count {
                    self.groups.push(RemPtr::null());
                    manager::allocate(self.groups.last_mut().unwrap(), None, scope);
                }
            };
            if let Some(scope) = scope {
                resize_allocate(scope);
            } else {
                let scope = RootScope::root();
                resize_allocate(&scope);
            }
        }
        self.size = size;
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn capacity(&self) -> usize {
        self.groups_count() * GROUP_SIZE
    }

    pub fn groups_count(&self) -> usize {
        self.groups.len()
    }

    pub fn push(&mut self, val: T, scope: &dyn DerefScopeTrait) {
        debug_assert!(self.capacity() >= self.size());
        let inner_idx = self.size() % GROUP_SIZE;
        if inner_idx == 0 {
            self.groups.push(RemPtr::null());
            let mut rem_mut_ref = manager::allocate(self.groups.last_mut().unwrap(), None, scope);
            (*rem_mut_ref)[0] = val;
        } else {
            let mut rem_mut_ref = manager::deref_mut_sync(self.groups.last_mut().unwrap(), scope);
            (*rem_mut_ref)[inner_idx] = val;
        }
        self.size += 1;
    }

    pub fn reserve(&mut self, count: usize) {
        self.groups.reserve((count + GROUP_SIZE - 1) / GROUP_SIZE);
    }

    pub fn clear<const MULTITHREAD: bool>(&mut self) {
        if MULTITHREAD {
            let self_ptr = unsafe { SendMutPtr::new(self) };
            fork_join_task(
                libfibre_port::cfibre_get_worker_count(),
                self.groups_count(),
                move |idx| {
                    let self_ref = unsafe { self_ptr.as_mut_unchecked() };
                    manager::deallocate(&mut self_ref.groups[idx]);
                },
            );
            self.groups.clear();
        } else {
            self.resize_shrink(0);
        }
    }

    pub fn iter_sync(&self) -> RemVecIter<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.iter_with_setting(0, self.size(), 1, |_, _, _, _, _, _| {})
    }

    pub fn iter_mut_sync(
        &mut self,
    ) -> RemVecIterMut<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.iter_mut_with_setting(0, self.size(), 1, |_, _, _, _, _, _| {})
    }

    pub fn group_iter_sync(
        &self,
    ) -> RemVecGroupIter<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.group_iter_with_setting(0, self.groups_count(), |_, _, _, _, _, _| {})
    }

    pub fn group_iter_mut_sync(
        &mut self,
    ) -> RemVecGroupIterMut<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.group_iter_mut_with_setting(0, self.groups_count(), |_, _, _, _, _, _| {})
    }

    pub fn iter_sync_with_setting(
        &self,
        start: usize,
        end: usize,
        step: usize,
    ) -> RemVecIter<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.iter_with_setting(start, end, step, |_, _, _, _, _, _| {})
    }

    pub fn iter_mut_sync_with_setting(
        &mut self,
        start: usize,
        end: usize,
        step: usize,
    ) -> RemVecIterMut<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.iter_mut_with_setting(start, end, step, |_, _, _, _, _, _| {})
    }

    pub fn group_iter_sync_with_setting(
        &self,
        start: usize,
        end: usize,
    ) -> RemVecGroupIter<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.group_iter_with_setting(start, end, |_, _, _, _, _, _| {})
    }

    pub fn group_iter_mut_sync_with_setting(
        &mut self,
        start: usize,
        end: usize,
    ) -> RemVecGroupIterMut<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.group_iter_mut_with_setting(start, end, |_, _, _, _, _, _| {})
    }

    pub fn iter_with_setting<F: RemVecIterOnMiss<T, GROUP_SIZE>>(
        &self,
        start: usize,
        end: usize,
        step: usize,
        on_miss: F,
    ) -> RemVecIter<T, GROUP_SIZE, F> {
        RemVecIter::new(self, start, end, step, on_miss)
    }

    pub fn iter_mut_with_setting<F: RemVecIterOnMiss<T, GROUP_SIZE>>(
        &mut self,
        start: usize,
        end: usize,
        step: usize,
        on_miss: F,
    ) -> RemVecIterMut<T, GROUP_SIZE, F> {
        RemVecIterMut::new(self, start, end, step, on_miss)
    }

    pub fn group_iter_with_setting<F: RemVecIterOnMiss<T, GROUP_SIZE>>(
        &self,
        start: usize,
        end: usize,
        on_miss: F,
    ) -> RemVecGroupIter<T, GROUP_SIZE, F> {
        RemVecGroupIter::new(self, start, end, on_miss)
    }

    pub fn group_iter_mut_with_setting<F: RemVecIterOnMiss<T, GROUP_SIZE>>(
        &mut self,
        start: usize,
        end: usize,
        on_miss: F,
    ) -> RemVecGroupIterMut<T, GROUP_SIZE, F> {
        RemVecGroupIterMut::new(self, start, end, on_miss)
    }

    pub fn prefetch(&self, idx: usize, scope: &dyn DerefScopeTrait) {
        self.prefetch_group(idx / GROUP_SIZE, scope);
    }

    pub fn prefetch_mut(&self, idx: usize, scope: &dyn DerefScopeTrait) {
        self.prefetch_group_mut(idx / GROUP_SIZE, scope);
    }

    pub fn prefetch_group(&self, group_idx: usize, scope: &dyn DerefScopeTrait) {
        manager::prefetch(&self.groups[group_idx], scope);
    }

    pub fn prefetch_group_mut(&self, group_idx: usize, scope: &dyn DerefScopeTrait) {
        manager::prefetch_mut(&self.groups[group_idx], scope);
    }

    fn iter_prefetch_fn(
        vec: &Self,
        _start: usize,
        end: usize,
        group_idx: usize,
        check_fetch: &mut dyn CheckFetch,
        scope: &dyn DerefScopeTrait,
    ) {
        for idx in group_idx..end {
            vec.prefetch(idx, scope);
            if check_fetch() {
                return;
            }
        }
    }

    fn iter_prefetch_mut_fn(
        vec: &Self,
        _start: usize,
        end: usize,
        group_idx: usize,
        check_fetch: &mut dyn CheckFetch,
        scope: &dyn DerefScopeTrait,
    ) {
        for idx in group_idx..end {
            vec.prefetch_mut(idx, scope);
            if check_fetch() {
                return;
            }
        }
    }

    pub fn iter_prefetch_with_setting(
        &self,
        start: usize,
        end: usize,
        step: usize,
    ) -> RemVecIter<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.iter_with_setting(
            start,
            end,
            step,
            |vec, start, end, group_idx, check_fetch, scope| {
                Self::iter_prefetch_fn(vec, start, end, group_idx, check_fetch, scope)
            },
        )
    }

    pub fn iter_mut_prefetch_with_setting(
        &mut self,
        start: usize,
        end: usize,
        step: usize,
    ) -> RemVecIterMut<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.iter_mut_with_setting(
            start,
            end,
            step,
            |vec, start, end, group_idx, check_fetch, scope| {
                Self::iter_prefetch_mut_fn(vec, start, end, group_idx, check_fetch, scope)
            },
        )
    }

    pub fn group_iter_prefetch_with_setting(
        &self,
        start: usize,
        end: usize,
    ) -> RemVecGroupIter<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.group_iter_with_setting(
            start,
            end,
            |vec, start, end, group_idx, check_fetch, scope| {
                Self::iter_prefetch_fn(vec, start, end, group_idx, check_fetch, scope)
            },
        )
    }

    pub fn group_iter_mut_prefetch_with_setting(
        &mut self,
        start: usize,
        end: usize,
    ) -> RemVecGroupIterMut<T, GROUP_SIZE, impl RemVecIterOnMiss<T, GROUP_SIZE>> {
        self.group_iter_mut_with_setting(
            start,
            end,
            |vec, start, end, group_idx, check_fetch, scope| {
                Self::iter_prefetch_mut_fn(vec, start, end, group_idx, check_fetch, scope)
            },
        )
    }
}

impl<T, const GROUP_SIZE: usize> RemVec<T, GROUP_SIZE>
where
    T: Sync + Copy,
{
    pub fn copy_to_local(&self, local_buf: &mut [T], start: usize, size: usize) {
        assert!(start + size <= self.size());
        assert_eq!(
            libfibre_port::get_scope_state(),
            MutatorState::OutOfScope as i32
        );
        let thread_count = libfibre_port::cfibre_get_worker_count();
        let block = (size + thread_count - 1) / thread_count;
        let local_buf_ptr = unsafe { SyncMutPtr::new(local_buf.as_mut_ptr()) };
        fork_join_with_id_with_scope(thread_count, |id, scope| {
            let local_start_idx = id * block;
            let local_end_idx = (local_start_idx + block).min(size);
            let start_idx = start + local_start_idx;
            let end_idx = start_idx + (local_end_idx - local_start_idx);
            for (i, (_, elem)) in RemIterator::new(
                self.iter_prefetch_with_setting(start_idx, end_idx, 1),
                scope,
            )
            .enumerate()
            {
                unsafe { *local_buf_ptr.add(local_start_idx + i) = *elem };
            }
        });
    }
}

pub struct RemVecGroupIter<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>> {
    vec: &'a RemVec<T, GROUP_SIZE>,
    group_idx: usize,
    start: usize,
    end: usize,
    rem_ref: Option<RemRef<'a, [T; GROUP_SIZE]>>,
    on_miss: F,
}

impl<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>>
    RemVecGroupIter<'a, T, GROUP_SIZE, F>
{
    pub fn new(vec: &'a RemVec<T, GROUP_SIZE>, start: usize, end: usize, on_miss: F) -> Self {
        Self {
            vec,
            group_idx: start,
            start,
            end,
            rem_ref: None,
            on_miss,
        }
    }
}

impl<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>> RemIteratorTrait<'a>
    for RemVecGroupIter<'a, T, GROUP_SIZE, F>
{
    type Item = RemRef<'a, [T; GROUP_SIZE]>;
    fn next(&mut self, scope: &dyn DerefScopeTrait) -> Option<Self::Item> {
        self.rem_ref = None;
        if self.group_idx >= self.end {
            None
        } else {
            let group_idx = self.group_idx;
            self.group_idx += 1;
            let on_miss = &mut self.on_miss as *mut F;
            self.rem_ref = Some(unsafe {
                std::mem::transmute(manager::deref(
                    &self.vec.groups[group_idx],
                    &|check| {
                        (*on_miss)(&self.vec, group_idx, self.start, self.end, check, scope);
                    },
                    scope,
                ))
            });
            Some(unsafe { std::mem::transmute_copy(&self.rem_ref) })
        }
    }

    fn pin(&self, scope: &dyn DerefScopeTrait) {
        if let Some(rem_ref) = &self.rem_ref {
            pin_remref(rem_ref, scope);
        }
    }

    fn unpin(&self, scope: &dyn DerefScopeTrait) {
        if let Some(rem_ref) = &self.rem_ref {
            unpin_remref(rem_ref, scope);
        }
    }
}

pub struct RemVecIter<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>> {
    group_iter: RemVecGroupIter<'a, T, GROUP_SIZE, F>,
    start: usize,
    end: usize,
    step: usize,
    idx: usize,
}

impl<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>>
    RemVecIter<'a, T, GROUP_SIZE, F>
{
    pub fn new(
        vec: &'a RemVec<T, GROUP_SIZE>,
        start: usize,
        end: usize,
        step: usize,
        on_miss: F,
    ) -> Self {
        assert!(start <= end);
        assert!(step <= GROUP_SIZE);
        Self {
            group_iter: RemVecGroupIter::new(
                vec,
                start / GROUP_SIZE,
                (end + GROUP_SIZE - 1) / GROUP_SIZE,
                on_miss,
            ),
            start,
            end,
            step,
            idx: start,
        }
    }
}

impl<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>> RemIteratorTrait<'a>
    for RemVecIter<'a, T, GROUP_SIZE, F>
{
    type Item = RemRef<'a, T>;
    fn next(&mut self, scope: &dyn DerefScopeTrait) -> Option<Self::Item> {
        if self.idx >= self.end {
            return None;
        }
        let (group_idx, inner_idx) = (self.idx / GROUP_SIZE, self.idx % GROUP_SIZE);
        if group_idx == self.group_iter.group_idx {
            self.group_iter.next(scope);
        }
        self.idx += self.step;
        self.group_iter
            .rem_ref
            .as_ref()
            .map(|rem_ref| RemRef::new(unsafe { std::mem::transmute(&(*rem_ref)[inner_idx]) }))
    }

    fn pin(&self, scope: &dyn DerefScopeTrait) {
        self.group_iter.pin(scope);
    }

    fn unpin(&self, scope: &dyn DerefScopeTrait) {
        self.group_iter.unpin(scope);
    }
}

pub struct RemVecGroupIterMut<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>> {
    vec: &'a mut RemVec<T, GROUP_SIZE>,
    group_idx: usize,
    start: usize,
    end: usize,
    rem_ref: Option<RemRefMut<'a, [T; GROUP_SIZE]>>,
    on_miss: F,
}

impl<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>>
    RemVecGroupIterMut<'a, T, GROUP_SIZE, F>
{
    pub fn new(vec: &'a mut RemVec<T, GROUP_SIZE>, start: usize, end: usize, on_miss: F) -> Self {
        Self {
            vec,
            group_idx: start,
            start,
            end,
            rem_ref: None,
            on_miss,
        }
    }
}

impl<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>> RemIteratorTrait<'a>
    for RemVecGroupIterMut<'a, T, GROUP_SIZE, F>
{
    type Item = RemRefMut<'a, [T; GROUP_SIZE]>;
    fn next(&mut self, scope: &dyn DerefScopeTrait) -> Option<Self::Item> {
        self.rem_ref = None;
        if self.group_idx >= self.end {
            None
        } else {
            let group_idx = self.group_idx;
            self.group_idx += 1;
            let self_ptr = self.vec as *const RemVec<T, GROUP_SIZE>;
            let on_miss = &mut self.on_miss as *mut F;
            self.rem_ref = Some(unsafe {
                std::mem::transmute(manager::deref_mut(
                    &mut self.vec.groups[group_idx],
                    &|check: &mut dyn CheckFetch| {
                        (*on_miss)(
                            self_ptr.as_ref().unwrap(),
                            group_idx,
                            self.start,
                            self.end,
                            check,
                            scope,
                        );
                    },
                    scope,
                ))
            });
            Some(unsafe { std::mem::transmute_copy(&self.rem_ref) })
        }
    }

    fn pin(&self, scope: &dyn DerefScopeTrait) {
        if let Some(rem_ref) = &self.rem_ref {
            pin_remref(rem_ref, scope);
        }
    }

    fn unpin(&self, scope: &dyn DerefScopeTrait) {
        if let Some(rem_ref) = &self.rem_ref {
            unpin_remref(rem_ref, scope);
        }
    }
}

pub struct RemVecIterMut<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>> {
    group_iter: RemVecGroupIterMut<'a, T, GROUP_SIZE, F>,
    start: usize,
    end: usize,
    step: usize,
    idx: usize,
}

impl<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>>
    RemVecIterMut<'a, T, GROUP_SIZE, F>
{
    pub fn new(
        vec: &'a mut RemVec<T, GROUP_SIZE>,
        start: usize,
        end: usize,
        step: usize,
        on_miss: F,
    ) -> Self {
        assert!(start <= end);
        assert!(step <= GROUP_SIZE);
        Self {
            group_iter: RemVecGroupIterMut::new(
                vec,
                start / GROUP_SIZE,
                (end + GROUP_SIZE - 1) / GROUP_SIZE,
                on_miss,
            ),
            start,
            end,
            step,
            idx: start,
        }
    }
}

impl<'a, T, const GROUP_SIZE: usize, F: RemVecIterOnMiss<T, GROUP_SIZE>> RemIteratorTrait<'a>
    for RemVecIterMut<'a, T, GROUP_SIZE, F>
{
    type Item = RemRefMut<'a, T>;
    fn next(&mut self, scope: &dyn DerefScopeTrait) -> Option<Self::Item> {
        if self.idx >= self.end {
            return None;
        }
        let (group_idx, inner_idx) = (self.idx / GROUP_SIZE, self.idx % GROUP_SIZE);
        if group_idx == self.group_iter.group_idx {
            self.group_iter.next(scope);
        }
        self.idx += self.step;
        self.group_iter.rem_ref.as_mut().map(|rem_ref| {
            RemRefMut::new(unsafe { std::mem::transmute(&mut (*rem_ref)[inner_idx]) })
        })
    }

    fn pin(&self, scope: &dyn DerefScopeTrait) {
        self.group_iter.pin(scope);
    }

    fn unpin(&self, scope: &dyn DerefScopeTrait) {
        self.group_iter.unpin(scope);
    }
}
