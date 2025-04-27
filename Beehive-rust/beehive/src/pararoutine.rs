mod task_allocator;

use crate::mem::SingleDerefScope;
use crate::mem::entry::Entry;
use crate::mem::manager::check_cq;
use crate::mem::{DerefScopeBaseTrait, DerefScopeTrait};
use crate::pararoutine::task_allocator::TaskAllocator;
use crate::utils::pointer::ConstNonNull;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

#[thread_local]
static mut MISS_ENTRY: Option<ConstNonNull<Entry>> = None;
#[thread_local]
static mut TASK_SCOPE: Option<ConstNonNull<dyn DerefScopeTrait>> = None;

trait Schedulable {
    fn check_fetch(&self) -> bool;
    fn pin(&self);
    fn unpin(&self);
    fn set_miss_entry(&mut self, entry: Option<ConstNonNull<Entry>>);
    fn set_task_scope(&mut self, scope: Option<ConstNonNull<dyn DerefScopeTrait>>);
}

trait ExecutableTask: Future<Output = ()> + Schedulable {}

#[repr(C)]
pub struct Task<'a, F>
where
    F: Future<Output = ()> + 'a,
{
    miss_entry: Option<ConstNonNull<Entry>>,
    future: F,
    scope: Option<ConstNonNull<dyn DerefScopeTrait>>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, F> Task<'a, F>
where
    F: Future<Output = ()> + 'a,
{
    fn new(task: *mut Self, future: F) -> Pin<Box<dyn ExecutableTask + 'a>>
    where
        F: Future<Output = ()> + 'a,
    {
        unsafe {
            *task = Self {
                miss_entry: None,
                future,
                scope: None,
                _phantom: PhantomData,
            };
        }
        unsafe { Pin::new_unchecked(Box::from_raw(task)) }
    }
}

impl<'a, F> Future for Task<'a, F>
where
    F: Future<Output = ()> + 'a,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { Pin::new_unchecked(&mut self.get_unchecked_mut().future).poll(cx) }
    }
}

impl<'a, F> Schedulable for Task<'a, F>
where
    F: Future<Output = ()> + 'a,
{
    fn check_fetch(&self) -> bool {
        check_cq();
        match self.miss_entry {
            Some(miss_entry) => unsafe {
                let miss_entry = miss_entry.as_ref();
                let is_local = miss_entry.is_local();
                // let is_fetching = miss_entry.is_fetching();
                // let is_remote = miss_entry.is_remote();
                // let is_evicting = miss_entry.is_evicting();
                // let is_null = miss_entry.is_null();
                // assert!(is_local || is_fetching);
                is_local
            },
            None => true,
        }
    }

    fn pin(&self) {
        self.miss_entry.map(|entry| unsafe {
            let entry = entry.as_ref();
            while !entry.is_local() {
                check_cq();
            }
            entry.pin();
        });
        self.scope.map(|scope| {
            unsafe { scope.as_ref().scope_pin() };
        });
    }

    fn unpin(&self) {
        self.miss_entry.map(|entry| unsafe {
            let entry = entry.as_ref();
            entry.unpin();
        });
        self.scope.map(|scope| {
            unsafe { scope.as_ref().scope_unpin() };
        });
    }

    fn set_miss_entry(&mut self, entry: Option<ConstNonNull<Entry>>) {
        self.miss_entry = entry;
    }

    fn set_task_scope(&mut self, scope: Option<ConstNonNull<dyn DerefScopeTrait>>) {
        self.scope = scope;
    }
}

impl<'a, F> ExecutableTask for Task<'a, F> where F: Future<Output = ()> {}

const DEFAULT_TASK_CAPACITY: usize = 16;
pub struct Executor<'a, const TASK_CAPACITY: usize = DEFAULT_TASK_CAPACITY> {
    tasks: VecDeque<Pin<Box<dyn ExecutableTask + 'a>>>,
    task_allocator: TaskAllocator,
    parent: &'a dyn DerefScopeTrait,
    outer_scope: Option<ConstNonNull<dyn DerefScopeTrait>>,
    pinning: AtomicBool,
}

impl<'a, const TASK_CAPACITY: usize> Executor<'a, TASK_CAPACITY> {
    pub fn new(parent: &'a dyn DerefScopeTrait) -> Self {
        Self {
            tasks: VecDeque::new(),
            task_allocator: TaskAllocator::new(),
            parent,
            outer_scope: None,
            pinning: AtomicBool::new(false),
        }
    }

    pub fn spawn_with_scope<F>(&mut self, future: F, scope: &dyn DerefScopeTrait)
    where
        F: Future<Output = ()> + 'a,
    {
        let node = self.task_allocator.alloc::<Task<F>>();
        let task = Task::new(node, future);
        if self.tasks.len() >= TASK_CAPACITY {
            self.poll_with_scope(scope);
        }
        self.run_one_with_scope(task, scope);
    }

    pub fn spawn<F, Fut>(&mut self, closure: F)
    where
        F: FnOnce(&'a dyn DerefScopeTrait) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        let node = self.task_allocator.alloc::<Task<Fut>>();
        let self_ptr = self as *const Self;
        let task = Task::new(node, closure(unsafe { self_ptr.as_ref_unchecked() }));
        if self.tasks.len() >= TASK_CAPACITY {
            self.poll();
        }
        self.run_one(task);
    }

    fn run_one_with_scope(
        &mut self,
        task: Pin<Box<dyn ExecutableTask + 'a>>,
        scope: &dyn DerefScopeTrait,
    ) {
        unsafe {
            self.outer_scope = Some(ConstNonNull::new_unchecked(std::mem::transmute(scope)));
        }
        self.run_one(task);
        self.outer_scope = None;
    }

    fn run_one(&mut self, mut task: Pin<Box<dyn ExecutableTask + 'a>>) {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(&waker);
        {
            let task = unsafe { Pin::get_unchecked_mut(Pin::as_mut(&mut task)) };
            task.set_miss_entry(None);
            task.set_task_scope(None);
        }
        match task.as_mut().poll(&mut cx) {
            Poll::Ready(_) => unsafe {
                let node = Box::into_raw(Pin::into_inner_unchecked(task));
                self.task_allocator.free(node);
            },
            Poll::Pending => {
                {
                    let task = unsafe { Pin::get_unchecked_mut(Pin::as_mut(&mut task)) };
                    task.set_miss_entry(unsafe { MISS_ENTRY });
                    task.set_task_scope(unsafe { TASK_SCOPE });
                }
                self.tasks.push_back(task);
            }
        }
        unsafe {
            MISS_ENTRY = None;
            TASK_SCOPE = None;
        }
    }

    pub fn poll(&mut self) {
        while let Some(task) = self.tasks.front() {
            if task.check_fetch() {
                let task = self.tasks.pop_front().unwrap();
                self.run_one(task);
            }
        }
    }

    pub fn poll_with_scope(&mut self, scope: &dyn DerefScopeTrait) {
        unsafe {
            self.outer_scope = Some(ConstNonNull::new_unchecked(std::mem::transmute(scope)));
        }
        self.poll();
        self.outer_scope = None;
    }
}

impl<'a, const TASK_CAPACITY: usize> Drop for Executor<'a, TASK_CAPACITY> {
    fn drop(&mut self) {
        // poll all tasks
        self.poll();
    }
}

impl<'a, const TASK_CAPACITY: usize> DerefScopeBaseTrait for Executor<'a, TASK_CAPACITY> {
    fn parent(&self) -> Option<&dyn DerefScopeTrait> {
        Some(self.parent)
    }

    fn pin(&self) {
        self.tasks.iter().for_each(|task| task.pin());
    }

    fn unpin(&self) {
        self.tasks.iter().for_each(|task| task.unpin());
    }

    fn push(&self, remref: &dyn crate::mem::RemRefTrait) -> crate::mem::SingleDerefScope {
        SingleDerefScope::new(Some(self), remref)
    }
}

impl<'a, const TASK_CAPACITY: usize> DerefScopeTrait for Executor<'a, TASK_CAPACITY> {
    fn scope_pin(&self) {
        if !self.pinning.load(std::sync::atomic::Ordering::Relaxed) {
            let self_ptr: *const Self = self;
            let self_mut_ptr = self_ptr as *mut Self;
            let self_mut = unsafe { self_mut_ptr.as_mut_unchecked() };
            self_mut.pinning.store(true, Ordering::Relaxed);
            self.tasks.iter().for_each(|task| {
                task.pin();
            });
            self.outer_scope.map(|outer_scope| {
                unsafe { outer_scope.as_ref().scope_pin() };
            });
            self_mut.pinning.store(false, Ordering::Relaxed);
        }
        if let Some(parent) = self.parent() {
            parent.scope_pin();
        }
    }

    fn scope_unpin(&self) {
        if !self.pinning.load(Ordering::Relaxed) {
            let self_ptr: *const Self = self;
            let self_mut_ptr = self_ptr as *mut Self;
            let self_mut = unsafe { self_mut_ptr.as_mut_unchecked() };
            self_mut.pinning.store(true, Ordering::Relaxed);
            self.tasks.iter().for_each(|task| {
                task.unpin();
            });
            self.outer_scope.map(|outer_scope| {
                unsafe { outer_scope.as_ref().scope_unpin() };
            });
            self_mut.pinning.store(false, Ordering::Relaxed);
        }
        if let Some(parent) = self.parent() {
            parent.scope_unpin();
        }
    }
}

impl<'a> Executor<'a, DEFAULT_TASK_CAPACITY> {
    pub fn new_default(parent: &'a dyn DerefScopeTrait) -> Self {
        Self::new(parent)
    }
}

struct YieldNow {
    yielded: bool,
}

impl YieldNow {
    fn new() -> Self {
        Self { yielded: false }
    }
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.yielded {
            self.yielded = true;
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

pub async fn yield_now() {
    YieldNow::new().await;
}

pub(crate) fn set_miss_entry(entry: &Entry) {
    unsafe { MISS_ENTRY = Some(ConstNonNull::new_unchecked(entry)) };
}

pub(crate) fn set_task_scope(scope: &dyn DerefScopeTrait) {
    unsafe { TASK_SCOPE = Some(ConstNonNull::new_unchecked(std::mem::transmute(scope))) };
}
