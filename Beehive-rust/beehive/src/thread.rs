pub mod meta;
use crate::mem::{DerefScopeTrait, RootScope};
use libfibre_port::*;
use std::ffi::c_void;
pub fn fork_join<F, Arg>(tf: F, arg_list: &[Arg])
where
    F: FnMut(Arg) + Send + Clone,
    Arg: Clone,
{
    let uthreads = arg_list
        .iter()
        .map(|arg| spawn(tf.clone(), arg.clone()))
        .collect::<Vec<_>>();
    for uthread in uthreads.iter() {
        uthread.join();
    }
}

pub fn fork_join_task<F>(thread_count: usize, task_count: usize, mut tf: F)
where
    F: FnMut(usize) + Send + Clone,
{
    let id_list = (0..thread_count).collect::<Vec<_>>();
    let task_per_thread = (task_count + thread_count - 1) / thread_count;
    fork_join(
        move |thread_id| {
            let task_start = thread_id * task_per_thread;
            let task_end = ((thread_id + 1) * task_per_thread).min(task_count);
            if task_start >= task_count {
                return;
            }
            for task_id in task_start..task_end {
                tf(task_id);
            }
        },
        &id_list,
    );
}

pub fn fork_join_with_id<F>(thread_count: usize, tf: F)
where
    F: FnMut(usize) + Send + Clone,
{
    fork_join_task(thread_count, thread_count, tf);
}

struct TaskScopeWrapper<F, Arg>
where
    F: FnOnce(Arg, &dyn DerefScopeTrait) + Send,
{
    closure: F,
    arg: Arg,
}

impl<F, Arg> TaskScopeWrapper<F, Arg>
where
    F: FnOnce(Arg, &dyn DerefScopeTrait) + Send,
{
    fn new(closure: F, arg: Arg) -> Self {
        Self { closure, arg }
    }

    extern "C" fn adapter(data: *mut c_void) {
        unsafe {
            (data as *mut TaskScopeWrapper<F, Arg>)
                .as_mut()
                .unwrap()
                .call();
        }
    }

    fn call(&mut self) {
        let closure = unsafe { std::ptr::read(&self.closure) };
        let scope = RootScope::root();
        closure(unsafe { std::ptr::read(&self.arg) }, &scope);
    }
}

pub struct ScopeUThread<F, Arg>
where
    F: FnMut(Arg, &dyn DerefScopeTrait) + Send + Clone,
{
    uthread: *mut Fibre,
    task_wrapper: *mut TaskScopeWrapper<F, Arg>,
}

impl<F, Arg> ScopeUThread<F, Arg>
where
    F: FnMut(Arg, &dyn DerefScopeTrait) + Send + Clone,
{
    fn new(f: F, arg: Arg, low_priority: bool) -> Self {
        let mut uthread = Self {
            uthread: std::ptr::null_mut(),
            task_wrapper: Box::into_raw(Box::new(TaskScopeWrapper::new(f, arg))),
        };
        uthread.uthread = cfibre_create(
            TaskScopeWrapper::<F, Arg>::adapter,
            uthread.task_wrapper as *mut c_void,
            low_priority,
        );
        uthread
    }

    fn join(&self) {
        cfibre_join(self.uthread);
    }
}

impl<F, Arg> Drop for ScopeUThread<F, Arg>
where
    F: FnMut(Arg, &dyn DerefScopeTrait) + Send + Clone,
{
    fn drop(&mut self) {
        cfibre_destroy(self.uthread);
    }
}
pub fn spawn_with_scope<F, Arg>(f: F, arg: Arg) -> ScopeUThread<F, Arg>
where
    F: FnMut(Arg, &dyn DerefScopeTrait) + Send + Clone,
{
    ScopeUThread::new(f, arg, false)
}

pub fn fork_join_with_scope<F, Arg>(tf: F, arg_list: &[Arg])
where
    F: FnMut(Arg, &dyn DerefScopeTrait) + Send + Clone,
    Arg: Clone,
{
    let uthreads = arg_list
        .iter()
        .map(|arg| spawn_with_scope(tf.clone(), arg.clone()))
        .collect::<Vec<_>>();
    for uthread in uthreads.iter() {
        uthread.join();
    }
}

pub fn fork_join_task_with_scope<F>(thread_count: usize, task_count: usize, mut tf: F)
where
    F: FnMut(usize, &dyn DerefScopeTrait) + Send + Clone,
{
    let id_list = (0..thread_count).collect::<Vec<_>>();
    let task_per_thread = (task_count + thread_count - 1) / thread_count;
    fork_join_with_scope(
        move |thread_id, scope| {
            let task_start = thread_id * task_per_thread;
            let task_end = ((thread_id + 1) * task_per_thread).min(task_count);
            if task_start >= task_count {
                return;
            }
            for task_id in task_start..task_end {
                tf(task_id, scope);
            }
        },
        &id_list,
    );
}

pub fn fork_join_with_id_with_scope<F>(thread_count: usize, tf: F)
where
    F: FnMut(usize, &dyn DerefScopeTrait) + Send + Clone,
{
    fork_join_task_with_scope(thread_count, thread_count, tf);
}
