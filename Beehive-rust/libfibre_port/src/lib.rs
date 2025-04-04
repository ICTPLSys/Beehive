#![feature(extern_types)]
use std::ffi::c_void;
// we dont need to operate those directly
// so just alias it
pub type FibreMutex = c_void;
pub type Fibre = c_void;
pub type FibreCondition = c_void;

#[repr(C)]
pub struct UThreadLocal {
    scope_state: i32,
}
#[link(name = "cfibre", kind = "static")]
unsafe extern "C" {
    fn uthread_init(workers: usize);
    fn uthread_yield() -> bool;
    fn uthread_create(
        func: extern "C" fn(*mut c_void),
        args: *mut c_void,
        low_priority: bool,
    ) -> *mut Fibre;
    fn uthread_join(uthread: *mut Fibre) -> *mut c_void;
    fn uthread_destroy(uthread: *mut Fibre);
    fn lock(mutex: *mut FibreMutex);
    fn try_lock(mutex: *mut FibreMutex) -> bool;
    fn unlock(mutex: *mut FibreMutex) -> bool;
    fn wait_locked(cond: *mut FibreCondition, mutex: *mut FibreMutex);
    fn wait(cond: *mut FibreCondition, mutex: *mut FibreMutex);
    fn notify_locked(cond: *mut FibreCondition);
    fn notify_all_locked(cond: *mut FibreCondition);
    fn notify(cond: *mut FibreCondition, mutex: *mut FibreMutex);
    fn notify_all(cond: *mut FibreCondition, mutex: *mut FibreMutex);
    fn new_mutex() -> *mut FibreMutex;
    fn new_condition() -> *mut FibreCondition;
    fn destroy_mutex(mutex: *mut FibreMutex);
    fn destroy_condition(cond: *mut FibreCondition);
    fn get_thread_idx() -> usize;
    fn get_thread_tid(idx: usize) -> i32;
    fn get_tls() -> *mut UThreadLocal;
    fn get_worker_count() -> usize;
}

pub fn cfibre_init(workers: usize) {
    unsafe {
        uthread_init(workers);
    }
}

pub fn cfibre_join(uthread: *mut Fibre) {
    unsafe {
        uthread_join(uthread);
    }
}

pub fn cfibre_destroy(uthread: *mut Fibre) {
    unsafe {
        uthread_destroy(uthread);
    }
}

pub fn cfibre_create(
    func: extern "C" fn(*mut c_void),
    args: *mut c_void,
    low_priority: bool,
) -> *mut Fibre {
    unsafe { uthread_create(func, args, low_priority) }
}

pub fn cfibre_get_worker_count() -> usize {
    unsafe { get_worker_count() }
}

pub fn yield_now() -> bool {
    unsafe { uthread_yield() }
}

pub fn cfibre_thread_idx() -> usize {
    unsafe { get_thread_idx() }
}

pub fn cfibre_thread_tid(id: usize) -> i32 {
    unsafe { get_thread_tid(id) }
}

pub struct Mutex {
    mutex: *mut FibreMutex,
}

impl Mutex {
    pub fn new() -> Self {
        Self {
            mutex: unsafe { new_mutex() },
        }
    }

    pub fn lock(&self) {
        unsafe {
            lock(self.mutex);
        }
    }

    pub fn unlock(&self) {
        unsafe {
            unlock(self.mutex);
        }
    }

    pub fn try_lock(&self) -> bool {
        unsafe { try_lock(self.mutex) }
    }
}

impl Drop for Mutex {
    fn drop(&mut self) {
        unsafe {
            destroy_mutex(self.mutex);
        }
    }
}

pub struct Condition {
    cond: *mut FibreCondition,
}

impl Condition {
    pub fn new() -> Self {
        Self {
            cond: unsafe { new_condition() },
        }
    }

    pub fn wait(&self, mutex: &Mutex) {
        unsafe {
            wait(self.cond, mutex.mutex);
        }
    }

    pub fn notify(&self, mutex: &Mutex) {
        unsafe {
            notify(self.cond, mutex.mutex);
        }
    }

    pub fn notify_all(&self, mutex: &Mutex) {
        unsafe {
            notify_all(self.cond, mutex.mutex);
        }
    }

    pub fn wait_locked(&self, mutex: &Mutex) {
        unsafe {
            wait_locked(self.cond, mutex.mutex);
        }
    }

    pub fn notify_locked(&self) {
        unsafe {
            notify_locked(self.cond);
        }
    }

    pub fn notify_all_locked(&self) {
        unsafe {
            notify_all_locked(self.cond);
        }
    }
}

impl Drop for Condition {
    fn drop(&mut self) {
        unsafe {
            destroy_condition(self.cond);
        }
    }
}

pub fn get_scope_state() -> i32 {
    unsafe { (*get_tls()).scope_state }
}

pub fn set_scope_state(state: i32) {
    unsafe {
        (*get_tls()).scope_state = state;
    }
}

#[repr(C)]
struct TaskWrapper<F, Arg>
where
    F: FnOnce(Arg) + Send,
{
    closure: F,
    arg: Arg,
}

impl<F, Arg> TaskWrapper<F, Arg>
where
    F: FnOnce(Arg) + Send,
{
    fn new(closure: F, arg: Arg) -> Self {
        Self { closure, arg }
    }

    fn call(&mut self) {
        let closure = unsafe { std::ptr::read(&self.closure) };
        closure(unsafe { std::ptr::read(&self.arg) });
    }

    extern "C" fn adapter(data: *mut c_void) {
        unsafe {
            (data as *mut TaskWrapper<F, Arg>).as_mut().unwrap().call();
        }
    }
}

pub struct UThread<F, Arg>
where
    F: FnOnce(Arg) + Send,
{
    uthread: *mut Fibre,
    task_wrapper: *mut TaskWrapper<F, Arg>,
}

impl<F, Arg> UThread<F, Arg>
where
    F: FnOnce(Arg) + Send,
{
    pub fn new(f: F, arg: Arg, low_priority: bool) -> Self {
        let mut uthread = Self {
            uthread: std::ptr::null_mut(),
            task_wrapper: Box::into_raw(Box::new(TaskWrapper::new(f, arg))),
        };
        uthread.uthread = cfibre_create(
            TaskWrapper::<F, Arg>::adapter,
            uthread.task_wrapper as *mut c_void,
            low_priority,
        );
        uthread
    }

    pub fn new_with_c_func(
        func: extern "C" fn(*mut c_void),
        arg: *mut c_void,
        low_priority: bool,
    ) -> Self {
        let mut uthread = Self {
            uthread: std::ptr::null_mut(),
            task_wrapper: std::ptr::null_mut(),
        };
        uthread.uthread = cfibre_create(func, arg as *mut c_void, low_priority);
        uthread
    }

    pub fn join(&self) {
        cfibre_join(self.uthread);
    }

    pub fn null() -> Self {
        Self {
            uthread: std::ptr::null_mut(),
            task_wrapper: std::ptr::null_mut(),
        }
    }
}

impl<F, Arg> Drop for UThread<F, Arg>
where
    F: FnOnce(Arg) + Send,
{
    fn drop(&mut self) {
        cfibre_destroy(self.uthread);
        if !self.task_wrapper.is_null() {
            let _ = unsafe { Box::from_raw(self.task_wrapper) };
        }
    }
}

pub fn spawn<F, Arg>(tf: F, arg: Arg) -> UThread<F, Arg>
where
    F: FnOnce(Arg) + Send,
{
    UThread::new(tf, arg, false)
}
