#![allow(internal_features)]
#![feature(thread_local)]
#![feature(core_intrinsics)]
#![feature(variant_count)]
#![feature(trait_alias)]
#![feature(ptr_as_ref_unchecked)]
#![allow(static_mut_refs)]
#![feature(generic_const_exprs)]
#![feature(non_null_from_ref)]

use libfibre_port::get_scope_state;
use mem::manager;
use mem::{DerefScopeTrait, evacuator::Evacuator, local_allocator::GlobalHeap};
use net::Config;
use std::ffi::c_void;
use std::sync::atomic::*;
use std::{
    mem::{MaybeUninit, variant_count},
    sync::atomic::{AtomicBool, Ordering},
};

#[macro_use]
pub mod utils;

pub mod mem;
pub mod net;
pub mod profile;
#[macro_use]
pub mod thread;

pub mod data_struct;
pub mod pararoutine;
pub mod rem_data_struct;

#[repr(C)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum MutatorState {
    OutOfScope = 0,
    InScopeV0 = 1,
    InScopeV1 = 2,
}

impl From<MutatorState> for i32 {
    fn from(value: MutatorState) -> Self {
        value as Self
    }
}

impl From<i32> for MutatorState {
    fn from(value: i32) -> Self {
        match value {
            0 => MutatorState::OutOfScope,
            1 => MutatorState::InScopeV0,
            2 => MutatorState::InScopeV1,
            _ => panic!("Error transform from i32 to MutatorState"),
        }
    }
}

pub(crate) static mut GLOBAL_MUTATOR_STATE: AtomicI32 =
    AtomicI32::new(MutatorState::InScopeV0 as i32);
static mut MUTATOR_STATE_COUNTS: [AtomicUsize; variant_count::<MutatorState>() as usize] = [
    AtomicUsize::new(0),
    AtomicUsize::new(0),
    AtomicUsize::new(0),
];

static mut BEEHIVE_RUNTIME: MaybeUninit<Runtime> = MaybeUninit::uninit();
struct Runtime {
    evacuate_cond: libfibre_port::Condition,
    evacuate_mutex: libfibre_port::Mutex,
    mutator_cond: libfibre_port::Condition,
    mutator_cannot_allocate: AtomicBool,
    evacuator_thread_count: usize,
    working: AtomicBool,
    evacuator_thread: libfibre_port::UThread<fn(*mut c_void), *mut c_void>,
}

impl Runtime {
    fn new(config: &Config) -> Self {
        manager::initialize(config);
        libfibre_port::cfibre_init(config.num_cores);
        profile::hardware_profiler::perf_init();
        Self {
            evacuate_cond: libfibre_port::Condition::new(),
            evacuate_mutex: libfibre_port::Mutex::new(),
            mutator_cond: libfibre_port::Condition::new(),
            mutator_cannot_allocate: AtomicBool::new(false),
            evacuator_thread_count: config.num_evacuate_threads,
            working: AtomicBool::new(true),
            evacuator_thread: libfibre_port::UThread::null(),
        }
    }

    fn destroy(&mut self) {
        self.working.store(false, Ordering::Relaxed);
        self.evacuate_cond.notify_all(&self.evacuate_mutex);
        self.evacuator_thread.join();
        manager::destroy();
    }

    pub fn start_evacuate(&'static mut self) {
        self.evacuator_thread = libfibre_port::UThread::new_with_c_func(
            Self::evacuate_work,
            (self as *mut Runtime) as *mut c_void,
            false,
        );
    }

    extern "C" fn evacuate_work(rt: *mut c_void) {
        let rt = unsafe { (rt as *mut Runtime).as_mut().unwrap() };
        let memory_low = || -> bool {
            rt.mutator_cannot_allocate.load(Ordering::Relaxed)
                || GlobalHeap::instance().memory_low()
        };
        let mut timestamp: u32 = 0;
        loop {
            if !rt.working.load(Ordering::Relaxed) {
                // println!("evacuate thread exit");
                return;
            }
            if !memory_low() {
                rt.evacuate_mutex.lock();
                // println!("evacuate thread notify");
                rt.mutator_cond.notify_all_locked();
                // println!("evacuate thread wait");
                rt.evacuate_cond.wait_locked(&rt.evacuate_mutex);
                // println!("evacuate thread wake up");
                continue;
            }
            Evacuator::evacuate_phase_work(&mut timestamp, rt.evacuator_thread_count);
            rt.mutator_cannot_allocate.store(false, Ordering::Relaxed);
        }
    }

    pub fn instance() -> &'static mut Self {
        unsafe { &mut *BEEHIVE_RUNTIME.as_mut_ptr() }
    }

    pub fn memory_low(&self) -> bool {
        GlobalHeap::instance().memory_low()
    }

    pub(crate) fn check_memory_low(&mut self, scope: &dyn DerefScopeTrait) {
        if self.memory_low() {
            self.evacuate_mutex.lock();
            self.mutator_cannot_allocate.store(true, Ordering::Relaxed);
            self.evacuate_cond.notify_all_locked();
            self.evacuate_mutex.unlock();
        }
        debug_assert_ne!(
            libfibre_port::get_scope_state(),
            MutatorState::OutOfScope as i32
        );
        // update scope
        let new_state = unsafe { GLOBAL_MUTATOR_STATE.load(Ordering::Relaxed) };
        if new_state != libfibre_port::get_scope_state() {
            unsafe { MUTATOR_STATE_COUNTS[new_state as usize].fetch_add(1, Ordering::Relaxed) };
            let old_state = libfibre_port::get_scope_state();
            libfibre_port::set_scope_state(new_state);
            scope.unmark();
            unsafe {
                let previous =
                    MUTATOR_STATE_COUNTS[old_state as usize].fetch_sub(1, Ordering::Relaxed);
                debug_assert!(previous > 0);
            };
        }
    }

    pub fn on_demand_evacuate(&mut self) {
        debug_assert_eq!(get_scope_state(), MutatorState::OutOfScope as i32);
        self.evacuate_mutex.lock();
        self.mutator_cannot_allocate.store(true, Ordering::Relaxed);
        // println!("mutator notify");
        self.evacuate_cond.notify_all_locked();
        // println!("mutator wait");
        self.mutator_cond.wait_locked(&self.evacuate_mutex);
        // println!("mutator wake up");
    }
}

pub fn initialize(config: &Config) {
    *Runtime::instance() = Runtime::new(config);
    Runtime::instance().start_evacuate();
}

pub fn destroy() {
    Runtime::instance().destroy();
}
pub fn flip_scope_state() {
    let old_state = unsafe { GLOBAL_MUTATOR_STATE.load(Ordering::Relaxed) };
    let new_state = {
        if old_state == MutatorState::InScopeV0 as i32 {
            MutatorState::InScopeV1 as i32
        } else {
            MutatorState::InScopeV0 as i32
        }
    };
    unsafe {
        while MUTATOR_STATE_COUNTS[new_state as usize].load(Ordering::SeqCst) != 0 {
            libfibre_port::yield_now();
        }
        GLOBAL_MUTATOR_STATE.store(new_state, Ordering::SeqCst);
        while MUTATOR_STATE_COUNTS[old_state as usize].load(Ordering::SeqCst) != 0 {
            libfibre_port::yield_now();
        }
    }
}

pub fn check_memory_low(scope: &dyn DerefScopeTrait) {
    Runtime::instance().check_memory_low(scope);
}
