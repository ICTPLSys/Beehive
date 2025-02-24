use super::papi::*;
use crate::thread::meta::*;

static RESULT_PER_TH: bool = false;
pub const CPU_FREQ_MHZ: u64 = 2794;

pub fn get_cycles() -> u64 {
    unsafe {
        let mut aux = std::mem::MaybeUninit::<u32>::uninit();
        core::arch::x86_64::__rdtscp(aux.as_mut_ptr())
    }
}

#[derive(Debug)]
pub struct PerfResult {
    pub run_duration: u64,
    pub l3_cache_miss: i64,
}

impl PerfResult {
    pub fn new() -> Self {
        PerfResult {
            run_duration: 0,
            l3_cache_miss: 0,
        }
    }
    pub fn print(&self) {
        log::info!("L3 miss: {}", self.l3_cache_miss);
    }
    pub fn print_all(&self) {
        self.print();
        log::info!("runtime: {}ms", self.run_duration / CPU_FREQ_MHZ / 1000);
    }
}

pub fn perf_profile<F>(worker_num: usize, func: F) -> PerfResult
where
    F: FnOnce(),
{
    const PAPI_EVENT_COUNT: usize = 1;
    let mut papi_events: [i32; PAPI_EVENT_COUNT] = [RS_PAPI_L3_TCM];
    let mut event_sets: Vec<i32> = vec![];
    unsafe {
        for i in 0..worker_num {
            let tid = THREAD_TIDS[i];
            let mut papi_event_set = PAPI_NULL;
            let papi_event_set_rawptr = &mut papi_event_set as *mut i32;
            let papi_events_rawptr = &mut papi_events as *mut i32;
            debug_assert!(PAPI_create_eventset(papi_event_set_rawptr) == (PAPI_OK as i32));
            debug_assert!(
                PAPI_add_events(papi_event_set, papi_events_rawptr, PAPI_EVENT_COUNT as i32)
                    == (PAPI_OK as i32)
            );
            debug_assert!(PAPI_attach(papi_event_set, tid as u64) == PAPI_OK as i32);
            event_sets.push(papi_event_set);
        }
    }
    for es in event_sets.iter() {
        unsafe {
            debug_assert!(PAPI_start(*es) == PAPI_OK as i32);
        }
    }
    let start = get_cycles();
    func();
    let duration = get_cycles() - start;
    let mut papi_values: [i64; PAPI_EVENT_COUNT] = [0; PAPI_EVENT_COUNT];
    for es in event_sets.iter_mut() {
        let mut th_values: [i64; PAPI_EVENT_COUNT] = [0; PAPI_EVENT_COUNT];
        let th_vaules_rawptr = &mut th_values as *mut i64;
        unsafe {
            debug_assert!(PAPI_stop(*es, th_vaules_rawptr) == PAPI_OK as i32);
            debug_assert!(PAPI_detach(*es) == PAPI_OK as i32);
            debug_assert!(PAPI_cleanup_eventset(*es) == PAPI_OK as i32);
            let events_rawptr = es as *mut i32;
            debug_assert!(PAPI_destroy_eventset(events_rawptr) == PAPI_OK as i32);
        }
        for i in 0..PAPI_EVENT_COUNT {
            papi_values[i] += th_values[i];
        }
        if RESULT_PER_TH {
            let mut result = PerfResult::new();
            result.l3_cache_miss = th_values[0];
            log::info!("---------------------------------------");
            result.print();
            log::info!("---------------------------------------");
        }
    }
    let mut result = PerfResult::new();
    result.run_duration = duration;
    result.l3_cache_miss = papi_values[0];
    return result;
}

pub fn perf_init() {
    unsafe {
        debug_assert_eq!(PAPI_library_init(RS_PAPI_VER_CURRENT), RS_PAPI_VER_CURRENT);
    }
}

#[cfg(test)]
mod tests {
    use crate::thread::meta::*;
    use tokio::runtime;

    use crate::{
        profile::hardware_profiler::{perf_init, perf_profile},
        thread::meta::{fork_join, init_thread_id},
    };
    #[test]
    fn hardware_profiler_test() {
        const NUM_THREADS: u64 = 16;
        const NUM_LOOP_COUNT: u64 = 1024;
        const VEC_SIZE: usize = 1 << 16;
        perf_init();
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(NUM_THREADS as usize)
            .enable_all()
            .on_thread_start(|| {
                init_thread_id();
                log::info!(
                    "tokio runtime worker thread start: id = {}",
                    get_thread_idx()
                );
            })
            .build()
            .unwrap();
        perf_profile(NUM_THREADS as usize, || {
            fork_join(&rt, NUM_THREADS * NUM_LOOP_COUNT, async || {
                let mut v = vec![];
                let mut sum: usize = 0;
                for i in 0..VEC_SIZE {
                    v.push(i);
                    sum += i;
                }
                assert_eq!(sum, v.iter().sum::<usize>());
            });
        })
        .print_all();
    }
    // TODO check if the hardware counter result is reasonable
}
