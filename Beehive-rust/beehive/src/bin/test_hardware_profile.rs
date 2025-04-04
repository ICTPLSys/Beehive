use beehive::profile::hardware_profiler::*;
use beehive::thread::fork_join_with_id;
fn main() {
    const NUM_THREADS: usize = 16;
    const NUM_UTHREADS: usize = NUM_THREADS;
    const VEC_SIZE: usize = 1 << 16;
    perf_init();
    libfibre_port::cfibre_init(NUM_THREADS);
    perf_profile(NUM_THREADS as usize, || {
        fork_join_with_id(NUM_UTHREADS, |_| {
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
    // TODO fibre has conflict with Rust test suite
    // just exit here to avoid stuck
}
