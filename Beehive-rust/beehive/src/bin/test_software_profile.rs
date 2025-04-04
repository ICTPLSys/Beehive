use beehive::profile::hardware_profiler::*;
use beehive::profile::software_profiler::*;
use beehive::thread::fork_join_with_id;
fn main() {
    const NUM_THREADS: usize = 16;
    const NUM_LOOP_COUNT: usize = 1024;
    const NUM_SLEEP_US: u64 = 1000;
    const GLOBAL_COUNT: usize = NUM_THREADS * NUM_LOOP_COUNT;
    const GLOBAL_SLEEP_US: u64 = NUM_SLEEP_US * GLOBAL_COUNT as u64;
    const SLEEP_US_EPS: u64 = GLOBAL_SLEEP_US / 1000 * 2;
    libfibre_port::cfibre_init(NUM_THREADS);
    init_profile_data(NUM_THREADS as usize);
    fork_join_with_id(NUM_THREADS * NUM_LOOP_COUNT, |_| {
        let _timer: Timer = Timer::new(0.into());
        let start = get_cycles();
        loop {
            if (get_cycles() - start) / CPU_FREQ_MHZ > NUM_SLEEP_US {
                break;
            }
        }
    });

    let glpd = collect_profile_data(NUM_THREADS as usize);
    let tc = glpd.time_datas()[0];
    assert_eq!(tc.count() as usize, GLOBAL_COUNT);
    assert!(
        (tc.time() / CPU_FREQ_MHZ as u64) < GLOBAL_SLEEP_US + SLEEP_US_EPS
            && (tc.time() / CPU_FREQ_MHZ as u64) > GLOBAL_SLEEP_US - SLEEP_US_EPS
    );
}
