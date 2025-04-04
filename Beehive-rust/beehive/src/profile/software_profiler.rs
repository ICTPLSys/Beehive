use super::hardware_profiler::{CPU_FREQ_MHZ, get_cycles};
use std::fmt::Display;
use std::mem;
use std::ops::AddAssign;

pub static mut THREAD_PROFILE_DATA: Vec<ProfileData> = vec![];

#[derive(Debug, Clone, Copy)]
#[repr(u64)]
#[allow(dead_code)]
pub enum ProfileTimeItem {
    Foo1 = 0,
    Foo2, // TODO add real profile item
}

#[derive(Debug, Clone, Copy)]
#[repr(u64)]
#[allow(dead_code)]
pub enum ProfileCountItem {
    Foo1 = 0,
}

impl From<ProfileTimeItem> for u64 {
    fn from(value: ProfileTimeItem) -> Self {
        value as u64
    }
}

impl From<u64> for ProfileTimeItem {
    fn from(value: u64) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

impl From<ProfileCountItem> for u64 {
    fn from(value: ProfileCountItem) -> Self {
        value as u64
    }
}

impl From<u64> for ProfileCountItem {
    fn from(value: u64) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProfileData {
    time_datas: [TimerCounterRecord; mem::variant_count::<ProfileTimeItem>() as usize],
    count_datas: [CounterCounterRecord; mem::variant_count::<ProfileCountItem>() as usize],
}

impl ProfileData {
    pub fn new() -> Self {
        ProfileData {
            time_datas: [
                TimerCounterRecord::new("Foo1"),
                TimerCounterRecord::new("Foo2"),
            ],
            count_datas: [CounterCounterRecord::new("Foo1")],
        }
    }

    pub fn reset(&mut self) {
        for td in self.time_datas.iter_mut() {
            td.reset();
        }
        for cd in self.count_datas.iter_mut() {
            cd.reset();
        }
    }

    pub fn time_datas(&self) -> &[TimerCounterRecord] {
        &self.time_datas
    }

    pub fn count_datas(&self) -> &[CounterCounterRecord] {
        &self.count_datas
    }
}

impl AddAssign<&Self> for ProfileData {
    fn add_assign(&mut self, rhs: &Self) {
        for i in 0..mem::variant_count::<ProfileTimeItem>() as usize {
            self.time_datas[i] += &rhs.time_datas[i];
        }
        for i in 0..mem::variant_count::<ProfileCountItem>() as usize {
            self.count_datas[i] += &rhs.count_datas[i];
        }
    }
}

impl Display for ProfileData {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "================ Profile Data ================\n")?;
        write!(f, "------------------ time Data -----------------\n")?;
        for e in self.time_datas.iter() {
            write!(f, "{}\n", e)?;
        }
        write!(f, "----------------- count Data -----------------\n")?;
        for e in self.count_datas.iter() {
            write!(f, "{}\n", e)?;
        }
        write!(f, "============== Profile Data End ==============")
    }
}
#[derive(Debug, Clone)]
pub struct Timer {
    start: u64,
    end: u64,
    profile_item: ProfileTimeItem,
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.end = get_cycles();
        unsafe {
            let id = libfibre_port::cfibre_thread_idx();
            THREAD_PROFILE_DATA[id].time_datas[self.profile_item as usize] += self as &Self;
        }
    }
}

impl Timer {
    pub fn new(profile_item: ProfileTimeItem) -> Self {
        let t = get_cycles();
        Timer {
            start: t,
            end: t,
            profile_item,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Counter {
    count: u64,
    profile_item: ProfileCountItem,
}

impl AddAssign<&Self> for Counter {
    fn add_assign(&mut self, rhs: &Self) {
        self.count += rhs.count;
    }
}

impl Drop for Counter {
    fn drop(&mut self) {
        unsafe {
            let id = libfibre_port::cfibre_thread_idx();
            THREAD_PROFILE_DATA[id].count_datas[self.profile_item as usize] += self as &Self;
        }
    }
}

impl Counter {
    pub fn new(profile_item: ProfileCountItem) -> Self {
        Counter {
            count: 0,
            profile_item,
        }
    }

    pub fn add(&mut self, cnt: u64) {
        self.count += cnt;
    }

    pub fn inc(&mut self) {
        self.add(1);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TimerCounterRecord {
    name: &'static str,
    time: u64,
    count: u64,
    step: u64,
}

impl TimerCounterRecord {
    pub fn new(name: &'static str) -> Self {
        Self {
            name: name,
            time: 0,
            count: 0,
            step: 1,
        }
    }

    pub fn reset(&mut self) {
        self.time = 0;
        self.count = 0;
        self.step = 1;
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn time(&self) -> u64 {
        self.time
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn step(&self) -> u64 {
        self.step
    }
}

impl AddAssign<&Timer> for TimerCounterRecord {
    fn add_assign(&mut self, rhs: &Timer) {
        self.time += rhs.end - rhs.start;
        self.count += self.step;
    }
}

impl Display for TimerCounterRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.count != 0 {
            write!(
                f,
                "Timer Record {}
                time: {}us
                count: {}
                avg: {}us",
                self.name,
                self.time / CPU_FREQ_MHZ,
                self.count,
                self.time / CPU_FREQ_MHZ / self.count
            )
        } else {
            write!(f, "Timer Record record nothing!")
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CounterCounterRecord {
    name: &'static str,
    counter: u64,
    count: u64,
    step: u64,
}

impl CounterCounterRecord {
    pub fn new(name: &'static str) -> Self {
        Self {
            name: name,
            counter: 0,
            count: 0,
            step: 1,
        }
    }

    pub fn reset(&mut self) {
        self.counter = 0;
        self.count = 1;
        self.step = 1;
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn step(&self) -> u64 {
        self.step
    }

    pub fn counter(&self) -> u64 {
        self.counter
    }
}
impl AddAssign<&Counter> for CounterCounterRecord {
    fn add_assign(&mut self, rhs: &Counter) {
        self.counter += rhs.count;
        self.count += self.step;
    }
}

impl Display for CounterCounterRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Counter Record: {}
            counter: {}
            count: {}
            avg: {}",
            self.name,
            self.counter,
            self.count,
            self.counter as f64 / self.count as f64
        )
    }
}

impl AddAssign<&Self> for TimerCounterRecord {
    fn add_assign(&mut self, rhs: &Self) {
        self.time += rhs.time;
        self.count += rhs.count;
    }
}

impl AddAssign<&Self> for CounterCounterRecord {
    fn add_assign(&mut self, rhs: &Self) {
        self.counter += rhs.counter;
        self.count += rhs.count;
    }
}

// TODO
// all handles are based on the tokio
// tokio should use n threads to run n tasks
// need further check
pub fn collect_profile_data(worker_num: usize) -> ProfileData {
    let mut global_profile_data = ProfileData::new();
    unsafe {
        for tpd in THREAD_PROFILE_DATA[..worker_num].iter() {
            global_profile_data += tpd;
        }
    }
    global_profile_data
}

pub fn reset_profile_data(worker_num: usize) {
    unsafe {
        THREAD_PROFILE_DATA.resize(worker_num, ProfileData::new());
        for tpd in THREAD_PROFILE_DATA[..worker_num].iter_mut() {
            tpd.reset();
        }
    }
}

pub fn init_profile_data(worker_num: usize) {
    reset_profile_data(worker_num);
}
