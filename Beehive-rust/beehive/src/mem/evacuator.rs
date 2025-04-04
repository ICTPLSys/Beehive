use super::RemoteAddr;
use super::local_allocator::GlobalHeap;
use crate::flip_scope_state;
use crate::mem::manager;
use crate::net::Client;
use crate::thread::fork_join_with_id;
use std::mem::MaybeUninit;

#[derive(Debug, Clone, Copy)]
pub(crate) struct EvacuateRequest {
    local_addr: u64,
    remote_addr: RemoteAddr,
    size: usize,
}

impl EvacuateRequest {
    pub fn new(local_addr: u64, remote_addr: RemoteAddr, size: usize) -> Self {
        Self {
            local_addr,
            remote_addr,
            size,
        }
    }
}

pub(crate) struct EvacuateBuffer {
    requests: [EvacuateRequest; Self::EVACUATE_BATCH_SIZE],
    count: usize,
}

impl EvacuateBuffer {
    const EVACUATE_BATCH_SIZE: usize = 32;
    pub fn new() -> Self {
        EvacuateBuffer {
            requests: unsafe { MaybeUninit::uninit().assume_init() },
            count: 0,
        }
    }

    pub fn add(&mut self, local_addr: u64, remote_addr: RemoteAddr, size: usize) {
        if std::intrinsics::unlikely(self.count == Self::EVACUATE_BATCH_SIZE) {
            self.post();
        }
        debug_assert!(self.count < Self::EVACUATE_BATCH_SIZE);
        self.requests[self.count] = EvacuateRequest::new(local_addr, remote_addr, size);
        self.count += 1;
    }

    pub fn post(&mut self) {
        if self.count == 0 {
            return;
        }
        debug_assert!(self.count <= Self::EVACUATE_BATCH_SIZE);
        for i in 0..self.count {
            let req = &self.requests[i];
            loop {
                if Client::post_write(
                    req.remote_addr,
                    req.local_addr,
                    req.size as u32,
                    req.local_addr,
                ) {
                    break;
                } else {
                    manager::check_cq();
                }
            }
        }
        self.count = 0;
        manager::check_cq();
        // todo!("batch write");
    }
}

impl Drop for EvacuateBuffer {
    fn drop(&mut self) {
        self.post();
        debug_assert_eq!(self.count, 0);
    }
}

pub(crate) struct Evacuator {}
impl Evacuator {
    fn mark_phase(ts: u32) {
        GlobalHeap::instance().mark(ts)
    }

    fn evacuate_phase(ts: u32) {
        let mut evacuate_buffer = EvacuateBuffer::new();
        GlobalHeap::instance().evacuate(&mut evacuate_buffer, ts);
        evacuate_buffer.post();
    }

    fn gc_phase(ts: u32) {
        GlobalHeap::instance().gc(ts)
    }

    pub fn evacuate_phase_work(ts: &mut u32, evacuate_thread_count: usize) {
        *ts += 1;
        let mark_ts = *ts;
        // println!("evacuate thread mark");
        fork_join_with_id(evacuate_thread_count, move |_| {
            Evacuator::mark_phase(mark_ts);
        });
        // println!("evacuate thread mark end");
        flip_scope_state();
        *ts += 1;
        let evacuate_ts = *ts;
        // println!("evacuate thread evacuate");
        fork_join_with_id(evacuate_thread_count, move |_| {
            Evacuator::evacuate_phase(evacuate_ts);
        });
        // println!("evacuate thread evacuate end");
        while manager::check_cq() > 0 {}
        *ts += 1;
        let gc_ts = *ts;
        // println!("evacuate thread gc");
        fork_join_with_id(evacuate_thread_count, move |_| {
            Evacuator::gc_phase(gc_ts);
        });
        // println!("evacuate thread gc end");
        while manager::check_cq() > 0 {}
    }
}
