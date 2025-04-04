#![feature(ptr_as_ref_unchecked)]
use core::panic;

use beehive::{data_struct::concurrent_map::ConcurrentMap, thread::fork_join_task};
use libfibre_port::*;
use sync_ptr::SendMutPtr;

const WORKER_COUNT: usize = 4;
const THREAD_COUNT: usize = 16;
const TASK_COUNT: usize = 500000;
fn main() {
    cfibre_init(WORKER_COUNT);
    let mut map = ConcurrentMap::<usize, usize>::new(20);
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_task(THREAD_COUNT, TASK_COUNT, move |id| {
        let map = unsafe { map_ptr.as_mut_unchecked() };
        map.insert(id, id);
    });
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_task(THREAD_COUNT, TASK_COUNT, move |id| {
        let map = unsafe { map_ptr.as_mut_unchecked() };
        assert!(map.get(&id, |k, v| {
            assert_eq!(*k, *v);
        }));
    });
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_task(THREAD_COUNT, TASK_COUNT, move |id| {
        let map = unsafe { map_ptr.as_mut_unchecked() };
        map.update(id, None, |_, v| {
            *v = 0;
        });
    });
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_task(THREAD_COUNT, TASK_COUNT, move |id| {
        let map = unsafe { map_ptr.as_mut_unchecked() };
        assert!(map.get(&id, |_, v| {
            assert_eq!(0, *v);
        }));
    });
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_task(THREAD_COUNT, TASK_COUNT, move |id| {
        let map = unsafe { map_ptr.as_mut_unchecked() };
        map.remove(&id);
    });
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_task(THREAD_COUNT, TASK_COUNT, move |id| {
        let map = unsafe { map_ptr.as_mut_unchecked() };
        assert!(!map.get(&id, |_, _| {
            panic!("key found!");
        }));
    });
    println!("test concurrent map success!");
}
