#![feature(generic_const_exprs)]
use beehive::{
    pararoutine::Executor,
    rem_data_struct::rem_concurrent_map::RemConcurrentMap,
    thread::{fork_join_task_with_scope, fork_join_with_id_with_scope},
};
use beehive_helper::beehive_main;
use core::mem::size_of;
use static_assertions::const_assert;
use std::hash::Hash;
use sync_ptr::SendMutPtr;
const THREAD_COUNT: usize = 16;
const TASK_COUNT: usize = 500000;
const NUM_SHIFT: usize = 20;
const_assert!((1 << NUM_SHIFT) >= TASK_COUNT);

struct Data<K>
where
    K: Hash + Eq,
    [(); 128 - 2 * size_of::<K>()]: Sized,
{
    key: K,
    _padding: [u8; 128 - 2 * size_of::<K>()],
}

impl<K> Data<K>
where
    K: Hash + Eq,
    [(); 128 - 2 * size_of::<K>()]: Sized,
{
    fn new(key: K) -> Self {
        Self {
            key,
            _padding: [0; 128 - 2 * size_of::<K>()],
        }
    }
}

type Key = usize;
type Value = Data<Key>;

fn test_rem_concurrent_map() {
    let mut map = RemConcurrentMap::<Key, Value>::new(NUM_SHIFT);
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_task_with_scope(THREAD_COUNT, TASK_COUNT, move |id, scope| {
        let map = unsafe { map_ptr.as_mut().unwrap() };
        map.insert_sync(id, Value::new(id), scope);
    });
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_task_with_scope(THREAD_COUNT, TASK_COUNT, move |id, scope| {
        let map = unsafe { map_ptr.as_mut().unwrap() };
        assert!(map.get_sync(
            &id,
            |_, v, _| {
                assert_eq!(v.key, id);
            },
            scope
        ));
    });
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_task_with_scope(THREAD_COUNT, TASK_COUNT, move |id, scope| {
        let map = unsafe { map_ptr.as_mut().unwrap() };
        map.remove_sync(&id, scope);
    });
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_task_with_scope(THREAD_COUNT, TASK_COUNT, move |id, scope| {
        let map = unsafe { map_ptr.as_mut().unwrap() };
        assert!(!map.get_sync(
            &id,
            |_, _, _| {
                panic!("key {} should not exist", id);
            },
            scope
        ));
    });
    println!("test_rem_concurrent_map passed");
}

fn test_pararoutine_rem_map() {
    let mut map = RemConcurrentMap::<Key, Value>::new(NUM_SHIFT);
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    const TASK_PER_THREAD: usize = TASK_COUNT / THREAD_COUNT;
    fork_join_with_id_with_scope(THREAD_COUNT, move |id, scope| {
        let mut executor = Executor::new_default(scope);
        (0..TASK_PER_THREAD)
            .map(|i| i + id * TASK_PER_THREAD)
            .for_each(|i| {
                executor.spawn(|scope| async move {
                    let map = unsafe { map_ptr.as_mut().unwrap() };
                    map.insert_async(i, Value::new(id), scope).await;
                });
            });
    });
    println!("insert end");
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_with_id_with_scope(THREAD_COUNT, move |id, scope| {
        let mut executor = Executor::new_default(scope);
        (0..TASK_PER_THREAD)
            .map(|i| i + id * TASK_PER_THREAD)
            .for_each(|i| {
                executor.spawn(|scope| async move {
                    let map = unsafe { map_ptr.as_mut().unwrap() };
                    assert!(
                        map.get_async(
                            &i,
                            |_, v, _| {
                                assert_eq!(v.key, id);
                            },
                            scope
                        )
                        .await
                    );
                });
            });
    });
    println!("get end");
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_with_id_with_scope(THREAD_COUNT, move |id, scope| {
        let mut executor = Executor::new_default(scope);
        (0..TASK_PER_THREAD)
            .map(|i| i + id * TASK_PER_THREAD)
            .for_each(|i| {
                executor.spawn(|scope| async move {
                    let map = unsafe { map_ptr.as_mut().unwrap() };
                    map.remove_async(&i, scope).await;
                });
            });
    });
    println!("remove end");
    let map_ptr = unsafe { SendMutPtr::new(&mut map) };
    fork_join_with_id_with_scope(THREAD_COUNT, move |id, scope| {
        let mut executor = Executor::new_default(scope);
        (0..TASK_PER_THREAD)
            .map(|i| i + id * TASK_PER_THREAD)
            .for_each(|i| {
                executor.spawn(|scope| async move {
                    let map = unsafe { map_ptr.as_mut().unwrap() };
                    assert!(
                        !map.get_async(
                            &i,
                            |_, _, _| {
                                panic!("key {} should not exist", i);
                            },
                            scope
                        )
                        .await
                    );
                });
            });
    });
    println!("get end");
    println!("test_pararoutine_rem_map passed");
}

#[beehive_main]
fn main() {
    test_rem_concurrent_map();
    test_pararoutine_rem_map();
}
