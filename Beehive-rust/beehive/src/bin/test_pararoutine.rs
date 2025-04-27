use beehive::mem::manager;
use beehive::mem::{RemPtr, RootScope};
use beehive::pararoutine::{Executor, yield_now};
use beehive::thread::fork_join_with_id_with_scope;
use beehive_helper::beehive_main;
use std::array;
use std::time::Instant;

const THREAD_NUM: usize = 16;
const PARAROUTINE_NUM: usize = 4;
const TEST_COUNT: usize = 100000;

fn test_pararoutine() {
    let start = Instant::now();
    let mut n = 0usize;
    let root = RootScope::root();

    let mut executor = Executor::new_default(&root);
    (0..PARAROUTINE_NUM).for_each(|_| {
        let n = &mut n as *mut usize;
        executor.spawn(|_| async move {
            for _ in 0..TEST_COUNT {
                yield_now().await;
                unsafe {
                    *n += 1;
                }
            }
        });
    });
    executor.poll();
    assert_eq!(n, PARAROUTINE_NUM * TEST_COUNT);
    let duration = start.elapsed();
    println!(
        "Executor Time taken per task: {:?}",
        duration.div_f64((PARAROUTINE_NUM * TEST_COUNT) as f64)
    );
}

const OBJECT_SIZE: usize = 1000;
const OBJECT_COUNT: usize = 32 << 10;
struct Object {
    value: u64,
    _padding: [u8; OBJECT_SIZE - std::mem::size_of::<u64>()],
}

impl Object {
    fn new(value: u64) -> Self {
        Self {
            value: value,
            _padding: [0; OBJECT_SIZE - std::mem::size_of::<u64>()],
        }
    }
}

fn test_scope_pararoutine() {
    fork_join_with_id_with_scope(THREAD_NUM, |_, scope| {
        let mut executor = Executor::new_default(scope);
        (0..PARAROUTINE_NUM).for_each(|_| {
            executor.spawn(async |scope| {
                let mut objects: [Vec<RemPtr<Object>>; 2] =
                    array::from_fn(|_| Vec::with_capacity(OBJECT_COUNT));
                objects.iter_mut().for_each(|v| {
                    (0..OBJECT_COUNT).for_each(|_| {
                        v.push(RemPtr::null());
                    });
                });
                objects.iter_mut().for_each(|v| {
                    for (i, object) in v.iter_mut().enumerate() {
                        let rem_ref = manager::allocate(object, Some(Object::new(i as u64)), scope);
                        assert_eq!(rem_ref.value, i as u64);
                    }
                });
                let _ = objects.iter_mut().map(async |v| {
                    for (i, object) in v.iter_mut().enumerate() {
                        let mut rem_ref = manager::deref_mut_async(object, scope).await;
                        assert_eq!(rem_ref.value, i as u64);
                        rem_ref.value *= 2;
                    }
                });
                let _ = objects.iter().map(async |v| {
                    for (i, object) in v.iter().enumerate() {
                        let rem_ref = manager::deref_async(object, scope).await;
                        assert_eq!(rem_ref.value, i as u64 * 2 as u64);
                    }
                });
                let _ = objects[0].iter().enumerate().map(async |(i, object1)| {
                    let object2 = &objects[1][i];
                    let rem_ref1 = manager::deref_async(object1, scope).await;
                    let scope = scope.push(&rem_ref1);
                    let rem_ref2 = manager::deref_async(object2, &scope).await;
                    assert_eq!(rem_ref1.value, rem_ref2.value);
                });
            });
        });
        let mut rem_ptr: RemPtr<u64> = RemPtr::null();
        let mut rem_ref = manager::allocate(&mut rem_ptr, Some(1234), scope);
        let scope = scope.push(&rem_ref);
        *rem_ref = 5678;
        executor.poll_with_scope(&scope);
        assert_eq!(*rem_ref, 5678);
    });
    println!("test scope pararoutine success!");
}
#[beehive_main]
fn main() {
    test_pararoutine();
    test_scope_pararoutine();
}
