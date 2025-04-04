use beehive::mem::{DerefScopeBaseTrait, RemPtr, RootScope, manager};

const OBJECT_SIZE: usize = 1000;
const OBJECT_COUNT: usize = 32 << 10;
const THREAD_COUNT: usize = 4;

struct Object {
    value: u64,
    _padding: [u8; OBJECT_SIZE - 8],
}

impl Object {
    fn new(value: u64) -> Self {
        Self {
            value: value,
            _padding: [0; OBJECT_SIZE - 8],
        }
    }
}

fn test_multiple_objects() {
    let scope = RootScope::root();
    let mut objects = Vec::with_capacity(OBJECT_COUNT);
    for _ in 0..OBJECT_COUNT {
        objects.push(RemPtr::null());
    }
    for (i, object) in objects.iter_mut().enumerate() {
        let rem_ref = manager::allocate(object, Some(Object::new(i as u64)), &scope);
        // in fact, this is not necessary since no more code will use _scope
        let _scope = scope.push(&rem_ref);
        assert_eq!(rem_ref.value, i as u64);
    }
    for (i, object) in objects.iter_mut().enumerate() {
        let mut rem_ref = manager::deref_mut_sync(object, &scope);
        assert_eq!(rem_ref.value, i as u64);
        rem_ref.value *= 2;
    }
    for (i, object) in objects.iter().enumerate() {
        let rem_ref = manager::deref_sync(object, &scope);
        assert_eq!(rem_ref.value, i as u64 * 2 as u64);
    }
}

#[beehive_helper::beehive_main]
fn main() {
    beehive::thread::fork_join_with_id(THREAD_COUNT, |_| {
        test_multiple_objects();
    });
    println!("test_multiple_objects success");
}
