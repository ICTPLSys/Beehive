use beehive::mem::{DerefScope, RemPtr, manager};
use beehive::net::Config;
use std::env;

const OBJECT_SIZE: usize = 1000;
const OBJECT_COUNT: usize = 8000;

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
    let scope = DerefScope {};
    let mut objects = Box::new(core::array::from_fn::<_, OBJECT_COUNT, _>(|_| {
        RemPtr::<Object>::null()
    }));
    for (i, object) in objects.iter_mut().enumerate() {
        let rem_ref = manager::allocate(object, Object::new(i as u64), &scope);
        assert_eq!(rem_ref.value, i as u64);
    }
    for (i, object) in objects.iter_mut().enumerate() {
        let mut rem_ref = manager::deref_mut(object, &scope);
        assert_eq!(rem_ref.value, i as u64);
        rem_ref.value *= 2;
    }
    for (i, object) in objects.iter().enumerate() {
        let rem_ref = manager::deref(object, &scope);
        assert_eq!(rem_ref.value, i as u64 * 2 as u64);
    }
}

fn main() {
    pretty_env_logger::init();
    let args: Vec<String> = env::args().collect();
    let path = &args[1];
    let config = Config::load_config(path);
    assert!(config.client_memory_size > OBJECT_SIZE * OBJECT_COUNT);
    manager::initialize(config);
    test_multiple_objects();
    manager::destroy();
}
