use beehive::mem::{DerefScope, RemPtr, manager};
use beehive::net::Config;
use std::env;

fn test_one_object() {
    let scope = DerefScope {};
    let mut ptr: RemPtr<i64> = RemPtr::null();
    let mut rem_ref = manager::allocate(&mut ptr, 1, &scope);
    assert_eq!(*rem_ref, 1);
    *rem_ref = 2;
    assert_eq!(*rem_ref, 2);
    manager::deallocate(&mut ptr);
}

fn main() {
    pretty_env_logger::init();
    let args: Vec<String> = env::args().collect();
    let path = &args[1];
    let config = Config::load_config(path);
    manager::initialize(config);
    test_one_object();
    manager::destroy();
}
