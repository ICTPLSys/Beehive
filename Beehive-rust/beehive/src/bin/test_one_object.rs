use beehive::mem::{DerefScopeBaseTrait, RemPtr, RootScope, manager};

fn test_one_object() {
    let scope = RootScope::root();
    let mut ptr: RemPtr<i64> = RemPtr::null();
    let mut rem_ref = manager::allocate(&mut ptr, Some(1), &scope);
    // in fact, this is not necessary since no more code will use _scope
    let _scope = scope.push(&rem_ref);
    assert_eq!(*rem_ref, 1);
    *rem_ref = 2;
    assert_eq!(*rem_ref, 2);
    manager::deallocate(&mut ptr);
}

#[beehive_helper::beehive_main]
fn main() {
    test_one_object();
}
