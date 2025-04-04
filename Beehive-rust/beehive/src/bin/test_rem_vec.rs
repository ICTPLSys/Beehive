use beehive::{
    mem::RootScope,
    rem_data_struct::{
        rem_iter::{RemIterator, RemZip2},
        rem_vec::RemVec,
    },
    thread::fork_join_with_id_with_scope,
};
use beehive_helper::beehive_main;
use sync_ptr::SyncMutPtr;
const VEC_SIZE: usize = 10000000;
const THREAD_NUM: usize = 16;
fn test_single_thread() {
    let mut rem_vec = RemVec::<usize>::with_capacity(VEC_SIZE);
    let scope = RootScope::root();
    for i in 0..VEC_SIZE {
        rem_vec.push(i, &scope);
    }
    for (i, (_, elem)) in RemIterator::new(rem_vec.iter_sync(), &scope).enumerate() {
        assert_eq!(*elem, i);
    }
    for (i, (_, mut elem)) in RemIterator::new(rem_vec.iter_mut_sync(), &scope).enumerate() {
        *elem = VEC_SIZE - i;
    }
    for (i, (_, elem)) in RemIterator::new(rem_vec.iter_sync(), &scope).enumerate() {
        assert_eq!(*elem, VEC_SIZE - i);
    }
    println!("test_single_thread success");
}

fn test_multi_thread() {
    let mut rem_vec = RemVec::<usize>::with_capacity(VEC_SIZE);
    {
        let scope = RootScope::root();
        for i in 0..VEC_SIZE {
            rem_vec.push(i, &scope);
        }
    }
    fork_join_with_id_with_scope(THREAD_NUM, |id, scope| {
        let rem_vec = &rem_vec;
        let start = id * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM;
        let end = ((id + 1) * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM).min(VEC_SIZE);
        for (i, (_, elem)) in
            RemIterator::new(rem_vec.iter_sync_with_setting(start, end, 1), scope).enumerate()
        {
            assert_eq!(*elem, start + i);
        }
    });
    let rem_vec_ptr = unsafe { SyncMutPtr::new(&mut rem_vec) };
    fork_join_with_id_with_scope(THREAD_NUM, |id, scope| {
        let rem_vec = unsafe { rem_vec_ptr.as_mut().unwrap() };
        let start = id * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM;
        let end = ((id + 1) * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM).min(VEC_SIZE);
        for (i, (_, mut elem)) in
            RemIterator::new(rem_vec.iter_mut_sync_with_setting(start, end, 1), scope).enumerate()
        {
            *elem = VEC_SIZE - start - i;
        }
    });
    fork_join_with_id_with_scope(THREAD_NUM, |id, scope| {
        let rem_vec: &RemVec<usize> = unsafe { std::mem::transmute(&rem_vec) };
        let start = id * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM;
        let end = ((id + 1) * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM).min(VEC_SIZE);
        for (i, (_, elem)) in
            RemIterator::new(rem_vec.iter_sync_with_setting(start, end, 1), scope).enumerate()
        {
            assert_eq!(*elem, VEC_SIZE - start - i);
        }
    });
    println!("test_multi_thread success");
}
fn test_zip() {
    let mut rem_vec1 = RemVec::<usize>::with_capacity(VEC_SIZE);
    let mut rem_vec2 = RemVec::<usize>::with_capacity(VEC_SIZE);
    {
        let scope = RootScope::root();
        for i in 0..VEC_SIZE {
            rem_vec1.push(i, &scope);
            rem_vec2.push(i, &scope);
        }
    }
    let rem_vec1_ptr = unsafe { SyncMutPtr::new(&mut rem_vec1) };
    let rem_vec2_ptr = unsafe { SyncMutPtr::new(&mut rem_vec2) };
    fork_join_with_id_with_scope(THREAD_NUM, |id, scope| {
        let rem_vec1 = unsafe { rem_vec1_ptr.as_mut().unwrap() };
        let rem_vec2 = unsafe { rem_vec2_ptr.as_mut().unwrap() };
        let start = id * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM;
        let end = ((id + 1) * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM).min(VEC_SIZE);
        for (i, (_, elem1, elem2)) in RemZip2::new(
            rem_vec1.iter_sync_with_setting(start, end, 1),
            rem_vec2.iter_sync_with_setting(start, end, 1),
            scope,
        )
        .enumerate()
        {
            assert_eq!((*elem1, *elem2), (start + i, start + i));
        }
    });
    fork_join_with_id_with_scope(THREAD_NUM, |id, scope| {
        let rem_vec1 = unsafe { rem_vec1_ptr.as_mut().unwrap() };
        let rem_vec2 = unsafe { rem_vec2_ptr.as_mut().unwrap() };
        let start = id * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM;
        let end = ((id + 1) * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM).min(VEC_SIZE);
        for (i, (_, mut elem1, mut elem2)) in RemZip2::new(
            rem_vec1.iter_mut_sync_with_setting(start, end, 1),
            rem_vec2.iter_mut_sync_with_setting(start, end, 1),
            scope,
        )
        .enumerate()
        {
            *elem1 = VEC_SIZE - start - i;
            *elem2 = VEC_SIZE - start - i;
        }
    });
    fork_join_with_id_with_scope(THREAD_NUM, |id, scope| {
        let rem_vec1 = unsafe { rem_vec1_ptr.as_mut().unwrap() };
        let rem_vec2 = unsafe { rem_vec2_ptr.as_mut().unwrap() };
        let start = id * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM;
        let end = ((id + 1) * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM).min(VEC_SIZE);
        for (i, (_, elem1, elem2)) in RemZip2::new(
            rem_vec1.iter_sync_with_setting(start, end, 1),
            rem_vec2.iter_sync_with_setting(start, end, 1),
            scope,
        )
        .enumerate()
        {
            assert_eq!(
                (*elem1, *elem2),
                (VEC_SIZE - start - i, VEC_SIZE - start - i)
            );
        }
    });
    println!("test_zip success");
}

fn test_async() {
    let mut rem_vec = RemVec::<usize>::with_capacity(VEC_SIZE);
    {
        let scope = RootScope::root();
        for i in 0..VEC_SIZE {
            rem_vec.push(i, &scope);
        }
    }
    fork_join_with_id_with_scope(THREAD_NUM, |id, scope| {
        let rem_vec: &RemVec<usize> = unsafe { std::mem::transmute(&rem_vec) };
        let start = id * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM;
        let end = ((id + 1) * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM).min(VEC_SIZE);
        for (i, (_, elem)) in
            RemIterator::new(rem_vec.iter_prefetch_with_setting(start, end, 1), scope).enumerate()
        {
            assert_eq!(*elem, start + i);
        }
    });
    let rem_vec_ptr = unsafe { SyncMutPtr::new(&mut rem_vec) };
    fork_join_with_id_with_scope(THREAD_NUM, |id, scope| {
        let rem_vec = unsafe { rem_vec_ptr.as_mut().unwrap() };
        let start = id * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM;
        let end = ((id + 1) * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM).min(VEC_SIZE);
        for (i, (_, mut elem)) in
            RemIterator::new(rem_vec.iter_mut_prefetch_with_setting(start, end, 1), scope)
                .enumerate()
        {
            *elem = VEC_SIZE - start - i;
        }
    });
    fork_join_with_id_with_scope(THREAD_NUM, |id, scope| {
        let rem_vec: &RemVec<usize> = unsafe { std::mem::transmute(&rem_vec) };
        let start = id * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM;
        let end = ((id + 1) * (VEC_SIZE + THREAD_NUM - 1) / THREAD_NUM).min(VEC_SIZE);
        for (i, (_, elem)) in
            RemIterator::new(rem_vec.iter_prefetch_with_setting(start, end, 1), scope).enumerate()
        {
            assert_eq!(*elem, VEC_SIZE - start - i);
        }
    });
    println!("test_async success");
}

fn test_copy_to_local() {
    let mut rem_vec = RemVec::<usize>::with_capacity(VEC_SIZE);
    {
        let scope = RootScope::root();
        for i in 0..VEC_SIZE {
            rem_vec.push(i, &scope);
        }
    }
    let mut local_buf = vec![0; VEC_SIZE];
    rem_vec.copy_to_local(&mut local_buf, 0, VEC_SIZE);
    for (i, elem) in local_buf.iter().enumerate() {
        assert_eq!(*elem, i);
    }
    println!("test_copy_to_local success");
}

fn test_group_iter() {
    let mut rem_vec = RemVec::<usize>::with_capacity(VEC_SIZE);
    let scope = RootScope::root();
    {
        for i in 0..VEC_SIZE {
            rem_vec.push(i, &scope);
        }
    }
    let group_count = rem_vec.groups_count() - 1;
    for (i, (_, elem)) in
        RemIterator::new(rem_vec.group_iter_sync_with_setting(0, group_count), &scope).enumerate()
    {
        for (j, elem) in elem.iter().enumerate() {
            assert_eq!(*elem, i * rem_vec.group_size() + j);
        }
    }
    println!("test_group_iter success");
}

#[beehive_main]
fn main() {
    test_single_thread();
    test_multi_thread();
    test_zip();
    test_async();
    test_copy_to_local();
    test_group_iter();
}
