use crate::manager::{OnMiss, deref, deref_async, deref_mut, deref_mut_async};
use crate::mem::{DerefScopeTrait, manager};
use crate::mem::{RemPtr, RemRef, RemRefMut};
use crate::utils::bitfield::*;
use crate::utils::spinlock::SpinLock;
use core::panic;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
// todo remote it
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EntryState(u32);

impl EntryState {
    const FREE: u32 = 0;
    const IMMUT: u32 = 1;
    const WRITE: u32 = 2;
    const BUSY: u32 = 3;

    define_bits_32!(state, 0, 2);
    define_bits_32!(read_cnt, 3, 31);

    fn new(state: u32, read_cnt: u32) -> Self {
        let mut entry_state = Self { 0: 0 };
        entry_state.set_state(state);
        entry_state.set_read_cnt(read_cnt);
        entry_state
    }

    fn inc_read_cnt(&mut self) {
        self.set_read_cnt(self.read_cnt() + 1);
    }

    fn dec_read_cnt(&mut self) {
        self.set_read_cnt(self.read_cnt() - 1);
    }

    fn free_state() -> Self {
        Self::new(EntryState::FREE, 0)
    }

    fn busy_state() -> Self {
        Self::new(EntryState::BUSY, 0)
    }

    fn immut_state(read_cnt: u32) -> Self {
        Self::new(EntryState::IMMUT, read_cnt)
    }

    fn write_state() -> Self {
        Self::new(EntryState::WRITE, 0)
    }
}

impl From<u32> for EntryState {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<EntryState> for u32 {
    fn from(value: EntryState) -> Self {
        value.0
    }
}

struct Data<K, V>(K, V);

impl<K, V> Data<K, V> {
    fn new(key: K, value: V) -> Self {
        Self(key, value)
    }

    fn key(&self) -> &K {
        &self.0
    }

    fn value(&self) -> &V {
        &self.1
    }

    fn value_mut(&mut self) -> &mut V {
        &mut self.1
    }
}

struct RemConcurrentMapEntry<K, V>
where
    K: Hash + Eq,
{
    bitmap: u32,
    spin: SpinLock,
    data: Option<RemPtr<Data<K, V>>>,
    timestamp: AtomicU64,
    state: AtomicU32,
}

impl<K, V> RemConcurrentMapEntry<K, V>
where
    K: Hash + Eq,
{
    fn new() -> Self {
        Self {
            bitmap: 0,
            spin: SpinLock::new(),
            data: None,
            timestamp: AtomicU64::new(0),
            state: AtomicU32::new(EntryState::FREE),
        }
    }

    fn move_from(&mut self, other: &mut Self) {
        self.timestamp
            .store(other.timestamp.load(Ordering::Relaxed), Ordering::Relaxed);
        let st: EntryState = other.state.load(Ordering::Relaxed).into();
        debug_assert_ne!(st.state(), EntryState::FREE);
        self.state.store(st.into(), Ordering::Relaxed);
        debug_assert!(self.data.is_none());
        debug_assert!(other.data.is_some());
        self.data = Some(RemPtr::null());
        self.data
            .as_mut()
            .unwrap()
            .move_from(other.data.as_mut().unwrap());
        other.data = None;
    }

    fn timestamp(&self) -> u64 {
        self.timestamp.load(Ordering::Relaxed)
    }

    fn set_timestamp(&self, timestamp: u64) {
        self.timestamp.store(timestamp, Ordering::Relaxed);
    }

    fn cas_free(&mut self) {
        let old_state: EntryState = self.state.load(Ordering::Relaxed).into();
        debug_assert_eq!(old_state.state(), EntryState::BUSY);
        let new_state = EntryState::free_state();
        let res = self.state.compare_exchange(
            old_state.into(),
            new_state.into(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        debug_assert!(res.is_ok());
    }

    fn cas_free_to_busy(&mut self) -> bool {
        let free_state = EntryState::free_state();
        let busy_state = EntryState::busy_state();
        self.state
            .compare_exchange(
                free_state.into(),
                busy_state.into(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    fn cas_busy(&mut self) {
        let mut old_state: EntryState = self.state.load(Ordering::Relaxed).into();
        debug_assert_eq!(old_state.state(), EntryState::IMMUT);
        loop {
            if old_state.read_cnt() > 0 {
                libfibre_port::yield_now();
                old_state = self.state.load(Ordering::Relaxed).into();
                continue;
            }
            let new_state = EntryState::busy_state();
            match self.state.compare_exchange_weak(
                old_state.into(),
                new_state.into(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(state) => {
                    old_state = state.into();
                    continue;
                }
            }
        }
    }

    async fn cas_busy_async(&mut self) {
        let mut old_state: EntryState = self.state.load(Ordering::Relaxed).into();
        debug_assert_eq!(old_state.state(), EntryState::IMMUT);
        loop {
            if old_state.read_cnt() > 0 {
                crate::pararoutine::yield_now().await;
                old_state = self.state.load(Ordering::Relaxed).into();
                continue;
            }
            let new_state = EntryState::busy_state();
            match self.state.compare_exchange_weak(
                old_state.into(),
                new_state.into(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(state) => {
                    old_state = state.into();
                    continue;
                }
            }
        }
    }

    fn cas_immut(&mut self) {
        let old_state: EntryState = self.state.load(Ordering::Relaxed).into();
        debug_assert_eq!(old_state.state(), EntryState::BUSY);
        debug_assert_eq!(old_state.read_cnt(), 0);
        let new_state = EntryState::immut_state(0);
        let res = self.state.compare_exchange(
            old_state.into(),
            new_state.into(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        debug_assert!(res.is_ok());
    }

    fn write_lock(&mut self) {
        let mut old_state: EntryState = self.state.load(Ordering::Relaxed).into();
        loop {
            if old_state.read_cnt() > 0 {
                libfibre_port::yield_now();
                old_state = self.state.load(Ordering::Relaxed).into();
                continue;
            }
            debug_assert_eq!(old_state.state(), EntryState::IMMUT);
            let new_state = EntryState::write_state();
            match self.state.compare_exchange_weak(
                old_state.into(),
                new_state.into(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(state) => {
                    old_state = state.into();
                    continue;
                }
            }
        }
    }

    async fn write_lock_async(&mut self) {
        let mut old_state: EntryState = self.state.load(Ordering::Relaxed).into();
        loop {
            if old_state.read_cnt() > 0 {
                crate::pararoutine::yield_now().await;
                old_state = self.state.load(Ordering::Relaxed).into();
                continue;
            }
            debug_assert_eq!(old_state.state(), EntryState::IMMUT);
            let new_state = EntryState::write_state();
            match self.state.compare_exchange_weak(
                old_state.into(),
                new_state.into(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(state) => {
                    old_state = state.into();
                    continue;
                }
            }
        }
    }

    fn write_unlock(&mut self) {
        let old_state: EntryState = self.state.load(Ordering::Relaxed).into();
        debug_assert_eq!(old_state.state(), EntryState::WRITE);
        debug_assert_eq!(old_state.read_cnt(), 0);
        let new_state = EntryState::immut_state(0);
        let res = self.state.compare_exchange(
            old_state.into(),
            new_state.into(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        debug_assert!(res.is_ok());
    }

    fn read_lock(&mut self, bucket_lock: bool) -> bool {
        let mut old_state: EntryState = self.state.load(Ordering::Relaxed).into();
        loop {
            let mut new_state = old_state;
            debug_assert!(
                !bucket_lock
                    || new_state.state() == EntryState::FREE
                    || new_state.state() == EntryState::IMMUT
            );
            if new_state.state() == EntryState::FREE || new_state.state() == EntryState::BUSY {
                return false;
            }
            if !bucket_lock && new_state.state() == EntryState::WRITE {
                libfibre_port::yield_now();
                old_state = self.state.load(Ordering::Relaxed).into();
                continue;
            }
            new_state.inc_read_cnt();
            match self.state.compare_exchange_weak(
                old_state.into(),
                new_state.into(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(state) => {
                    old_state = state.into();
                    continue;
                }
            }
        }
    }

    async fn read_lock_async(&mut self, bucket_lock: bool) -> bool {
        let mut old_state: EntryState = self.state.load(Ordering::Relaxed).into();
        loop {
            let mut new_state = old_state;
            debug_assert!(
                !bucket_lock
                    || new_state.state() == EntryState::FREE
                    || new_state.state() == EntryState::IMMUT
            );
            if new_state.state() == EntryState::FREE || new_state.state() == EntryState::BUSY {
                return false;
            }
            if !bucket_lock && new_state.state() == EntryState::WRITE {
                crate::pararoutine::yield_now().await;
                old_state = self.state.load(Ordering::Relaxed).into();
                continue;
            }
            new_state.inc_read_cnt();
            match self.state.compare_exchange_weak(
                old_state.into(),
                new_state.into(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(state) => {
                    old_state = state.into();
                    continue;
                }
            }
        }
    }

    fn read_unlock(&mut self) {
        let mut old_state: EntryState = self.state.load(Ordering::Relaxed).into();
        loop {
            let mut new_state = old_state;
            debug_assert_eq!(new_state.state(), EntryState::IMMUT);
            debug_assert!(new_state.read_cnt() > 0);
            new_state.dec_read_cnt();
            match self.state.compare_exchange_weak(
                old_state.into(),
                new_state.into(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(state) => {
                    old_state = state.into();
                    continue;
                }
            }
        }
    }

    fn data_ref<'a>(
        &self,
        on_miss: &dyn OnMiss,
        scope: &dyn DerefScopeTrait,
    ) -> Option<RemRef<'a, Data<K, V>>> {
        self.data.as_ref().map(|data| deref(data, on_miss, scope))
    }

    fn data_ref_mut<'a>(
        &mut self,
        on_miss: &dyn OnMiss,
        scope: &dyn DerefScopeTrait,
    ) -> Option<RemRefMut<'a, Data<K, V>>> {
        self.data
            .as_mut()
            .map(|data| deref_mut(data, on_miss, scope))
    }

    async fn data_ref_async<'a>(
        &self,
        scope: &dyn DerefScopeTrait,
    ) -> Option<RemRef<'a, Data<K, V>>>
    where
        K: 'a,
        V: 'a,
    {
        match self.data.as_ref() {
            Some(data) => Some(deref_async(data, scope).await),
            None => None,
        }
    }

    async fn data_ref_mut_async<'a>(
        &mut self,
        scope: &dyn DerefScopeTrait,
    ) -> Option<RemRefMut<'a, Data<K, V>>>
    where
        K: 'a,
        V: 'a,
    {
        match self.data.as_mut() {
            Some(data) => Some(deref_mut_async(data, scope).await),
            None => None,
        }
    }

    fn is_free(&self) -> bool {
        let state: EntryState = self.state.load(Ordering::Relaxed).into();
        state.state() == EntryState::FREE
    }

    fn is_busy(&self) -> bool {
        let state: EntryState = self.state.load(Ordering::Relaxed).into();
        state.state() == EntryState::BUSY
    }

    fn is_locked(&self) -> bool {
        self.spin.is_locked()
    }
}

pub struct RemConcurrentMap<K, V, H = RandomState>
where
    K: Hash + Eq,
    H: BuildHasher + Default,
{
    entries: Box<[RemConcurrentMapEntry<K, V>]>,
    hash_mask: u32,
    num_entries: usize,
    size: AtomicUsize,
    hasher: H,
}

impl<K, V, H> RemConcurrentMap<K, V, H>
where
    K: Hash + Eq,
    H: BuildHasher + Default,
{
    const NEIGHBOR_COUNT: usize = 32;
    const MAX_RETRY: usize = 2;

    pub fn new(num_entries_shift: usize) -> Self {
        assert!(num_entries_shift < 32);
        let num_entries = (1 << num_entries_shift) + Self::NEIGHBOR_COUNT;
        Self {
            entries: (0..num_entries)
                .map(|_| RemConcurrentMapEntry::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            hash_mask: (1 << num_entries_shift) - 1,
            num_entries: num_entries,
            size: AtomicUsize::new(0),
            hasher: H::default(),
        }
    }

    pub fn get_hash(&self, key: &K) -> u32 {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        hasher.finish() as u32 & self.hash_mask
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn get<F>(
        &mut self,
        key: &K,
        mut read: F,
        on_miss: &dyn OnMiss,
        scope: &dyn DerefScopeTrait,
    ) -> bool
    where
        F: FnMut(&K, &V, &dyn DerefScopeTrait),
    {
        let hash = self.get_hash(key);
        let bucket_idx = hash as usize;

        let mut retry_count: usize = 0;

        let mut get_once = |lock: bool| -> (bool, bool) {
            let (mut bitmap, timestamp) = {
                let bucket_bitmap = {
                    if lock {
                        self.entry_lock_bitmap(bucket_idx, scope)
                    } else {
                        self.entries[bucket_idx].bitmap
                    }
                };
                let entries = &mut self.entries;
                let bucket = &mut entries[bucket_idx];
                (bucket_bitmap, bucket.timestamp.load(Ordering::Relaxed))
            };
            let mut res = false;
            while bitmap != 0 {
                let offset = bitmap.trailing_zeros();
                debug_assert!(offset < 32);
                let entry = &mut self.entries[bucket_idx + offset as usize];
                let entry_ptr = entry as *mut RemConcurrentMapEntry<K, V>;
                if let Some(data) = entry.data_ref(on_miss, scope) {
                    if data.key() == key {
                        let scope = scope.push(&data);
                        // bypass compiler check by using pointer
                        unsafe { entry_ptr.as_mut_unchecked().read_lock(lock) };
                        read(key, data.value(), &scope);
                        unsafe { entry_ptr.as_mut_unchecked().read_unlock() };
                        res = true;
                        break;
                    }
                }
                bitmap ^= 1 << offset;
            }
            if lock {
                self.entry_unlock(bucket_idx);
            }
            let bucket = &mut self.entries[bucket_idx];
            (res, bucket.timestamp() != timestamp)
        };
        // fast path
        loop {
            let (res, need_continue) = get_once(false);
            if res {
                return true;
            }
            retry_count += 1;
            if retry_count > Self::MAX_RETRY || !need_continue {
                break;
            }
        }
        // slow path
        get_once(true).0
    }

    #[inline]
    fn entry_lock_bitmap(&mut self, entry_idx: usize, scope: &dyn DerefScopeTrait) -> u32 {
        let entry = &mut self.entries[entry_idx];
        entry.spin.lock_with_scope(scope);
        entry.bitmap
    }

    #[inline]
    async fn entry_lock_bitmap_async(
        &mut self,
        entry_idx: usize,
        scope: &dyn DerefScopeTrait,
    ) -> u32 {
        let entry = &mut self.entries[entry_idx];
        entry.spin.lock_async_with_scope(scope).await;
        entry.bitmap
    }

    #[inline]
    fn entry_unlock(&mut self, entry_idx: usize) {
        let entry = &mut self.entries[entry_idx];
        entry.spin.unlock();
    }

    pub fn remove(&mut self, key: &K, on_miss: &dyn OnMiss, scope: &dyn DerefScopeTrait) -> bool {
        let hash = self.get_hash(key);
        let bucket_idx = hash as usize;
        let mut bitmap = self.entry_lock_bitmap(bucket_idx, scope);
        while bitmap != 0 {
            let offset = bitmap.trailing_zeros();
            debug_assert!(offset < 32);
            let removed = {
                let entry = &mut self.entries[bucket_idx + offset as usize];
                if let Some(data) = entry.data_ref(on_miss, scope) {
                    if data.key() == key {
                        entry.cas_busy();
                        entry.data = None;
                        entry.cas_free();
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            };
            if removed {
                let bucket = &mut self.entries[bucket_idx];
                debug_assert!(bucket.is_locked());
                debug_assert_ne!(bucket.bitmap & (1 << offset), 0);
                bucket.bitmap ^= 1 << offset;
                bucket.timestamp.fetch_add(1, Ordering::Relaxed);
                self.entry_unlock(bucket_idx);
                self.size.fetch_sub(1, Ordering::Relaxed);
                return true;
            }
            bitmap ^= 1 << offset;
        }
        self.entry_unlock(bucket_idx);
        return false;
    }

    pub fn insert(
        &mut self,
        key: K,
        value: V,
        on_miss: &dyn OnMiss,
        scope: &dyn DerefScopeTrait,
    ) -> bool {
        let hash = self.get_hash(&key);
        let mut bucket_idx = hash as usize;
        let orig_bucket_idx = bucket_idx;
        let mut bitmap = self.entry_lock_bitmap(orig_bucket_idx, scope);
        while bitmap != 0 {
            let offset = bitmap.trailing_zeros();
            debug_assert!(offset < 32);
            let entry = &mut self.entries[orig_bucket_idx + offset as usize];
            let entry_ptr = entry as *mut RemConcurrentMapEntry<K, V>;
            // 1.key exists, update
            if let Some(mut data) = entry.data_ref_mut(on_miss, scope) {
                if *data.key() == key {
                    unsafe { entry_ptr.as_mut_unchecked().write_lock() };
                    *data.value_mut() = value;
                    unsafe { entry_ptr.as_mut_unchecked().write_unlock() };
                    self.entry_unlock(orig_bucket_idx);
                    return true;
                }
            }
            bitmap ^= 1 << offset;
        }
        // 2. not exists, find empty slot to insert
        while bucket_idx < self.num_entries {
            if self.entries[bucket_idx].cas_free_to_busy() {
                break;
            }
            bucket_idx += 1;
        }
        // 3. buckets full, can not insert
        if bucket_idx == self.num_entries {
            self.entry_unlock(orig_bucket_idx);
            panic!("hashmap is full");
        }

        // 2.4 buckets not full, move this bucket to the neighborhood
        let mut distance_to_orig_bucket = bucket_idx - orig_bucket_idx;
        while distance_to_orig_bucket >= Self::NEIGHBOR_COUNT {
            // Try to see if we can move things backward.
            let mut distance = 0;
            for d in (1..=Self::NEIGHBOR_COUNT - 1).rev() {
                let idx = bucket_idx - d;
                {
                    let anchor_entry = &mut self.entries[idx];
                    if anchor_entry.bitmap == 0 {
                        continue;
                    }
                }
                // Lock and recheck bitmap.
                let bitmap = self.entry_lock_bitmap(idx, scope);
                if bitmap == 0 {
                    self.entry_unlock(idx);
                    continue;
                }
                // Get the offset of the first entry within the bucket.
                let offset = bitmap.trailing_zeros();
                debug_assert!(offset < 32);
                if idx + offset as usize >= bucket_idx {
                    self.entry_unlock(idx);
                    continue;
                }
                // Swap entry [closest_bucket + offset] and [bucket_idx]
                {
                    let (left, right) = self.entries.split_at_mut(bucket_idx);
                    let from_entry = &mut left[idx + offset as usize];
                    let to_entry = &mut right[0];
                    to_entry.move_from(from_entry);
                }
                {
                    let anchor_entry = &mut self.entries[idx];
                    debug_assert_eq!(anchor_entry.bitmap & (1 << d), 0);
                    anchor_entry.bitmap |= 1 << d;
                    anchor_entry.timestamp.fetch_add(1, Ordering::Relaxed);
                }
                {
                    let from_entry = &mut self.entries[idx + offset as usize];
                    from_entry.cas_busy();
                }
                {
                    let anchor_entry = &mut self.entries[idx];
                    debug_assert_ne!(anchor_entry.bitmap & (1 << offset), 0);
                    anchor_entry.bitmap ^= 1 << offset;
                }

                // Jump backward.
                bucket_idx = idx + offset as usize;
                self.entry_unlock(idx);
                distance = d;
                break;
            }
            if distance == 0 {
                self.entry_unlock(orig_bucket_idx);
                return false;
            }
            distance_to_orig_bucket = bucket_idx - orig_bucket_idx;
        }
        // Allocate memory.
        // final entry has been locked
        {
            let final_entry = &mut self.entries[bucket_idx];
            debug_assert!(final_entry.is_busy());
            self.size.fetch_add(1, Ordering::Relaxed);
            debug_assert!(final_entry.data.is_none());
            final_entry.data = Some(RemPtr::null());
            manager::allocate(
                final_entry.data.as_mut().unwrap(),
                Some(Data::new(key, value)),
                scope,
            );
        }
        {
            let bucket = &mut self.entries[orig_bucket_idx];
            debug_assert_eq!(bucket.bitmap & (1 << distance_to_orig_bucket), 0);
            bucket.bitmap |= 1 << distance_to_orig_bucket;
        }
        {
            let final_entry = &mut self.entries[bucket_idx];
            final_entry.cas_immut();
        }
        self.entry_unlock(orig_bucket_idx);
        return true;
    }

    // update a entry, if not exists, insert it, then update it.
    pub fn update<F>(
        &mut self,
        key: K,
        value: Option<V>,
        mut update: F,
        on_miss: &dyn OnMiss,
        scope: &dyn DerefScopeTrait,
    ) -> bool
    where
        F: FnMut(&K, &mut V, &dyn DerefScopeTrait),
    {
        let hash = self.get_hash(&key);
        let mut bucket_idx = hash as usize;
        let orig_bucket_idx = bucket_idx;
        let mut bitmap = self.entry_lock_bitmap(orig_bucket_idx, scope);
        while bitmap != 0 {
            let offset = bitmap.trailing_zeros();
            debug_assert!(offset < 32);
            let entry = &mut self.entries[orig_bucket_idx + offset as usize];
            let entry_ptr = entry as *mut RemConcurrentMapEntry<K, V>;
            // 1.key exists, update
            if let Some(mut data) = entry.data_ref_mut(on_miss, scope) {
                if *data.key() == key {
                    unsafe { entry_ptr.as_mut_unchecked().write_lock() };
                    // if exists, update
                    let scope = scope.push(&data);
                    update(&key, data.value_mut(), &scope);
                    unsafe { entry_ptr.as_mut_unchecked().write_unlock() };
                    self.entry_unlock(orig_bucket_idx);
                    return true;
                }
            }
            bitmap ^= 1 << offset;
        }
        if let None = value {
            self.entry_unlock(orig_bucket_idx);
            return false;
        }
        let value = value.unwrap();
        // 2. not exists, find empty slot to insert
        while bucket_idx < self.num_entries {
            if self.entries[bucket_idx].cas_free_to_busy() {
                break;
            }
            bucket_idx += 1;
        }
        // 3. buckets full, can not insert
        if bucket_idx == self.num_entries {
            self.entry_unlock(orig_bucket_idx);
            panic!("hashmap is full");
        }

        // 2.4 buckets not full, move this bucket to the neighborhood
        let mut distance_to_orig_bucket = bucket_idx - orig_bucket_idx;
        while distance_to_orig_bucket > Self::NEIGHBOR_COUNT {
            // Try to see if we can move things backward.
            let mut distance = 0;
            for d in (1..=Self::NEIGHBOR_COUNT - 1).rev() {
                let idx = bucket_idx - d;
                {
                    let anchor_entry = &mut self.entries[idx];
                    if anchor_entry.bitmap == 0 {
                        continue;
                    }
                }
                // Lock and recheck bitmap.
                let bitmap = self.entry_lock_bitmap(idx, scope);
                if bitmap == 0 {
                    self.entry_unlock(idx);
                    continue;
                }
                // Get the offset of the first entry within the bucket.
                let offset = bitmap.trailing_zeros();
                debug_assert!(offset < 32);
                if idx + offset as usize >= bucket_idx {
                    self.entry_unlock(idx);
                    continue;
                }
                // Swap entry [closest_bucket + offset] and [bucket_idx]
                {
                    let (left, right) = self.entries.split_at_mut(bucket_idx);
                    let from_entry = &mut left[idx + offset as usize];
                    let to_entry = &mut right[0];
                    to_entry.move_from(from_entry);
                }
                {
                    let anchor_entry = &mut self.entries[idx];
                    debug_assert_eq!(anchor_entry.bitmap & (1 << d), 0);
                    anchor_entry.bitmap |= 1 << d;
                    anchor_entry.timestamp.fetch_add(1, Ordering::Relaxed);
                }
                {
                    let from_entry = &mut self.entries[idx + offset as usize];
                    from_entry.cas_busy();
                }
                {
                    let anchor_entry = &mut self.entries[idx];
                    debug_assert_ne!(anchor_entry.bitmap & (1 << offset), 0);
                    anchor_entry.bitmap ^= 1 << offset;
                }

                // Jump backward.
                bucket_idx = idx + offset as usize;
                self.entry_unlock(idx);
                distance = d;
                break;
            }
            if distance == 0 {
                self.entry_unlock(orig_bucket_idx);
                return false;
            }
            distance_to_orig_bucket = bucket_idx - orig_bucket_idx;
        }
        // Allocate memory.
        // final entry has been locked
        {
            let final_entry = &mut self.entries[bucket_idx];
            debug_assert!(final_entry.is_busy());
            self.size.fetch_add(1, Ordering::Relaxed);
            final_entry.data = Some(RemPtr::null());
            let mut data = manager::allocate(
                final_entry.data.as_mut().unwrap(),
                Some(Data::new(key, value)),
                scope,
            );
            let scope = scope.push(&data);
            unsafe {
                // bypass compiler check by using pointer
                let k = data.key() as *const K;
                let v = data.value_mut() as *mut V;
                update(&*k, &mut *v, &scope);
            }
        }
        {
            let bucket = &mut self.entries[orig_bucket_idx];
            debug_assert_eq!(bucket.bitmap & (1 << distance_to_orig_bucket), 0);
            bucket.bitmap |= 1 << distance_to_orig_bucket;
        }
        {
            let final_entry = &mut self.entries[bucket_idx];
            final_entry.cas_immut();
        }
        self.entry_unlock(orig_bucket_idx);
        return true;
    }

    pub fn get_sync<F>(&mut self, key: &K, read: F, scope: &dyn DerefScopeTrait) -> bool
    where
        F: FnMut(&K, &V, &dyn DerefScopeTrait),
    {
        self.get(key, read, &|_| {}, scope)
    }

    pub fn insert_sync(&mut self, key: K, value: V, scope: &dyn DerefScopeTrait) -> bool {
        self.insert(key, value, &|_| {}, scope)
    }

    pub fn update_sync<F>(
        &mut self,
        key: K,
        value: Option<V>,
        update: F,
        scope: &dyn DerefScopeTrait,
    ) -> bool
    where
        F: FnMut(&K, &mut V, &dyn DerefScopeTrait),
    {
        self.update(key, value, update, &|_| {}, scope)
    }

    pub fn remove_sync(&mut self, key: &K, scope: &dyn DerefScopeTrait) -> bool {
        self.remove(key, &|_| {}, scope)
    }

    pub fn prefetch_key_bucket(&self, key: &K, scope: &dyn DerefScopeTrait) {
        let hash = self.get_hash(key);
        let bucket_idx = hash as usize;
        self.prefetch_bucket(bucket_idx, scope);
    }

    pub fn prefetch_key_bucket_mut(&mut self, key: &K, scope: &dyn DerefScopeTrait) {
        let hash = self.get_hash(key);
        let bucket_idx = hash as usize;
        self.prefetch_bucket_mut(bucket_idx, scope);
    }

    pub fn prefetch_bucket(&self, bucket_idx: usize, scope: &dyn DerefScopeTrait) {
        let entry = &self.entries[bucket_idx];
        let mut bitmap = entry.bitmap;
        while bitmap != 0 {
            let offset = bitmap.trailing_zeros();
            debug_assert!(offset < 32);
            let entry = &self.entries[bucket_idx + offset as usize];
            if let Some(data) = &entry.data {
                manager::prefetch(data, scope);
            }
            bitmap ^= 1 << offset;
        }
    }

    pub fn prefetch_bucket_mut(&mut self, bucket_idx: usize, scope: &dyn DerefScopeTrait) {
        let entry = &mut self.entries[bucket_idx];
        let mut bitmap = entry.bitmap;
        while bitmap != 0 {
            let offset = bitmap.trailing_zeros();
            debug_assert!(offset < 32);
            let entry = &mut self.entries[bucket_idx + offset as usize];
            if let Some(data) = &mut entry.data {
                manager::prefetch_mut(data, scope);
            }
            bitmap ^= 1 << offset;
        }
    }

    pub async fn get_async<F>(&mut self, key: &K, mut read: F, scope: &dyn DerefScopeTrait) -> bool
    where
        F: FnMut(&K, &V, &dyn DerefScopeTrait),
    {
        let hash = self.get_hash(key);
        let bucket_idx = hash as usize;

        let mut retry_count: usize = 0;

        let mut get_once = async |lock: bool| -> (bool, bool) {
            let (mut bitmap, timestamp) = {
                let bucket_bitmap = {
                    if lock {
                        self.entry_lock_bitmap_async(bucket_idx, scope).await
                    } else {
                        self.entries[bucket_idx].bitmap
                    }
                };
                let entries = &mut self.entries;
                let bucket = &mut entries[bucket_idx];
                (bucket_bitmap, bucket.timestamp.load(Ordering::Relaxed))
            };
            let mut res = false;
            while bitmap != 0 {
                let offset = bitmap.trailing_zeros();
                debug_assert!(offset < 32);
                let entry = &mut self.entries[bucket_idx + offset as usize];
                let entry_ptr = entry as *mut RemConcurrentMapEntry<K, V>;
                if let Some(data) = entry.data_ref_async(scope).await {
                    if data.key() == key {
                        let scope = scope.push(&data);
                        // bypass compiler check by using pointer
                        unsafe { entry_ptr.as_mut_unchecked().read_lock_async(lock).await };
                        read(key, data.value(), &scope);
                        unsafe { entry_ptr.as_mut_unchecked().read_unlock() };
                        res = true;
                        break;
                    }
                }
                bitmap ^= 1 << offset;
            }
            if lock {
                self.entry_unlock(bucket_idx);
            }
            let bucket = &mut self.entries[bucket_idx];
            (res, bucket.timestamp() != timestamp)
        };
        // fast path
        loop {
            let (res, need_continue) = get_once(false).await;
            if res {
                return true;
            }
            retry_count += 1;
            if retry_count > Self::MAX_RETRY || !need_continue {
                break;
            }
        }
        // slow path
        get_once(true).await.0
    }

    pub async fn remove_async(&mut self, key: &K, scope: &dyn DerefScopeTrait) -> bool {
        let hash = self.get_hash(key);
        let bucket_idx = hash as usize;
        let mut bitmap = self.entry_lock_bitmap_async(bucket_idx, scope).await;
        while bitmap != 0 {
            let offset = bitmap.trailing_zeros();
            debug_assert!(offset < 32);
            let removed = {
                let entry = &mut self.entries[bucket_idx + offset as usize];
                if let Some(data) = entry.data_ref_async(scope).await {
                    if data.key() == key {
                        entry.cas_busy_async().await;
                        entry.data = None;
                        entry.cas_free();
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            };
            if removed {
                let bucket = &mut self.entries[bucket_idx];
                debug_assert!(bucket.is_locked());
                debug_assert_ne!(bucket.bitmap & (1 << offset), 0);
                bucket.bitmap ^= 1 << offset;
                bucket.timestamp.fetch_add(1, Ordering::Relaxed);
                self.entry_unlock(bucket_idx);
                self.size.fetch_sub(1, Ordering::Relaxed);
                return true;
            }
            bitmap ^= 1 << offset;
        }
        self.entry_unlock(bucket_idx);
        return false;
    }

    pub async fn insert_async(&mut self, key: K, value: V, scope: &dyn DerefScopeTrait) -> bool {
        let hash = self.get_hash(&key);
        let mut bucket_idx = hash as usize;
        let orig_bucket_idx = bucket_idx;
        let mut bitmap = self.entry_lock_bitmap_async(orig_bucket_idx, scope).await;
        while bitmap != 0 {
            let offset = bitmap.trailing_zeros();
            debug_assert!(offset < 32);
            let entry = &mut self.entries[orig_bucket_idx + offset as usize];
            let entry_ptr = entry as *mut RemConcurrentMapEntry<K, V>;
            // 1.key exists, update
            if let Some(mut data) = entry.data_ref_mut_async(scope).await {
                if *data.key() == key {
                    unsafe { entry_ptr.as_mut_unchecked().write_lock_async().await };
                    *data.value_mut() = value;
                    unsafe { entry_ptr.as_mut_unchecked().write_unlock() };
                    self.entry_unlock(orig_bucket_idx);
                    return true;
                }
            }
            bitmap ^= 1 << offset;
        }
        // 2. not exists, find empty slot to insert
        while bucket_idx < self.num_entries {
            if self.entries[bucket_idx].cas_free_to_busy() {
                break;
            }
            bucket_idx += 1;
        }
        // 3. buckets full, can not insert
        if bucket_idx == self.num_entries {
            self.entry_unlock(orig_bucket_idx);
            panic!("hashmap is full");
        }

        // 2.4 buckets not full, move this bucket to the neighborhood
        let mut distance_to_orig_bucket = bucket_idx - orig_bucket_idx;
        while distance_to_orig_bucket >= Self::NEIGHBOR_COUNT {
            // Try to see if we can move things backward.
            let mut distance = 0;
            for d in (1..=Self::NEIGHBOR_COUNT - 1).rev() {
                let idx = bucket_idx - d;
                {
                    let anchor_entry = &mut self.entries[idx];
                    if anchor_entry.bitmap == 0 {
                        continue;
                    }
                }
                // Lock and recheck bitmap.
                let bitmap = self.entry_lock_bitmap_async(idx, scope).await;
                if bitmap == 0 {
                    self.entry_unlock(idx);
                    continue;
                }
                // Get the offset of the first entry within the bucket.
                let offset = bitmap.trailing_zeros();
                debug_assert!(offset < 32);
                if idx + offset as usize >= bucket_idx {
                    self.entry_unlock(idx);
                    continue;
                }
                // Swap entry [closest_bucket + offset] and [bucket_idx]
                {
                    let (left, right) = self.entries.split_at_mut(bucket_idx);
                    let from_entry = &mut left[idx + offset as usize];
                    let to_entry = &mut right[0];
                    to_entry.move_from(from_entry);
                }
                {
                    let anchor_entry = &mut self.entries[idx];
                    debug_assert_eq!(anchor_entry.bitmap & (1 << d), 0);
                    anchor_entry.bitmap |= 1 << d;
                    anchor_entry.timestamp.fetch_add(1, Ordering::Relaxed);
                }
                {
                    let from_entry = &mut self.entries[idx + offset as usize];
                    from_entry.cas_busy_async().await;
                }
                {
                    let anchor_entry = &mut self.entries[idx];
                    debug_assert_ne!(anchor_entry.bitmap & (1 << offset), 0);
                    anchor_entry.bitmap ^= 1 << offset;
                }

                // Jump backward.
                bucket_idx = idx + offset as usize;
                self.entry_unlock(idx);
                distance = d;
                break;
            }
            if distance == 0 {
                self.entry_unlock(orig_bucket_idx);
                return false;
            }
            distance_to_orig_bucket = bucket_idx - orig_bucket_idx;
        }
        // Allocate memory.
        // final entry has been locked
        {
            let final_entry = &mut self.entries[bucket_idx];
            debug_assert!(final_entry.is_busy());
            self.size.fetch_add(1, Ordering::Relaxed);
            debug_assert!(final_entry.data.is_none());
            final_entry.data = Some(RemPtr::null());
            manager::allocate(
                final_entry.data.as_mut().unwrap(),
                Some(Data::new(key, value)),
                scope,
            );
        }
        {
            let bucket = &mut self.entries[orig_bucket_idx];
            debug_assert_eq!(bucket.bitmap & (1 << distance_to_orig_bucket), 0);
            bucket.bitmap |= 1 << distance_to_orig_bucket;
        }
        {
            let final_entry = &mut self.entries[bucket_idx];
            final_entry.cas_immut();
        }
        self.entry_unlock(orig_bucket_idx);
        return true;
    }

    pub async fn update_async<F>(
        &mut self,
        key: K,
        value: Option<V>,
        mut update: F,
        scope: &dyn DerefScopeTrait,
    ) -> bool
    where
        F: FnMut(&K, &mut V, &dyn DerefScopeTrait),
    {
        let hash = self.get_hash(&key);
        let mut bucket_idx = hash as usize;
        let orig_bucket_idx = bucket_idx;
        let mut bitmap = self.entry_lock_bitmap_async(orig_bucket_idx, scope).await;
        while bitmap != 0 {
            let offset = bitmap.trailing_zeros();
            debug_assert!(offset < 32);
            let entry = &mut self.entries[orig_bucket_idx + offset as usize];
            let entry_ptr = entry as *mut RemConcurrentMapEntry<K, V>;
            // 1.key exists, update
            if let Some(mut data) = entry.data_ref_mut_async(scope).await {
                if *data.key() == key {
                    unsafe { entry_ptr.as_mut_unchecked().write_lock_async().await };
                    // if exists, update
                    let scope = scope.push(&data);
                    update(&key, data.value_mut(), &scope);
                    unsafe { entry_ptr.as_mut_unchecked().write_unlock() };
                    self.entry_unlock(orig_bucket_idx);
                    return true;
                }
            }
            bitmap ^= 1 << offset;
        }
        if let None = value {
            self.entry_unlock(orig_bucket_idx);
            return false;
        }
        let value = value.unwrap();
        // 2. not exists, find empty slot to insert
        while bucket_idx < self.num_entries {
            if self.entries[bucket_idx].cas_free_to_busy() {
                break;
            }
            bucket_idx += 1;
        }
        // 3. buckets full, can not insert
        if bucket_idx == self.num_entries {
            self.entry_unlock(orig_bucket_idx);
            panic!("hashmap is full");
        }

        // 2.4 buckets not full, move this bucket to the neighborhood
        let mut distance_to_orig_bucket = bucket_idx - orig_bucket_idx;
        while distance_to_orig_bucket > Self::NEIGHBOR_COUNT {
            // Try to see if we can move things backward.
            let mut distance = 0;
            for d in (1..=Self::NEIGHBOR_COUNT - 1).rev() {
                let idx = bucket_idx - d;
                {
                    let anchor_entry = &mut self.entries[idx];
                    if anchor_entry.bitmap == 0 {
                        continue;
                    }
                }
                // Lock and recheck bitmap.
                let bitmap = self.entry_lock_bitmap_async(idx, scope).await;
                if bitmap == 0 {
                    self.entry_unlock(idx);
                    continue;
                }
                // Get the offset of the first entry within the bucket.
                let offset = bitmap.trailing_zeros();
                debug_assert!(offset < 32);
                if idx + offset as usize >= bucket_idx {
                    self.entry_unlock(idx);
                    continue;
                }
                // Swap entry [closest_bucket + offset] and [bucket_idx]
                {
                    let (left, right) = self.entries.split_at_mut(bucket_idx);
                    let from_entry = &mut left[idx + offset as usize];
                    let to_entry = &mut right[0];
                    to_entry.move_from(from_entry);
                }
                {
                    let anchor_entry = &mut self.entries[idx];
                    debug_assert_eq!(anchor_entry.bitmap & (1 << d), 0);
                    anchor_entry.bitmap |= 1 << d;
                    anchor_entry.timestamp.fetch_add(1, Ordering::Relaxed);
                }
                {
                    let from_entry = &mut self.entries[idx + offset as usize];
                    from_entry.cas_busy_async().await;
                }
                {
                    let anchor_entry = &mut self.entries[idx];
                    debug_assert_ne!(anchor_entry.bitmap & (1 << offset), 0);
                    anchor_entry.bitmap ^= 1 << offset;
                }

                // Jump backward.
                bucket_idx = idx + offset as usize;
                self.entry_unlock(idx);
                distance = d;
                break;
            }
            if distance == 0 {
                self.entry_unlock(orig_bucket_idx);
                return false;
            }
            distance_to_orig_bucket = bucket_idx - orig_bucket_idx;
        }
        // Allocate memory.
        // final entry has been locked
        {
            let final_entry = &mut self.entries[bucket_idx];
            debug_assert!(final_entry.is_busy());
            self.size.fetch_add(1, Ordering::Relaxed);
            final_entry.data = Some(RemPtr::null());
            let mut data = manager::allocate(
                final_entry.data.as_mut().unwrap(),
                Some(Data::new(key, value)),
                scope,
            );
            let scope = scope.push(&data);
            unsafe {
                // bypass compiler check by using pointer
                let k = data.key() as *const K;
                let v = data.value_mut() as *mut V;
                update(&*k, &mut *v, &scope);
            }
        }
        {
            let bucket = &mut self.entries[orig_bucket_idx];
            debug_assert_eq!(bucket.bitmap & (1 << distance_to_orig_bucket), 0);
            bucket.bitmap |= 1 << distance_to_orig_bucket;
        }
        {
            let final_entry = &mut self.entries[bucket_idx];
            final_entry.cas_immut();
        }
        self.entry_unlock(orig_bucket_idx);
        return true;
    }
}
