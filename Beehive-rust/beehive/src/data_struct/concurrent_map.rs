use crate::mem::DerefScopeTrait;
use crate::utils::bitfield::*;
use crate::utils::spinlock::SpinLock;
use core::panic;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

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

struct ConcurrentMapEntry<K, V>
where
    K: Hash + Eq,
{
    bitmap: u32,
    spin: SpinLock,
    data: Option<(K, V)>,
    timestamp: AtomicU64,
    state: AtomicU32,
}

impl<K, V> ConcurrentMapEntry<K, V>
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
        self.data = other.data.take();
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

    fn key(&self) -> Option<&K> {
        self.data.as_ref().map(|(k, _)| k)
    }

    fn value(&self) -> Option<&V> {
        self.data.as_ref().map(|(_, v)| v)
    }

    fn value_mut(&mut self) -> Option<&mut V> {
        self.data.as_mut().map(|(_, v)| v)
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

pub struct ConcurrentMap<K, V, H = RandomState>
where
    K: Hash + Eq,
    H: BuildHasher + Default,
{
    entries: Box<[ConcurrentMapEntry<K, V>]>,
    hash_mask: u32,
    num_entries: usize,
    size: AtomicUsize,
    hasher: H,
}

impl<K, V, H> ConcurrentMap<K, V, H>
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
                .map(|_| ConcurrentMapEntry::new())
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

    pub fn get<F>(&mut self, key: &K, read: F) -> bool
    where
        F: FnMut(&K, &V),
    {
        self.get_impl(key, read, None)
    }

    pub fn get_with_scope<F>(&mut self, key: &K, read: F, scope: &dyn DerefScopeTrait) -> bool
    where
        F: FnMut(&K, &V),
    {
        self.get_impl(key, read, Some(scope))
    }

    fn get_impl<F>(&mut self, key: &K, mut read: F, scope: Option<&dyn DerefScopeTrait>) -> bool
    where
        F: FnMut(&K, &V),
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
                if entry.key() == Some(key) {
                    entry.read_lock(lock);
                    read(key, entry.value().unwrap());
                    entry.read_unlock();
                    res = true;
                    break;
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
    fn entry_lock_bitmap(&mut self, entry_idx: usize, scope: Option<&dyn DerefScopeTrait>) -> u32 {
        let entry = &mut self.entries[entry_idx];
        if let Some(scope) = scope {
            entry.spin.lock_with_scope(scope);
        } else {
            entry.spin.lock();
        }
        entry.bitmap
    }

    #[inline]
    fn entry_unlock(&mut self, entry_idx: usize) {
        let entry = &mut self.entries[entry_idx];
        entry.spin.unlock();
    }

    fn remove_impl(&mut self, key: &K, scope: Option<&dyn DerefScopeTrait>) -> bool {
        let hash = self.get_hash(key);
        let bucket_idx = hash as usize;
        let mut bitmap = self.entry_lock_bitmap(bucket_idx, scope);
        while bitmap != 0 {
            let offset = bitmap.trailing_zeros();
            debug_assert!(offset < 32);
            let removed = {
                let entry = &mut self.entries[bucket_idx + offset as usize];
                if entry.key() == Some(key) {
                    entry.cas_busy();
                    entry.data = None;
                    entry.cas_free();
                    true
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

    pub fn remove(&mut self, key: &K) -> bool {
        self.remove_impl(key, None)
    }

    pub fn remove_with_scope(&mut self, key: &K, scope: &dyn DerefScopeTrait) -> bool {
        self.remove_impl(key, Some(scope))
    }

    fn insert_impl(&mut self, key: K, value: V, scope: Option<&dyn DerefScopeTrait>) -> bool {
        let hash = self.get_hash(&key);
        let mut bucket_idx = hash as usize;
        let orig_bucket_idx = bucket_idx;
        let mut bitmap = self.entry_lock_bitmap(orig_bucket_idx, scope);
        while bitmap != 0 {
            let offset = bitmap.trailing_zeros();
            debug_assert!(offset < 32);
            let entry = &mut self.entries[orig_bucket_idx + offset as usize];
            // 1.key exists, update
            if entry.key() == Some(&key) {
                entry.write_lock();
                entry.data = Some((key, value));
                entry.write_unlock();
                self.entry_unlock(orig_bucket_idx);
                return true;
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
            final_entry.data = Some((key, value));
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

    pub fn insert(&mut self, key: K, value: V) -> bool {
        self.insert_impl(key, value, None)
    }

    pub fn insert_with_scope(&mut self, key: K, value: V, scope: &dyn DerefScopeTrait) -> bool {
        self.insert_impl(key, value, Some(scope))
    }
    // update a entry, if not exists, insert it, then update it.
    fn update_impl<F>(
        &mut self,
        key: K,
        value: Option<V>,
        mut update: F,
        scope: Option<&dyn DerefScopeTrait>,
    ) -> bool
    where
        F: FnMut(&K, &mut V),
    {
        let hash = self.get_hash(&key);
        let mut bucket_idx = hash as usize;
        let orig_bucket_idx = bucket_idx;
        let mut bitmap = self.entry_lock_bitmap(orig_bucket_idx, scope);
        while bitmap != 0 {
            let offset = bitmap.trailing_zeros();
            debug_assert!(offset < 32);
            let entry = &mut self.entries[orig_bucket_idx + offset as usize];
            // 1.key exists, update
            if entry.key() == Some(&key) {
                entry.write_lock();
                // if exists, update
                if let Some(v) = entry.value_mut() {
                    update(&key, v);
                } else {
                    // if not exists, panic!
                    // because it must exists in this if branch.
                    panic!("hashmap update: key not exists at bucket");
                }
                entry.write_unlock();
                self.entry_unlock(orig_bucket_idx);
                return true;
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
            final_entry.data = Some((key, value));
            if let Some((k, v)) = &mut final_entry.data {
                update(k, v);
            } else {
                panic!("hashmap update: key not exists at final entry");
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

    pub fn update<F>(&mut self, key: K, value: Option<V>, update: F) -> bool
    where
        F: FnMut(&K, &mut V),
    {
        self.update_impl(key, value, update, None)
    }

    pub fn update_with_scope<F>(
        &mut self,
        key: K,
        value: Option<V>,
        update: F,
        scope: &dyn DerefScopeTrait,
    ) -> bool
    where
        F: FnMut(&K, &mut V),
    {
        self.update_impl(key, value, update, Some(scope))
    }
}
