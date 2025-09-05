use std::cell::Cell;
use std::sync::atomic::Ordering;

use std::sync::Arc;
use tracing::trace;

use crate::turso_assert;

use super::pager::PageRef;

/// FIXME: https://github.com/tursodatabase/turso/issues/1661
const DEFAULT_PAGE_CACHE_SIZE_IN_PAGES_MAKE_ME_SMALLER_ONCE_WAL_SPILL_IS_IMPLEMENTED: usize =
    100000;

#[derive(Debug, Copy, Eq, Hash, PartialEq, Clone)]
#[repr(transparent)]
pub struct PageCacheKey(usize);

const NULL: usize = usize::MAX;

#[derive(Clone, Copy, Debug)]
/// To distinguish between a page touched by a scan vs a 'hot' page,
/// we use a Reference Bit with 4 states, incrementing this bit up to a maximum
/// of 3 on each access, and decrementing on each eviction scan pass.
enum RefBit {
    Clear,
    Min,
    Med,
    Max,
}

#[derive(Clone, Debug)]
struct PageCacheEntry {
    /// Key identifying this page
    key: PageCacheKey,
    /// The cached page, None if this slot is free
    page: Option<PageRef>,
    /// Reference bit for SIEVE algorithm - set on access, cleared during eviction scan
    ref_bit: Cell<RefBit>,
    /// Index of next entry in SIEVE queue (newer/toward head)
    next: Cell<usize>,
    /// Index of previous entry in SIEVE queue (older/toward tail)
    prev: Cell<usize>,
}

impl Default for PageCacheEntry {
    fn default() -> Self {
        Self {
            key: PageCacheKey(0),
            page: None,
            ref_bit: Cell::new(RefBit::Clear),
            next: Cell::new(NULL),
            prev: Cell::new(NULL),
        }
    }
}

impl PageCacheEntry {
    #[inline]
    fn bump_ref(&self) {
        self.ref_bit.set(match self.ref_bit.get() {
            RefBit::Clear => RefBit::Min,
            RefBit::Min => RefBit::Med,
            _ => RefBit::Max,
        });
    }

    #[inline]
    /// Returns the old value
    fn decrement_ref(&self) -> RefBit {
        let old = self.ref_bit.get();
        let new = match old {
            RefBit::Max => RefBit::Med,
            RefBit::Med => RefBit::Min,
            _ => RefBit::Clear,
        };
        self.ref_bit.set(new);
        old
    }
    #[inline]
    fn clear_ref(&self) {
        self.ref_bit.set(RefBit::Clear);
    }
    #[inline]
    fn empty() -> Self {
        Self::default()
    }
    #[inline]
    fn reset_links(&self) {
        self.next.set(NULL);
        self.prev.set(NULL);
    }
}

/// PageCache implements a variation of the SIEVE algorithm that maintains an intrusive linked list queue of
/// pages which keep a 'reference_bit' to determine how recently/frequently the page has been accessed.
/// The bit is set to `Clear` on initial insertion and then bumped on each access and decremented
/// during eviction scans.
///
/// - New pages enter at the head (MRU position)
/// - Eviction candidates are examined from the tail (LRU position)
/// - Each page has a reference bit that is set when accessed
/// - During eviction, if a page's reference bit is decremented
///   and the page moves to the head, Pages with Clear reference
///   bits are evicted immediately
pub struct PageCache {
    /// Capacity in pages
    capacity: usize,
    /// Map of Key -> usize in entries array
    map: PageHashMap,
    /// Pointers to intrusive doubly-linked list for eviction order
    head: Cell<usize>,
    tail: Cell<usize>,
    clock_hand: Cell<usize>,
    /// Fixed-size vec holding page entries
    entries: Vec<PageCacheEntry>,
    /// Free list: Stack of available slot indices
    freelist: Vec<usize>,
}

unsafe impl Send for PageCache {}
unsafe impl Sync for PageCache {}

struct PageHashMap {
    buckets: Vec<Vec<HashMapNode>>,
    capacity: usize,
    size: usize,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CacheError {
    #[error("{0}")]
    InternalError(String),
    #[error("page {pgno} is locked")]
    Locked { pgno: usize },
    #[error("page {pgno} is dirty")]
    Dirty { pgno: usize },
    #[error("page {pgno} is pinned")]
    Pinned { pgno: usize },
    #[error("cache active refs")]
    ActiveRefs,
    #[error("Page cache is full")]
    Full,
    #[error("key already exists")]
    KeyExists,
}

#[derive(Debug, PartialEq)]
pub enum CacheResizeResult {
    Done,
    PendingEvictions,
}

impl PageCacheKey {
    pub fn new(pgno: usize) -> Self {
        Self(pgno)
    }
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        let freelist = (0..capacity).rev().collect::<Vec<usize>>();
        Self {
            capacity,
            map: PageHashMap::new(capacity),
            head: Cell::new(NULL),
            tail: Cell::new(NULL),
            clock_hand: Cell::new(NULL),
            entries: vec![PageCacheEntry::empty(); capacity],
            freelist,
        }
    }

    #[inline]
    fn link_at_head(&self, slot: usize) {
        let old_head = self.head.replace(slot);
        self.entries[slot].next.set(old_head);
        self.entries[slot].prev.set(NULL);

        if old_head != NULL {
            self.entries[old_head].prev.set(slot);
        } else {
            // List was empty, this is now both head and tail
            self.tail.set(slot);
        }
        // If hand was NULL/list was empty, set it to the new element
        if self.clock_hand.get() == NULL {
            self.clock_hand.set(slot);
        }
    }

    #[inline]
    fn unlink(&self, slot: usize) {
        let p = self.entries[slot].prev.get();
        let n = self.entries[slot].next.get();

        if p != NULL {
            self.entries[p].next.set(n);
        } else {
            self.head.set(n);
        }
        if n != NULL {
            self.entries[n].prev.set(p);
        } else {
            self.tail.set(p);
        }

        self.entries[slot].reset_links();
    }

    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        self.map.contains_key(key)
    }

    #[inline]
    pub fn insert(&mut self, key: PageCacheKey, value: PageRef) -> Result<(), CacheError> {
        self._insert(key, value, false)
    }

    #[inline]
    pub fn upsert_page(&mut self, key: PageCacheKey, value: PageRef) -> Result<(), CacheError> {
        self._insert(key, value, true)
    }

    pub fn _insert(
        &mut self,
        key: PageCacheKey,
        value: PageRef,
        update_in_place: bool,
    ) -> Result<(), CacheError> {
        trace!("insert(key={:?})", key);
        let slot = { self.map.get(&key) };
        if let Some(slot) = slot {
            let p = self.entries[slot]
                .page
                .as_ref()
                .expect("slot must have a page");

            if !p.is_loaded() && !p.is_locked() {
                // evict, then continue with fresh insert
                self._delete(key, true)?;
                let slot_index = self.find_free_slot()?;
                let entry = &mut self.entries[slot_index];
                entry.key = key;
                entry.page = Some(value);
                entry.clear_ref();
                self.map.insert(key, slot_index);
                self.link_at_head(slot_index);
                return Ok(());
            }

            let existing = &mut self.entries[slot];
            existing.bump_ref();
            if update_in_place {
                existing.page = Some(value);
                return Ok(());
            } else {
                turso_assert!(
                    Arc::ptr_eq(existing.page.as_ref().unwrap(), &value),
                    "Attempted to insert different page with same key: {key:?}"
                );
                return Err(CacheError::KeyExists);
            }
        }
        // Key doesn't exist, proceed with new entry
        self.make_room_for(1)?;
        let slot_index = self.find_free_slot()?;
        let entry = &mut self.entries[slot_index];
        turso_assert!(entry.page.is_none(), "page must be None in free slot");
        entry.key = key;
        entry.page = Some(value);
        // Sieve ref bit starts cleared, will be set on first access
        entry.clear_ref();
        self.map.insert(key, slot_index);
        self.link_at_head(slot_index);
        Ok(())
    }

    fn find_free_slot(&mut self) -> Result<usize, CacheError> {
        let slot = self.freelist.pop().ok_or_else(|| {
            CacheError::InternalError("No free slots available after make_room_for".into())
        })?;
        #[cfg(debug_assertions)]
        {
            turso_assert!(
                self.entries[slot].page.is_none(),
                "allocating non-free slot {}",
                slot
            );
        }
        // Reset linkage on entry itself
        self.entries[slot].reset_links();
        Ok(slot)
    }

    fn _delete(&mut self, key: PageCacheKey, clean_page: bool) -> Result<(), CacheError> {
        if !self.contains_key(&key) {
            return Ok(());
        }

        let slot_idx = self
            .map
            .get(&key)
            .ok_or_else(|| CacheError::InternalError("Key exists but not found in map".into()))?;
        let entry = self.entries[slot_idx]
            .page
            .as_ref()
            .expect("page in map was None")
            .clone();

        if entry.is_locked() {
            return Err(CacheError::Locked {
                pgno: entry.get().id,
            });
        }
        if entry.is_dirty() {
            return Err(CacheError::Dirty {
                pgno: entry.get().id,
            });
        }
        if entry.is_pinned() {
            return Err(CacheError::Pinned {
                pgno: entry.get().id,
            });
        }

        if clean_page {
            entry.clear_loaded();
            let _ = entry.get().contents.take();
        }

        self.unlink(slot_idx);
        self.map.remove(&key);
        let entry = &mut self.entries[slot_idx];
        entry.page = None;
        entry.clear_ref();
        entry.reset_links();
        self.freelist.push(slot_idx);
        Ok(())
    }

    #[inline]
    /// Deletes a page from the cache
    pub fn delete(&mut self, key: PageCacheKey) -> Result<(), CacheError> {
        trace!("cache_delete(key={:?})", key);
        self._delete(key, true)
    }

    #[inline]
    pub fn get(&mut self, key: &PageCacheKey) -> crate::Result<Option<PageRef>> {
        let Some(slot) = self.map.get(key) else {
            return Ok(None);
        };
        // Because we can abort a read_page completion, this means a page can be in the cache but be unloaded and unlocked.
        // However, if we do not evict that page from the page cache, we will return an unloaded page later which will trigger
        // assertions later on. This is worsened by the fact that page cache is not per `Statement`, so you can abort a completion
        // in one Statement, and trigger some error in the next one if we don't evict the page here.
        let entry = &self.entries[slot];
        entry.bump_ref();
        let page = entry
            .page
            .as_ref()
            .expect("page in the map to exist")
            .clone();
        if !page.is_loaded() && !page.is_locked() {
            self.delete(*key)?;
            Ok(None)
        } else {
            Ok(Some(page))
        }
    }

    #[inline]
    pub fn peek(&self, key: &PageCacheKey, touch: bool) -> Option<PageRef> {
        let slot = self.map.get(key)?;
        let entry = &self.entries[slot];
        let page = entry.page.as_ref()?.clone();
        if touch {
            // set reference bit to 'touch' page
            entry.bump_ref();
        }
        Some(page)
    }

    /// Resizes the cache to a new capacity
    ///
    /// If shrinking, attempts to evict pages using the SIEVE algorithm.
    /// If growing, simply increases capacity.
    pub fn resize(&mut self, new_cap: usize) -> CacheResizeResult {
        if new_cap == self.capacity {
            return CacheResizeResult::Done;
        }
        if new_cap < self.len() {
            let need = self.len() - new_cap;
            // repeat SIEVE passes until we evict `need` pages or give up
            let mut evicted = 0;
            while evicted < need {
                match self.make_room_for(1) {
                    Ok(()) => evicted += 1,
                    Err(CacheError::Full) => return CacheResizeResult::PendingEvictions,
                    Err(_) => return CacheResizeResult::PendingEvictions,
                }
            }
        }
        assert!(new_cap > 0);
        // Collect survivors as payload, no linkage
        struct Payload {
            key: PageCacheKey,
            page: PageRef,
            ref_bit: RefBit,
        }
        let survivors: Vec<Payload> = {
            let entries = &self.entries;
            let mut v = Vec::with_capacity(self.len());
            // walk tail..head to preserve recency when re-linking via link_front
            let mut cur = self.tail.get();
            while cur != NULL {
                let e = &entries[cur];
                if let Some(ref p) = e.page {
                    v.push(Payload {
                        key: e.key,
                        page: p.clone(),
                        ref_bit: e.ref_bit.get(),
                    });
                }
                cur = entries[cur].prev.get();
            }
            v
        };

        // Resize entry array; reset heads
        self.entries.resize(new_cap, PageCacheEntry::empty());
        self.capacity = new_cap;
        let mut new_map = PageHashMap::new(new_cap);
        self.head.set(NULL);
        self.tail.set(NULL);

        // Repack compactly: survivors[tail..head] pushed to front -> final order == original
        for (slot, pl) in survivors.iter().enumerate().take(new_cap) {
            let e = &mut self.entries[slot];
            e.key = pl.key;
            e.page = Some(pl.page.clone());
            e.ref_bit.set(pl.ref_bit);
            e.reset_links();
            new_map.insert(pl.key, slot);
        }
        for slot in 0..survivors.len().min(new_cap) {
            self.link_at_head(slot);
        }
        self.map = new_map;

        // Rebuild freelist
        let used = survivors.len().min(new_cap);
        let fl = &mut self.freelist;
        fl.clear();
        for i in (used..new_cap).rev() {
            fl.push(i);
        }

        CacheResizeResult::Done
    }

    /// Ensures at least `n` free slots are available
    ///
    /// Uses the SIEVE algorithm to evict pages if necessary:
    /// 1. Start at tail (LRU position)
    /// 2. If page is marked, decrement mark and move to head
    /// 3. If page mark was already Cleared, evict it
    /// 4. If page is unevictable (dirty/locked/pinned), move to head
    ///
    /// Returns `CacheError::Full` if not enough pages can be evicted
    pub fn make_room_for(&mut self, n: usize) -> Result<(), CacheError> {
        if n > self.capacity {
            return Err(CacheError::Full);
        }

        let available = self.capacity.saturating_sub(self.len());
        if n <= available {
            return Ok(());
        }

        let mut need = n - available;
        let mut examined = 0;
        // Max ref bit value + 1
        let max_examinations = self.len() * 4;

        // Start from where we left off, or from tail if hand is invalid
        let mut current = self.clock_hand.get();
        if current == NULL || self.entries[current].page.is_none() {
            current = self.tail.get();
        }
        let start_position = current;
        let mut wrapped = false;
        while need > 0 && examined < max_examinations {
            if current == NULL {
                break;
            }
            let entry = &self.entries[current];
            let next = entry.next.get();

            if let Some(ref page) = entry.page {
                if !page.is_dirty() && !page.is_locked() && !page.is_pinned() {
                    if matches!(entry.ref_bit.get(), RefBit::Clear) {
                        // Evict this page
                        let key = entry.key;
                        self.clock_hand
                            .set(if next != NULL { next } else { self.tail.get() });
                        self._delete(key, true)?;
                        need -= 1;
                        examined = 0;
                        // After deletion, current is invalid, use hand
                        current = self.clock_hand.get();
                        continue;
                    } else {
                        // Decrement and continue
                        entry.decrement_ref();
                        examined += 1;
                    }
                } else {
                    examined += 1;
                }
            }
            // Move to next
            if next != NULL {
                current = next;
            } else if !wrapped {
                // Wrap around to tail once
                current = self.tail.get();
                wrapped = true;
            } else {
                // We've wrapped and hit the end again
                break;
            }
            // Stop if we've come full circle
            if wrapped && current == start_position {
                break;
            }
        }
        self.clock_hand.set(current);
        if need > 0 {
            return Err(CacheError::Full);
        }
        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), CacheError> {
        for e in self.entries.iter() {
            if let Some(ref p) = e.page {
                if p.is_dirty() {
                    return Err(CacheError::Dirty { pgno: p.get().id });
                }
                p.clear_loaded();
                let _ = p.get().contents.take();
            }
        }
        self.entries.fill(PageCacheEntry::empty());
        self.map.clear();
        self.head.set(NULL);
        self.tail.set(NULL);
        let fl = &mut self.freelist;
        fl.clear();
        for i in (0..self.capacity).rev() {
            fl.push(i);
        }
        Ok(())
    }

    /// Removes all pages from the cache with pgno greater than len
    pub fn truncate(&mut self, len: usize) -> Result<(), CacheError> {
        let keys_to_delete: Vec<PageCacheKey> = {
            self.entries
                .iter()
                .filter_map(|entry| {
                    entry.page.as_ref().and({
                        if entry.key.0 > len {
                            Some(entry.key)
                        } else {
                            None
                        }
                    })
                })
                .collect()
        };
        for key in keys_to_delete.iter() {
            self.delete(*key)?;
        }
        Ok(())
    }

    pub fn print(&self) {
        tracing::debug!("page_cache_len={}", self.map.len());
        let entries = &self.entries;

        for (i, entry_opt) in entries.iter().enumerate() {
            if let Some(ref page) = entry_opt.page {
                tracing::debug!(
                    "slot={}, page={:?}, flags={}, pin_count={}, ref_bit={:?}",
                    i,
                    entry_opt.key,
                    page.get().flags.load(Ordering::Relaxed),
                    page.get().pin_count.load(Ordering::Relaxed),
                    entry_opt.ref_bit.get(),
                );
            }
        }
    }

    #[cfg(test)]
    pub fn keys(&mut self) -> Vec<PageCacheKey> {
        let mut keys = Vec::with_capacity(self.len());
        let entries = &self.entries;
        for entry in entries.iter() {
            if entry.page.is_none() {
                continue;
            }
            keys.push(entry.key);
        }
        keys
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn unset_dirty_all_pages(&mut self) {
        let entries = &self.entries;
        for entry in entries.iter() {
            if entry.page.is_none() {
                continue;
            }
            entry.page.as_ref().unwrap().clear_dirty();
        }
    }

    #[cfg(test)]
    fn verify_cache_integrity(&self) {
        let entries = &self.entries;
        let map = &self.map;

        let head = self.head.get();
        let tail = self.tail.get();

        // Head/tail base constraints
        if head == NULL {
            assert_eq!(tail, NULL, "tail must be NULL when head is NULL");
            assert_eq!(map.len(), 0, "map not empty but list is empty");
        } else {
            assert_eq!(entries[head].prev.get(), NULL, "head.prev must be NULL");
        }
        if tail != NULL {
            assert_eq!(entries[tail].next.get(), NULL, "tail.next must be NULL");
        }

        // 0 = unseen, 1 = freelist, 2 = sieve
        let mut seen = vec![0u8; self.capacity];

        // Walk SIEVE forward from head, check links and detect cycles
        let mut cnt = 0usize;
        let mut cur = head;
        let mut prev = NULL;
        let mut hops = 0usize;
        while cur != NULL {
            hops += 1;
            assert!(hops <= self.capacity, "SIEVE cycle detected");
            let e = &entries[cur];

            assert!(e.page.is_some(), "SIEVE points to empty slot {cur}");
            assert_eq!(e.prev.get(), prev, "prev link broken at slot {cur}");
            assert_eq!(seen[cur], 0, "slot {cur} appears twice (SIEVE/freelist)");

            seen[cur] = 2;
            cnt += 1;
            prev = cur;
            cur = e.next.get();
        }
        assert_eq!(tail, prev, "tail mismatch");
        assert_eq!(
            cnt,
            map.len(),
            "list length {} != map size {}",
            cnt,
            map.len()
        );

        // Map bijection: every map entry must be on the SIEVE list with matching key
        for node in map.iter() {
            let slot = node.slot_index;
            assert!(
                entries[slot].page.is_some(),
                "map points to empty slot {cur}",
            );
            assert_eq!(
                entries[slot].key, node.key,
                "map key mismatch at slot {cur}",
            );
            assert_eq!(seen[slot], 2, "map slot {slot} not on SIEVE list");
        }

        // Freelist disjointness and shape: free slots must be unlinked and empty
        let freelist = &self.freelist;
        let mut free_count = 0usize;
        for &s in freelist.iter() {
            free_count += 1;
            assert_eq!(seen[s], 0, "slot {s} in both freelist and SIEVE");
            assert!(entries[s].page.is_none(), "freelist slot {s} has a page");
            assert_eq!(
                entries[s].next.get(),
                NULL,
                "freelist slot {s} next != NULL",
            );
            assert_eq!(
                entries[s].prev.get(),
                NULL,
                "freelist slot {s} prev != NULL",
            );
            seen[s] = 1;
        }

        // No orphans; partition covers capacity
        let orphans = seen.iter().filter(|&&v| v == 0).count();
        assert_eq!(orphans, 0, "orphan slots detected: {orphans}");
        assert_eq!(
            free_count + cnt,
            self.capacity,
            "free {} + sieve {} != capacity {}",
            free_count,
            cnt,
            self.capacity
        );
    }

    #[cfg(test)]
    fn slot_of(&self, key: &PageCacheKey) -> Option<usize> {
        self.map.get(key)
    }
    #[cfg(test)]
    fn ref_of(&self, key: &PageCacheKey) -> Option<RefBit> {
        self.slot_of(key).map(|i| self.entries[i].ref_bit.get())
    }
}

impl Default for PageCache {
    fn default() -> Self {
        PageCache::new(
            DEFAULT_PAGE_CACHE_SIZE_IN_PAGES_MAKE_ME_SMALLER_ONCE_WAL_SPILL_IS_IMPLEMENTED,
        )
    }
}

#[derive(Clone)]
struct HashMapNode {
    key: PageCacheKey,
    slot_index: usize,
}

#[allow(dead_code)]
impl PageHashMap {
    pub fn new(capacity: usize) -> PageHashMap {
        PageHashMap {
            buckets: vec![vec![]; capacity],
            capacity,
            size: 0,
        }
    }

    pub fn insert(&mut self, key: PageCacheKey, slot_index: usize) {
        let bucket = self.hash(&key);
        let bucket = &mut self.buckets[bucket];
        let mut idx = 0;
        while let Some(node) = bucket.get_mut(idx) {
            if node.key == key {
                node.slot_index = slot_index;
                node.key = key;
                return;
            }
            idx += 1;
        }
        bucket.push(HashMapNode { key, slot_index });
        self.size += 1;
    }

    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        let bucket = self.hash(key);
        self.buckets[bucket].iter().any(|node| node.key == *key)
    }

    pub fn get(&self, key: &PageCacheKey) -> Option<usize> {
        let bucket = self.hash(key);
        let bucket = &self.buckets[bucket];
        for node in bucket {
            if node.key == *key {
                return Some(node.slot_index);
            }
        }
        None
    }

    pub fn remove(&mut self, key: &PageCacheKey) -> Option<usize> {
        let bucket = self.hash(key);
        let bucket = &mut self.buckets[bucket];
        let mut idx = 0;
        while let Some(node) = bucket.get(idx) {
            if node.key == *key {
                break;
            }
            idx += 1;
        }
        if idx == bucket.len() {
            None
        } else {
            let v = bucket.remove(idx);
            self.size -= 1;
            Some(v.slot_index)
        }
    }

    pub fn clear(&mut self) {
        for bucket in &mut self.buckets {
            bucket.clear();
        }
        self.size = 0;
    }

    pub fn len(&self) -> usize {
        self.size
    }

    fn iter(&self) -> impl Iterator<Item = &HashMapNode> {
        self.buckets.iter().flat_map(|b| b.iter())
    }

    fn hash(&self, key: &PageCacheKey) -> usize {
        if self.capacity.is_power_of_two() {
            key.0 & (self.capacity - 1)
        } else {
            key.0 % self.capacity
        }
    }

    fn rehash(&self, new_capacity: usize) -> PageHashMap {
        let mut new_hash_map = PageHashMap::new(new_capacity);
        for node in self.iter() {
            new_hash_map.insert(node.key, node.slot_index);
        }
        new_hash_map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page_cache::CacheError;
    use crate::storage::pager::{Page, PageRef};
    use crate::storage::sqlite3_ondisk::PageContent;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };
    use std::sync::Arc;

    fn create_key(id: usize) -> PageCacheKey {
        PageCacheKey::new(id)
    }

    pub fn page_with_content(page_id: usize) -> PageRef {
        let page = Arc::new(Page::new(page_id));
        {
            let buffer = crate::Buffer::new_temporary(4096);
            let page_content = PageContent {
                offset: 0,
                buffer: Arc::new(buffer),
                overflow_cells: Vec::new(),
            };
            page.get().contents = Some(page_content);
            page.set_loaded();
        }
        page
    }

    fn insert_page(cache: &mut PageCache, id: usize) -> PageCacheKey {
        let key = create_key(id);
        let page = page_with_content(id);
        assert!(cache.insert(key, page).is_ok());
        key
    }

    #[test]
    fn test_delete_only_element() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 1);

        assert!(cache.delete(key1).is_ok());

        assert_eq!(
            cache.len(),
            0,
            "Length should be 0 after deleting only element"
        );
        assert!(
            !cache.contains_key(&key1),
            "Cache should not contain key after delete"
        );
        assert_eq!(cache.head.get(), NULL, "Head should be NULL when empty");
        assert_eq!(cache.tail.get(), NULL, "Tail should be NULL when empty");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_detach_tail() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1); // tail
        let key2 = insert_page(&mut cache, 2); // middle
        let key3 = insert_page(&mut cache, 3); // head
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 3);

        // Verify initial tail (key1 should be at tail)
        let tail_slot = cache.tail.get();
        assert_ne!(tail_slot, NULL, "Tail should not be NULL");
        assert_eq!(
            cache.entries[tail_slot].key, key1,
            "Initial tail should be key1"
        );

        // Delete tail
        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.len(), 2, "Length should be 2 after deleting tail");
        assert!(
            !cache.contains_key(&key1),
            "Cache should not contain deleted tail key"
        );
        cache.verify_cache_integrity();

        // Check new tail is key2 (next oldest)
        let new_tail_slot = cache.tail.get();
        assert_ne!(new_tail_slot, NULL, "New tail should not be NULL");
        let entries = &cache.entries;
        assert_eq!(entries[new_tail_slot].key, key2, "New tail should be key2");
        assert_eq!(
            entries[new_tail_slot].next.get(),
            NULL,
            "New tail's next should be NULL"
        );

        // Verify head is key3
        let head_slot = cache.head.get();
        assert_ne!(head_slot, NULL, "Head should not be NULL");
        assert_eq!(entries[head_slot].key, key3, "Head should be key3");
        assert_eq!(
            entries[head_slot].prev.get(),
            NULL,
            "Head's prev should be NULL"
        );
    }

    #[test]
    fn test_detach_middle() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1); // Will be tail
        let key2 = insert_page(&mut cache, 2); // Will be middle
        let key3 = insert_page(&mut cache, 3); // Will be middle
        let key4 = insert_page(&mut cache, 4); // Will be head
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 4);

        // Verify initial state
        let tail_slot = cache.tail.get();
        let head_slot = cache.head.get();
        assert_eq!(cache.entries[tail_slot].key, key1, "Initial tail check");
        assert_eq!(cache.entries[head_slot].key, key4, "Initial head check");

        // Delete middle element (key2)
        assert!(cache.delete(key2).is_ok());
        assert_eq!(cache.len(), 3, "Length should be 3 after deleting middle");
        assert!(
            !cache.contains_key(&key2),
            "Cache should not contain deleted middle key2"
        );
        cache.verify_cache_integrity();

        // Verify head and tail keys remain the same
        let new_head_slot = cache.head.get();
        let new_tail_slot = cache.tail.get();
        let entries = &cache.entries;
        assert_eq!(
            entries[new_head_slot].key, key4,
            "Head should still be key4"
        );
        assert_eq!(
            entries[new_tail_slot].key, key1,
            "Tail should still be key1"
        );

        // Check that key3 and key1 are now properly linked
        let key3_slot = cache.map.get(&key3).unwrap();
        let key1_slot = cache.map.get(&key1).unwrap();

        // key3 should be between head(key4) and tail(key1)
        assert_eq!(
            entries[key3_slot].prev.get(),
            new_head_slot,
            "Key3's prev should point to head (key4)"
        );
        assert_eq!(
            entries[key3_slot].next.get(),
            key1_slot,
            "Key3's next should point to tail (key1)"
        );
    }

    #[test]
    fn test_insert_existing_key_updates_in_place() {
        let mut cache = PageCache::default();
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page1_v1.clone(); // Same Arc instance

        assert!(cache.insert(key1, page1_v1.clone()).is_ok());
        assert_eq!(cache.len(), 1);

        // Inserting same page instance should return KeyExists error
        let result = cache.insert(key1, page1_v2.clone());
        assert_eq!(result, Err(CacheError::KeyExists));
        assert_eq!(cache.len(), 1);

        // Verify the page is still accessible
        assert!(cache.get(&key1).unwrap().is_some());
        cache.verify_cache_integrity();
    }

    #[test]
    #[should_panic(expected = "Attempted to insert different page with same key")]
    fn test_insert_different_page_same_key_panics() {
        let mut cache = PageCache::default();
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page_with_content(1); // Different Arc instance

        assert!(cache.insert(key1, page1_v1.clone()).is_ok());
        assert_eq!(cache.len(), 1);
        cache.verify_cache_integrity();

        // This should panic because it's a different page instance
        let _ = cache.insert(key1, page1_v2.clone());
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut cache = PageCache::default();
        let key_nonexist = create_key(99);

        // Deleting non-existent key should be a no-op (returns Ok)
        assert!(cache.delete(key_nonexist).is_ok());
        assert_eq!(cache.len(), 0);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_evict() {
        let mut cache = PageCache::new(1);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // With capacity=1, inserting key2 should evict key1
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "key1 should be evicted"
        );

        // key2 should still be accessible
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "capacity=1 should have evicted the older page"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_sieve_touch_non_tail_does_not_affect_immediate_eviction() {
        // SIEVE algorithm: touching a non-tail page marks it but doesn't move it.
        // The tail (if unmarked) will still be the first eviction candidate.

        // Insert 1,2,3 -> order [3,2,1] with tail=1
        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Touch key2 (middle) to mark it with reference bit
        assert!(cache.get(&key2).unwrap().is_some());

        // Insert 4: SIEVE examines tail (key1, unmarked) -> evict key1
        let key4 = insert_page(&mut cache, 4);

        assert!(
            cache.get(&key2).unwrap().is_some(),
            "marked non-tail (key2) should remain"
        );
        assert!(cache.get(&key3).unwrap().is_some(), "key3 should remain");
        assert!(
            cache.get(&key4).unwrap().is_some(),
            "key4 was just inserted"
        );
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "unmarked tail (key1) should be evicted first"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_sieve_second_chance_preserves_marked_tail() {
        // SIEVE gives a "second chance" to marked pages at the tail
        // by clearing their bit and moving them to head

        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        assert_eq!(cache.len(), 3);

        // Mark the tail (key1) by accessing it
        assert!(cache.get(&key1).unwrap().is_some());

        // Insert 4: SIEVE process:
        // 1. Examines tail (key1, marked) -> clear bit, move to head
        // 2. New tail is key2 (unmarked) -> evict key2
        let key4 = insert_page(&mut cache, 4);

        assert!(
            cache.get(&key1).unwrap().is_some(),
            "key1 had ref bit set, got second chance"
        );
        assert!(cache.get(&key3).unwrap().is_some(), "key3 should remain");
        assert!(cache.get(&key4).unwrap().is_some(), "key4 just inserted");
        assert!(
            cache.get(&key2).unwrap().is_none(),
            "key2 became new tail after key1's second chance and was evicted"
        );
        assert_eq!(cache.len(), 3);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_locked_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).unwrap().unwrap();
        page.set_locked();

        assert_eq!(cache.delete(key), Err(CacheError::Locked { pgno: 1 }));
        assert_eq!(cache.len(), 1, "Locked page should not be deleted");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_dirty_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).unwrap().unwrap();
        page.set_dirty();

        assert_eq!(cache.delete(key), Err(CacheError::Dirty { pgno: 1 }));
        assert_eq!(cache.len(), 1, "Dirty page should not be deleted");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_pinned_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).unwrap().unwrap();
        page.pin();

        assert_eq!(cache.delete(key), Err(CacheError::Pinned { pgno: 1 }));
        assert_eq!(cache.len(), 1, "Pinned page should not be deleted");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_make_room_for_with_dirty_pages() {
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Make both pages dirty (unevictable)
        cache.get(&key1).unwrap().unwrap().set_dirty();
        cache.get(&key2).unwrap().unwrap().set_dirty();

        // Try to insert a third page, should fail because can't evict dirty pages
        let key3 = create_key(3);
        let page3 = page_with_content(3);
        let result = cache.insert(key3, page3);

        assert_eq!(result, Err(CacheError::Full));
        assert_eq!(cache.len(), 2);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_insert_and_get() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        assert_eq!(cache.get(&key1).unwrap().unwrap().get().id, 1);
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_over_capacity() {
        // Test SIEVE eviction when exceeding capacity
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Insert 3: tail (key1, unmarked) should be evicted
        let key3 = insert_page(&mut cache, 3);

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&key2).unwrap().is_some(), "key2 should remain");
        assert!(cache.get(&key3).unwrap().is_some(), "key3 just inserted");
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "key1 (oldest, unmarked) should be evicted"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_delete() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);

        assert!(cache.delete(key1).is_ok());
        assert!(cache.get(&key1).unwrap().is_none());
        assert_eq!(cache.len(), 0);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_clear() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        assert!(cache.clear().is_ok());
        assert!(cache.get(&key1).unwrap().is_none());
        assert!(cache.get(&key2).unwrap().is_none());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.head.get(), NULL);
        assert_eq!(cache.tail.get(), NULL);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_resize_smaller_success() {
        let mut cache = PageCache::default();
        for i in 1..=5 {
            let _ = insert_page(&mut cache, i);
        }
        assert_eq!(cache.len(), 5);

        let result = cache.resize(3);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity(), 3);

        // Should still be able to insert after resize
        assert!(cache.insert(create_key(6), page_with_content(6)).is_ok());
        assert_eq!(cache.len(), 3); // One was evicted to make room
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_detach_with_multiple_pages() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Verify initial ordering (head=key3, tail=key1)
        let head_slot = cache.head.get();
        let tail_slot = cache.tail.get();
        assert_eq!(cache.entries[head_slot].key, key3, "Head should be key3");
        assert_eq!(cache.entries[tail_slot].key, key1, "Tail should be key1");

        // Delete middle element (key2)
        assert!(cache.delete(key2).is_ok());

        // Verify structure after deletion
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains_key(&key2));

        // Head and tail keys should remain the same
        let new_head_slot = cache.head.get();
        let new_tail_slot = cache.tail.get();
        let entries = &cache.entries;
        assert_eq!(
            entries[new_head_slot].key, key3,
            "Head should still be key3"
        );
        assert_eq!(
            entries[new_tail_slot].key, key1,
            "Tail should still be key1"
        );

        // Check direct linkage between head and tail
        assert_eq!(
            entries[new_head_slot].next.get(),
            new_tail_slot,
            "Head's next should point directly to tail after middle deleted"
        );
        assert_eq!(
            entries[new_tail_slot].prev.get(),
            new_head_slot,
            "Tail's prev should point directly to head after middle deleted"
        );

        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_multiple_elements() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 3);

        // Delete head (key3)
        assert!(cache.delete(key3).is_ok());
        assert_eq!(cache.len(), 2, "Length should be 2 after deleting head");
        assert!(
            !cache.contains_key(&key3),
            "Cache should not contain deleted head key"
        );
        cache.verify_cache_integrity();

        // Check new head is key2
        let head_slot = cache.head.get();
        assert_eq!(
            cache.entries[head_slot].key, key2,
            "New head should be key2"
        );
        assert_eq!(
            cache.entries[head_slot].prev.get(),
            NULL,
            "New head's prev should be NULL"
        );

        // Delete tail (key1)
        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.len(), 1, "Length should be 1 after deleting two");
        cache.verify_cache_integrity();

        // Delete last element (key2)
        assert!(cache.delete(key2).is_ok());
        assert_eq!(cache.len(), 0, "Length should be 0 after deleting all");
        assert_eq!(cache.head.get(), NULL, "Head should be NULL when empty");
        assert_eq!(cache.tail.get(), NULL, "Tail should be NULL when empty");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_resize_larger() {
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.len(), 2);

        let result = cache.resize(5);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.capacity(), 5);

        // Existing pages should still be accessible
        assert!(cache.get(&key1).is_ok_and(|p| p.is_some()));
        assert!(cache.get(&key2).is_ok_and(|p| p.is_some()));

        // Now we should be able to add 3 more without eviction
        for i in 3..=5 {
            let _ = insert_page(&mut cache, i);
        }
        assert_eq!(cache.len(), 5);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_resize_same_capacity() {
        let mut cache = PageCache::new(3);
        for i in 1..=3 {
            let _ = insert_page(&mut cache, i);
        }

        let result = cache.resize(3);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity(), 3);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_truncate_page_cache() {
        let mut cache = PageCache::new(10);
        let _ = insert_page(&mut cache, 1);
        let _ = insert_page(&mut cache, 4);
        let _ = insert_page(&mut cache, 8);
        let _ = insert_page(&mut cache, 10);

        // Truncate to keep only pages <= 4
        cache.truncate(4).unwrap();

        assert!(cache.contains_key(&PageCacheKey(1)));
        assert!(cache.contains_key(&PageCacheKey(4)));
        assert!(!cache.contains_key(&PageCacheKey(8)));
        assert!(!cache.contains_key(&PageCacheKey(10)));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.capacity(), 10);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_truncate_page_cache_remove_all() {
        let mut cache = PageCache::new(10);
        let _ = insert_page(&mut cache, 8);
        let _ = insert_page(&mut cache, 10);

        // Truncate to 4 (removes all pages since they're > 4)
        cache.truncate(4).unwrap();

        assert!(!cache.contains_key(&PageCacheKey(8)));
        assert!(!cache.contains_key(&PageCacheKey(10)));
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.capacity(), 10);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_fuzz() {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        tracing::info!("fuzz test seed: {}", seed);

        let max_pages = 10;
        let mut cache = PageCache::new(10);
        let mut reference_map = std::collections::HashMap::new();

        for _ in 0..10000 {
            cache.print();

            match rng.next_u64() % 2 {
                0 => {
                    // Insert operation
                    let id_page = rng.next_u64() % max_pages;
                    let key = PageCacheKey::new(id_page as usize);
                    #[allow(clippy::arc_with_non_send_sync)]
                    let page = Arc::new(Page::new(id_page as usize));

                    if cache.peek(&key, false).is_some() {
                        continue; // Skip duplicate page ids
                    }

                    tracing::debug!("inserting page {:?}", key);
                    match cache.insert(key, page.clone()) {
                        Err(CacheError::Full | CacheError::ActiveRefs) => {} // Expected, ignore
                        Err(err) => {
                            panic!("Cache insertion failed unexpectedly: {err:?}");
                        }
                        Ok(_) => {
                            reference_map.insert(key, page);
                            // Clean up reference_map if cache evicted something
                            if cache.len() < reference_map.len() {
                                reference_map.retain(|k, _| cache.contains_key(k));
                            }
                        }
                    }
                    assert!(cache.len() <= 10, "Cache size exceeded capacity");
                }
                1 => {
                    // Delete operation
                    let random = rng.next_u64() % 2 == 0;
                    let key = if random || reference_map.is_empty() {
                        let id_page: u64 = rng.next_u64() % max_pages;
                        PageCacheKey::new(id_page as usize)
                    } else {
                        let i = rng.next_u64() as usize % reference_map.len();
                        *reference_map.keys().nth(i).unwrap()
                    };

                    tracing::debug!("removing page {:?}", key);
                    reference_map.remove(&key);
                    assert!(cache.delete(key).is_ok());
                }
                _ => unreachable!(),
            }

            cache.verify_cache_integrity();

            // Verify all pages in reference_map are in cache
            for (key, page) in &reference_map {
                let cached_page = cache.peek(key, false).expect("Page should be in cache");
                assert_eq!(cached_page.get().id, key.0);
                assert_eq!(page.get().id, key.0);
            }
        }
    }

    #[test]
    fn test_peek_without_touch() {
        // Test that peek with touch=false doesn't mark pages
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Peek key1 without touching (no ref bit set)
        assert!(cache.peek(&key1, false).is_some());

        // Insert 3: should evict unmarked tail (key1)
        let key3 = insert_page(&mut cache, 3);

        assert!(cache.get(&key2).unwrap().is_some(), "key2 should remain");
        assert!(
            cache.get(&key3).unwrap().is_some(),
            "key3 was just inserted"
        );
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "key1 should be evicted since peek(false) didn't mark it"
        );
        assert_eq!(cache.len(), 2);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_peek_with_touch() {
        // Test that peek with touch=true marks pages for SIEVE
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Peek key1 WITH touching (sets ref bit)
        assert!(cache.peek(&key1, true).is_some());

        // Insert 3: key1 is marked, so it gets second chance
        // key2 becomes new tail and gets evicted
        let key3 = insert_page(&mut cache, 3);

        assert!(
            cache.get(&key1).unwrap().is_some(),
            "key1 should survive (was marked)"
        );
        assert!(
            cache.get(&key3).unwrap().is_some(),
            "key3 was just inserted"
        );
        assert!(
            cache.get(&key2).unwrap().is_none(),
            "key2 should be evicted after key1's second chance"
        );
        assert_eq!(cache.len(), 2);
        cache.verify_cache_integrity();
    }

    #[test]
    #[ignore = "long running test, remove ignore to verify memory stability"]
    fn test_clear_memory_stability() {
        let initial_memory = memory_stats::memory_stats().unwrap().physical_mem;

        for _ in 0..100000 {
            let mut cache = PageCache::new(1000);

            for i in 0..1000 {
                let key = create_key(i);
                let page = page_with_content(i);
                cache.insert(key, page).unwrap();
            }

            cache.clear().unwrap();
            drop(cache);
        }

        let final_memory = memory_stats::memory_stats().unwrap().physical_mem;
        let growth = final_memory.saturating_sub(initial_memory);

        println!("Memory growth: {growth} bytes");
        assert!(
            growth < 10_000_000,
            "Memory grew by {growth} bytes over test cycles (limit: 10MB)",
        );
    }

    #[test]
    fn gclock_hot_survives_scan_pages() {
        let mut c = PageCache::new(4);
        let _k1 = insert_page(&mut c, 1);
        let k2 = insert_page(&mut c, 2);
        let _k3 = insert_page(&mut c, 3);
        let _k4 = insert_page(&mut c, 4);

        // Make k2 truly hot: three real touches
        for _ in 0..3 {
            assert!(c.get(&k2).unwrap().is_some());
        }
        assert!(matches!(c.ref_of(&k2), Some(RefBit::Max)));

        // Now simulate a scan inserting new pages 5..10 (one-hit wonders).
        for id in 5..=10 {
            let _ = insert_page(&mut c, id);
        }

        // Hot k2 should still be present; most single-hit scan pages should churn.
        assert!(
            c.get(&k2).unwrap().is_some(),
            "hot page should survive scan"
        );
        // The earliest single-hit page should be gone.
        assert!(c.get(&create_key(5)).unwrap().is_none());
        c.verify_cache_integrity();
    }

    #[test]
    fn resize_preserves_ref_and_recency() {
        let mut c = PageCache::new(4);
        let _k1 = insert_page(&mut c, 1);
        let k2 = insert_page(&mut c, 2);
        let _k3 = insert_page(&mut c, 3);
        let _k4 = insert_page(&mut c, 4);
        // Make k2 hot.
        for _ in 0..3 {
            assert!(c.get(&k2).unwrap().is_some());
        }
        let _r_before = c.ref_of(&k2);

        // Shrink to 3 (one page will be evicted during repack/next insert)
        assert_eq!(c.resize(3), CacheResizeResult::Done);
        assert!(matches!(c.ref_of(&k2), _r_before));

        // Force an eviction; hot k2 should survive more passes.
        let _ = insert_page(&mut c, 5);
        assert!(c.get(&k2).unwrap().is_some());
        c.verify_cache_integrity();
    }
}
