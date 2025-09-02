use std::cell::Cell;
use std::cell::RefCell;
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

type SlotIndex = usize;

const NULL: SlotIndex = usize::MAX;

#[derive(Clone, Debug)]
struct PageCacheEntry {
    /// Key identifying this page
    key: PageCacheKey,
    /// The cached page, None if this slot is free
    page: Option<PageRef>,
    /// Reference bit for SIEVE algorithm - set on access, cleared during eviction scan
    ref_bit: Cell<bool>,
    /// Index of next entry in SIEVE queue (newer/toward head)
    next: Cell<SlotIndex>,
    /// Index of previous entry in SIEVE queue (older/toward tail)
    prev: Cell<SlotIndex>,
}

impl Default for PageCacheEntry {
    fn default() -> Self {
        Self {
            key: PageCacheKey(0),
            page: None,
            ref_bit: Cell::new(false),
            next: Cell::new(NULL),
            prev: Cell::new(NULL),
        }
    }
}

impl PageCacheEntry {
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

/// PageCache implements the SIEVE eviction algorithm, a simpler and more efficient
/// alternative to LRU that achieves comparable or better hit ratios with lower overhead.
///
/// # Algorithm Overview
///
/// SIEVE maintains a queue of cached pages and uses a "second chance" mechanism:
/// - New pages enter at the head (MRU position)
/// - Eviction candidates are examined from the tail (LRU position)
/// - Each page has a reference bit that is set when accessed
/// - During eviction, if a page's reference bit is set, it gets a "second chance":
///   the bit is cleared and the page moves to the head
/// - Pages with clear reference bits are evicted immediately
pub struct PageCache {
    /// Capacity in pages
    capacity: usize,
    /// Map of Key -> SlotIndex in entries array
    map: RefCell<PageHashMap>,
    /// Pointers to intrusive doubly-linked list for eviction order (head=MRU, tail=LRU)
    head: Cell<SlotIndex>,
    tail: Cell<SlotIndex>,
    /// Fixed-size vec holding page entries
    entries: RefCell<Vec<PageCacheEntry>>,
    /// Free list: Stack of available slot indices
    freelist: RefCell<Vec<SlotIndex>>,
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
            map: RefCell::new(PageHashMap::new(capacity)),
            head: Cell::new(NULL),
            tail: Cell::new(NULL),
            entries: RefCell::new(vec![PageCacheEntry::empty(); capacity]),
            freelist: RefCell::new(freelist),
        }
    }

    #[inline]
    fn link_front(&self, slot: SlotIndex) {
        let entries = self.entries.borrow();
        let old_head = self.head.replace(slot);

        entries[slot].next.set(old_head);
        entries[slot].prev.set(NULL);

        if old_head != NULL {
            entries[old_head].prev.set(slot);
        } else {
            // list was empty
            self.tail.set(slot);
        }
    }

    #[inline]
    fn unlink(&self, slot: SlotIndex) {
        let entries = self.entries.borrow();

        let p = entries[slot].prev.get();
        let n = entries[slot].next.get();

        if p != NULL {
            entries[p].next.set(n);
        } else {
            self.head.set(n);
        }
        if n != NULL {
            entries[n].prev.set(p);
        } else {
            self.tail.set(p);
        }

        entries[slot].reset_links();
    }

    #[inline]
    fn move_to_front(&self, slot: SlotIndex) {
        if self.head.get() == slot {
            return;
        }
        turso_assert!(self.entries.borrow()[slot].page.is_some(), "must be linked");
        self.unlink(slot);
        self.link_front(slot);
    }

    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        self.map.borrow().contains_key(key)
    }

    #[inline]
    pub fn insert(&mut self, key: PageCacheKey, value: PageRef) -> Result<(), CacheError> {
        self._insert(key, value, false)
    }

    #[inline]
    pub fn insert_ignore_existing(
        &mut self,
        key: PageCacheKey,
        value: PageRef,
    ) -> Result<(), CacheError> {
        self._insert(key, value, true)
    }

    pub fn _insert(
        &mut self,
        key: PageCacheKey,
        value: PageRef,
        ignore_exists: bool,
    ) -> Result<(), CacheError> {
        trace!("insert(key={:?})", key);
        // Check first if page already exists in cache
        if let Some(slot) = self.map.borrow().get(&key) {
            if !ignore_exists {
                let existing = self.entries.borrow()[slot]
                    .page
                    .as_ref()
                    .expect("map points to empty slot")
                    .clone();
                turso_assert!(
                    Arc::ptr_eq(&existing, &value),
                    "Attempted to insert different page with same key: {key:?}"
                );
                return Err(CacheError::KeyExists);
            }
        }
        // Key doesn't exist, proceed with new entry
        self.make_room_for(1)?;
        let slot_index = self.find_free_slot()?;
        {
            let mut entries = self.entries.borrow_mut();
            let s = &mut entries[slot_index];
            turso_assert!(s.page.is_none(), "page must be None in free slot");
            s.key = key;
            s.page = Some(value);
            // Sieve ref bit starts cleared, will be set on first access
            s.ref_bit.set(false);
        }
        self.map.borrow_mut().insert(key, slot_index);
        self.link_front(slot_index);
        Ok(())
    }

    fn find_free_slot(&self) -> Result<usize, CacheError> {
        let slot = self.freelist.borrow_mut().pop().ok_or_else(|| {
            CacheError::InternalError("No free slots available after make_room_for".into())
        })?;
        #[cfg(debug_assertions)]
        {
            let entries = self.entries.borrow();
            turso_assert!(
                entries[slot].page.is_none(),
                "allocating non-free slot {}",
                slot
            );
        }
        // Reset linkage on entry itself
        self.entries.borrow()[slot].reset_links();
        Ok(slot)
    }

    fn _delete(&mut self, key: PageCacheKey, clean_page: bool) -> Result<(), CacheError> {
        if !self.contains_key(&key) {
            return Ok(());
        }

        let (entry, slot_idx) = {
            let map = self.map.borrow();
            let idx = map.get(&key).ok_or_else(|| {
                CacheError::InternalError("Key exists but not found in map".into())
            })?;
            (
                self.entries.borrow()[idx]
                    .page
                    .as_ref()
                    .expect("page in map was None")
                    .clone(),
                idx,
            )
        };

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
        self.map.borrow_mut().remove(&key);
        {
            let e = &mut self.entries.borrow_mut()[slot_idx];
            e.page = None;
            e.ref_bit.set(false);
            e.reset_links();
            self.freelist.borrow_mut().push(slot_idx);
        }
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
        let Some(slot) = self.map.borrow().get(key) else {
            return Ok(None);
        };
        // Because we can abort a read_page completion, this means a page can be in the cache but be unloaded and unlocked.
        // However, if we do not evict that page from the page cache, we will return an unloaded page later which will trigger
        // assertions later on. This is worsened by the fact that page cache is not per `Statement`, so you can abort a completion
        // in one Statement, and trigger some error in the next one if we don't evict the page here.
        let page = self.entries.borrow()[slot]
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
        let slot = self.map.borrow().get(key)?;
        let entries = self.entries.borrow();
        let page = entries[slot].page.as_ref()?.clone();
        if touch {
            // set reference bit to 'touch' page
            entries[slot].ref_bit.set(true);
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
        assert!(new_cap > 0);
        // Collect survivors as payload, no linkage
        struct Payload {
            key: PageCacheKey,
            page: PageRef,
            ref_bit: bool,
        }
        let survivors: Vec<Payload> = {
            let entries = self.entries.borrow();
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
        self.entries
            .borrow_mut()
            .resize(new_cap, PageCacheEntry::empty());
        self.capacity = new_cap;
        let mut new_map = PageHashMap::new(new_cap);
        self.head.set(NULL);
        self.tail.set(NULL);

        // Repack compactly: survivors[tail..head] pushed to front -> final order == original
        {
            let mut entries_mut = self.entries.borrow_mut();
            for (slot, pl) in survivors.iter().enumerate().take(new_cap) {
                let e = &mut entries_mut[slot];
                e.key = pl.key;
                e.page = Some(pl.page.clone());
                e.ref_bit.set(pl.ref_bit);
                e.reset_links();
                new_map.insert(pl.key, slot);
            }
        }
        for slot in 0..survivors.len().min(new_cap) {
            self.link_front(slot);
        }
        self.map.replace(new_map);

        // Rebuild freelist
        let used = survivors.len().min(new_cap);
        {
            let mut fl = self.freelist.borrow_mut();
            fl.clear();
            for i in (used..new_cap).rev() {
                fl.push(i);
            }
        }

        CacheResizeResult::Done
    }

    /// Ensures at least `n` free slots are available
    ///
    /// Uses the SIEVE algorithm to evict pages if necessary:
    /// 1. Start at tail (LRU position)
    /// 2. If page is marked, unmark and move to head (second chance)
    /// 3. If page is unmarked, evict it
    /// 4. If page is unevictable (dirty/locked/pinned), move to head
    ///
    /// Returns `CacheError::Full` if not enough pages can be evicted
    pub fn make_room_for(&mut self, n: usize) -> Result<(), CacheError> {
        if n > self.capacity {
            return Err(CacheError::Full);
        }

        let len = self.len();
        let available = self.capacity.saturating_sub(len);
        if n <= available && len <= self.capacity {
            return Ok(());
        }

        let mut need = n - available;
        let mut examined = 0usize;
        let max_examinations = self.len().saturating_mul(2);

        while need > 0 && examined < max_examinations {
            let tail_idx = match self.tail.get() {
                NULL => {
                    return Err(CacheError::InternalError(
                        "Tail is None but map not empty".into(),
                    ))
                }
                t => t,
            };

            let (was_marked, key) = {
                let mut entries = self.entries.borrow_mut();
                let s = &mut entries[tail_idx];
                turso_assert!(s.page.is_some(), "tail points to empty slot");
                (s.ref_bit.replace(false), s.key)
            };

            examined += 1;

            if was_marked {
                self.move_to_front(tail_idx);
                continue;
            }

            match self._delete(key, true) {
                Ok(_) => {
                    need -= 1;
                    examined = 0;
                }
                Err(
                    CacheError::Dirty { .. }
                    | CacheError::Locked { .. }
                    | CacheError::Pinned { .. },
                ) => {
                    self.move_to_front(tail_idx);
                }
                Err(e) => return Err(e),
            }
        }
        if need > 0 {
            return Err(CacheError::Full);
        }
        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), CacheError> {
        for e in self.entries.borrow().iter() {
            if let Some(ref p) = e.page {
                if p.is_dirty() {
                    return Err(CacheError::Dirty { pgno: p.get().id });
                }
                p.clear_loaded();
                let _ = p.get().contents.take();
            }
        }
        self.entries.borrow_mut().fill(PageCacheEntry::empty());
        self.map.borrow_mut().clear();
        self.head.set(NULL);
        self.tail.set(NULL);
        {
            let mut fl = self.freelist.borrow_mut();
            fl.clear();
            for i in (0..self.capacity).rev() {
                fl.push(i);
            }
        }
        Ok(())
    }

    /// Removes all pages from the cache with pgno greater than len
    pub fn truncate(&mut self, len: usize) -> Result<(), CacheError> {
        let keys_to_delete: Vec<PageCacheKey> = {
            self.entries
                .borrow()
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
        tracing::debug!("page_cache_len={}", self.map.borrow().len());
        let entries = self.entries.borrow();

        for (i, entry_opt) in entries.iter().enumerate() {
            if let Some(ref page) = entry_opt.page {
                tracing::debug!(
                    "slot={}, page={:?}, flags={}, pin_count={}, ref_bit={}",
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
        let entries = self.entries.borrow();
        for entry in entries.iter() {
            if entry.page.is_none() {
                continue;
            }
            keys.push(entry.key);
        }
        keys
    }

    pub fn len(&self) -> usize {
        self.map.borrow().len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn unset_dirty_all_pages(&mut self) {
        let entries = self.entries.borrow();
        for entry in entries.iter() {
            if entry.page.is_none() {
                continue;
            }
            entry.page.as_ref().unwrap().clear_dirty();
        }
    }

    #[cfg(test)]
    fn verify_cache_integrity(&self) {
        let entries = self.entries.borrow();
        let map = self.map.borrow();

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
        let freelist = self.freelist.borrow();
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
    slot_index: SlotIndex,
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

    pub fn insert(&mut self, key: PageCacheKey, slot_index: SlotIndex) {
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

    pub fn get(&self, key: &PageCacheKey) -> Option<SlotIndex> {
        let bucket = self.hash(key);
        let bucket = &self.buckets[bucket];
        for node in bucket {
            if node.key == *key {
                return Some(node.slot_index);
            }
        }
        None
    }

    pub fn remove(&mut self, key: &PageCacheKey) -> Option<SlotIndex> {
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
    use crate::{BufferPool, IO};
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };
    use std::sync::Arc;
    use std::sync::OnceLock;

    fn create_key(id: usize) -> PageCacheKey {
        PageCacheKey::new(id)
    }

    static TEST_BUFFER_POOL: OnceLock<Arc<BufferPool>> = OnceLock::new();

    #[allow(clippy::arc_with_non_send_sync)]
    pub fn page_with_content(page_id: usize) -> PageRef {
        let page = Arc::new(Page::new(page_id));
        {
            let mock_io = Arc::new(crate::PlatformIO::new().unwrap()) as Arc<dyn IO>;
            let pool = TEST_BUFFER_POOL
                .get_or_init(|| BufferPool::begin_init(&mock_io, BufferPool::TEST_ARENA_SIZE));
            let buffer = pool.allocate(4096);
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

    fn insert_and_get_entry(cache: &mut PageCache, id: usize) -> (PageCacheKey, PageCacheEntry) {
        let key = create_key(id);
        let page = page_with_content(id);
        assert!(cache.insert(key, page).is_ok());
        let entry = cache.map.borrow().get(&key).expect("Key should exist");
        let entry = cache.entries.borrow()[entry].clone();
        (key, entry)
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
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_detach_tail() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1); // Will be tail
        let key2 = insert_page(&mut cache, 2); // Will be middle
        let _key3 = insert_page(&mut cache, 3); // Will be head
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 3);

        // Verify initial tail
        let tail_slot = cache.tail.get();
        assert_eq!(
            cache.entries.borrow()[tail_slot].key,
            key1,
            "Initial tail check"
        );

        // Delete tail
        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.len(), 2, "Length should be 2 after deleting tail");
        assert!(
            cache.map.borrow().get(&key1).is_none(),
            "Map should not contain deleted tail key"
        );
        cache.verify_cache_integrity();

        // Check new tail is key2
        let new_tail_slot = cache.tail.get();
        let entries = cache.entries.borrow();
        assert_eq!(entries[new_tail_slot].key, key2, "New tail should be key2");
        assert_eq!(
            entries[new_tail_slot].next.get(),
            NULL,
            "New tail's next should be NULL"
        );

        let head_slot = cache.head.get();
        assert_eq!(
            entries[head_slot].prev.get(),
            NULL,
            "Head's prev should be NULL"
        );
        assert_eq!(
            entries[head_slot].next.get(),
            new_tail_slot,
            "Head's next should point to new tail"
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
        assert_eq!(
            cache.entries.borrow()[cache.tail.get()].key,
            key1,
            "Initial tail check"
        );
        assert_eq!(
            cache.entries.borrow()[cache.head.get()].key,
            key4,
            "Initial head check"
        );

        let head_slot_before = cache.head.get();
        let tail_slot_before = cache.tail.get();

        // Delete middle element (key2)
        assert!(cache.delete(key2).is_ok());
        assert_eq!(cache.len(), 3, "Length should be 3 after deleting middle");
        assert!(
            cache.map.borrow().get(&key2).is_none(),
            "Map should not contain deleted middle key2"
        );
        cache.verify_cache_integrity();

        // Head and tail should remain the same
        assert_eq!(
            cache.head.get(),
            head_slot_before,
            "Head should remain key4"
        );
        assert_eq!(
            cache.tail.get(),
            tail_slot_before,
            "Tail should remain key1"
        );

        // Check that key3 and key1 are now linked
        let entries = cache.entries.borrow();
        let key3_slot = cache.map.borrow().get(&key3).unwrap();
        let key1_slot = cache.map.borrow().get(&key1).unwrap();

        assert_eq!(
            entries[key3_slot].next.get(),
            key1_slot,
            "Key3's next should point to key1"
        );
        assert_eq!(
            entries[key1_slot].prev.get(),
            key3_slot,
            "Key1's prev should point to key3"
        );
    }

    #[test]
    fn test_insert_existing_key_updates_in_place() {
        let mut cache = PageCache::default();
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page1_v1.clone();

        assert!(cache.insert(key1, page1_v1.clone()).is_ok());
        assert_eq!(cache.len(), 1);
        // This should return KeyExists error but not panic
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
        let page1_v2 = page_with_content(1); // Different instance
        assert!(cache.insert(key1, page1_v1.clone()).is_ok());
        assert_eq!(cache.len(), 1);
        cache.verify_cache_integrity();
        let _ = cache.insert(key1, page1_v2.clone()); // Panic
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut cache = PageCache::default();
        let key_nonexist = create_key(99);

        assert!(cache.delete(key_nonexist).is_ok()); // no-op
    }

    #[test]
    fn test_page_cache_evict() {
        let mut cache = PageCache::new(1);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);
        assert!(cache.get(&key1).unwrap().is_none());
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "capacity=1 should evict the older page"
        );
    }

    #[test]
    fn test_sieve_touch_non_tail_does_not_affect_immediate_eviction() {
        // Insert 1,2,3 -> [3,2,1], tail=1
        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Touch key2 (not tail) to mark it.
        assert!(cache.get(&key2).unwrap().is_some());

        // Insert 4: tail is still 1 (unmarked) -> evict 1 (not 2).
        let key4 = insert_page(&mut cache, 4);

        assert!(
            cache.get(&key2).unwrap().is_some(),
            "marked non-tail should remain"
        );
        assert!(cache.get(&key3).unwrap().is_some());
        assert!(cache.get(&key4).unwrap().is_some());
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "unmarked tail should be evicted"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_sieve_second_chance_preserves_marked_tail() {
        // Capacity 3, insert 1,2,3 → order(head..tail) = [3,2,1]
        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        assert_eq!(cache.len(), 3);

        // Mark the TAIL (key1). SIEVE should not move it yet
        assert!(cache.get(&key1).unwrap().is_some());

        // Insert 4: must evict exactly one unmarked page.
        // Tail is 1 (marked): give second chance: move 1 to head & clear its bit.
        // New tail becomes 2 (unmarked): evict 2. Final set: (1,3,4).
        let key4 = insert_page(&mut cache, 4);

        assert!(
            cache.get(&key1).unwrap().is_some(),
            "key1 is marked tail and should survive"
        );
        assert!(cache.get(&key3).unwrap().is_some(), "key3 should remain");
        assert!(cache.get(&key4).unwrap().is_some(), "key4 just inserted");
        assert!(
            cache.get(&key2).unwrap().is_none(),
            "key2 should be the one evicted by SIEVE"
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
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_dirty_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).expect("Page should exist").unwrap();
        page.set_dirty();

        assert_eq!(cache.delete(key), Err(CacheError::Dirty { pgno: 1 }));
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_pinned_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).expect("Page should exist").unwrap();
        page.pin();

        assert_eq!(cache.delete(key), Err(CacheError::Pinned { pgno: 1 }));
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_make_room_for_with_dirty_pages() {
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Make both pages dirty
        cache.get(&key1).unwrap().unwrap().set_dirty();
        cache.get(&key2).unwrap().unwrap().set_dirty();

        // Try to insert a third page, should fail because can't evict dirty pages
        let key3 = create_key(3);
        let page3 = page_with_content(3);
        let result = cache.insert(key3, page3);

        assert_eq!(result, Err(CacheError::Full));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_page_cache_insert_and_get() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.get(&key1).unwrap().unwrap().get().id, 1);
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);
    }

    #[test]
    fn test_page_cache_over_capacity() {
        // Capacity 2, insert 1,2 -> [2,1] with tail=1 (unmarked)
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Insert 3 -> tail(1) is unmarked → evict 1; keep 2.
        let key3 = insert_page(&mut cache, 3);

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&key2).unwrap().is_some(), "key2 should remain");
        assert!(cache.get(&key3).unwrap().is_some(), "key3 just inserted");
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "key1 (tail, unmarked) must be evicted"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_delete() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        assert!(cache.delete(key1).is_ok());
        assert!(cache.get(&key1).unwrap().is_none());
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
        assert!(cache.insert(create_key(6), page_with_content(6)).is_ok());
    }

    #[test]
    fn test_detach_with_multiple_pages() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        assert_eq!(cache.head.get(), 2, "Head should be slot for key3");
        assert_eq!(cache.tail.get(), 0, "Tail should be slot for key1");

        // Delete middle element
        assert!(cache.delete(key2).is_ok());

        // Verify structure after deletion
        assert_eq!(cache.len(), 2);
        assert!(cache.map.borrow().get(&key2).is_none());

        // Head should still be key3's slot, tail should still be key1's slot
        let head_slot = cache.head.get();
        let tail_slot = cache.tail.get();
        let entries = cache.entries.borrow();
        assert_eq!(entries[head_slot].key, key3, "Head should still be key3");
        assert_eq!(entries[tail_slot].key, key1, "Tail should still be key1");

        // Check linkage
        assert_eq!(
            entries[head_slot].next.get(),
            tail_slot,
            "Head's next should point directly to tail after middle deleted"
        );
        assert_eq!(
            entries[tail_slot].prev.get(),
            head_slot,
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
            cache.map.borrow().get(&key3).is_none(),
            "Map should not contain deleted head key"
        );
        cache.verify_cache_integrity();

        // Check new head is key2
        let head_slot = cache.head.get();
        {
            let entries = cache.entries.borrow_mut();
            assert_eq!(entries[head_slot].key, key2, "New head should be key2");
            assert_eq!(
                entries[head_slot].prev.get(),
                NULL,
                "New head's prev should be NULL"
            );
        }
        // Delete another
        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.len(), 1, "Length should be 1 after deleting two");
        cache.verify_cache_integrity();

        // Delete last
        assert!(cache.delete(key2).is_ok());
        assert_eq!(cache.len(), 0, "Length should be 0 after deleting all");
        assert_eq!(cache.head.get(), NULL, "Head should be NULL when empty");
        assert_eq!(cache.tail.get(), NULL, "Tail should be NULL when empty");
        cache.verify_cache_integrity();
    }

    fn test_resize_larger() {
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.len(), 2);

        let result = cache.resize(5);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.capacity(), 5);

        assert!(cache.get(&key1).is_ok_and(|p| p.is_some()));
        assert!(cache.get(&key2).is_ok_and(|p| p.is_some()));

        // Now we should be able to add 3 more
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
        tracing::info!("super seed: {}", seed);
        let max_pages = 10;
        let mut cache = PageCache::new(10);
        let mut reference_map = std::collections::HashMap::new();

        for _ in 0..10000 {
            cache.print();

            match rng.next_u64() % 2 {
                0 => {
                    // add
                    let id_page = rng.next_u64() % max_pages;
                    let key = PageCacheKey::new(id_page as usize);
                    #[allow(clippy::arc_with_non_send_sync)]
                    let page = Arc::new(Page::new(id_page as usize));

                    if cache.peek(&key, false).is_some() {
                        continue; // skip duplicate page ids
                    }

                    tracing::debug!("inserting page {:?}", key);
                    match cache.insert(key, page.clone()) {
                        Err(CacheError::Full | CacheError::ActiveRefs) => {} // Ignore
                        Err(err) => {
                            panic!("Cache insertion failed: {err:?}");
                        }
                        Ok(_) => {
                            reference_map.insert(key, page);
                            // Clean up reference_map if cache evicted something
                            if cache.len() < reference_map.len() {
                                reference_map.retain(|k, _| cache.contains_key(k));
                            }
                        }
                    }
                    assert!(cache.len() <= 10);
                }
                1 => {
                    // remove
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
        // Capacity 2: insert 1,2 -> [2,1] tail=1
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.get(&key1).unwrap().unwrap().get().id, 1);
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);

        // Peek without touching DOES NOT mark key1
        assert!(cache.peek(&key1, false).is_some());

        // Insert 3 -> tail(1) still unmarked → evict 1.
        let key3 = insert_page(&mut cache, 3);
        assert!(cache.get(&key1).unwrap().is_none());
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);
        assert_eq!(cache.get(&key3).unwrap().unwrap().get().id, 3);

        assert!(cache.get(&key3).is_ok_and(|p| p.is_some()));
        assert!(cache.get(&key2).is_ok_and(|p| p.is_some()));
        assert!(
            cache.get(&key1).is_ok_and(|p| p.is_none()),
            "key1 should be evicted since peek(false) didn't mark"
        );
        assert_eq!(cache.len(), 2);
        cache.verify_cache_integrity();
    }

    #[test]
    #[ignore = "long running test, remove to verify"]
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
        println!("Growth: {growth}");
        assert!(
            growth < 10_000_000,
            "Memory grew by {growth} bytes over 10 cycles"
        );
    }
}
