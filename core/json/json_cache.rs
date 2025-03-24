use std::cell::{Cell, UnsafeCell};

use crate::OwnedValue;

use super::jsonb::Jsonb;

const JSON_CACHE_SIZE: usize = 4;

#[derive(Debug)]
pub struct JsonCache {
    entries: [Option<(OwnedValue, Jsonb)>; JSON_CACHE_SIZE],
    age: [usize; JSON_CACHE_SIZE],
    used: usize,
    counter: usize,
}

impl JsonCache {
    pub fn new() -> Self {
        Self {
            entries: [None, None, None, None],
            age: [0, 0, 0, 0],
            used: 0,
            counter: 0,
        }
    }

    fn find_oldest_entry(&self) -> usize {
        let mut oldest_idx = 0;
        let mut oldest_age = self.age[0];

        for i in 1..self.used {
            if self.age[i] < oldest_age {
                oldest_idx = i;
                oldest_age = self.age[i];
            }
        }

        oldest_idx
    }

    pub fn insert(&mut self, key: &OwnedValue, value: &Jsonb) {
        if self.used < JSON_CACHE_SIZE {
            self.entries[self.used] = Some((key.clone(), value.clone()));
            self.age[self.used] = self.counter;
            self.counter += 1;
            self.used += 1
        } else {
            let id = self.find_oldest_entry();

            self.entries[id] = Some((key.clone(), value.clone()));
            self.age[id] = self.counter;
            self.counter += 1;
        }
    }

    pub fn lookup(&mut self, key: &OwnedValue) -> Option<Jsonb> {
        for i in (0..self.used).rev() {
            if let Some((stored_key, value)) = &self.entries[i] {
                if key == stored_key {
                    self.age[i] = self.counter;
                    self.counter += 1;
                    let json = value.clone();

                    return Some(json);
                }
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct JsonCacheCell {
    inner: UnsafeCell<Option<JsonCache>>,
    accessed: Cell<bool>,
}

impl JsonCacheCell {
    pub fn new() -> Self {
        Self {
            inner: UnsafeCell::new(None),
            accessed: Cell::new(false),
        }
    }

    pub fn lookup(&self, key: &OwnedValue) -> Option<Jsonb> {
        assert_eq!(self.accessed.get(), false);

        self.accessed.set(true);

        let result = unsafe {
            let cache_ptr = self.inner.get();
            if (*cache_ptr).is_none() {
                *cache_ptr = Some(JsonCache::new());
            }

            if let Some(cache) = &mut (*cache_ptr) {
                cache.lookup(key)
            } else {
                None
            }
        };

        self.accessed.set(false);
        result
    }

    pub fn get_or_insert_with(
        &self,
        key: &OwnedValue,
        value: impl Fn(&OwnedValue) -> crate::Result<Jsonb>,
    ) -> crate::Result<Jsonb> {
        assert_eq!(self.accessed.get(), false);

        self.accessed.set(true);
        let result = unsafe {
            let cache_ptr = self.inner.get();
            if (*cache_ptr).is_none() {
                *cache_ptr = Some(JsonCache::new());
            }

            if let Some(cache) = &mut (*cache_ptr) {
                if let Some(jsonb) = cache.lookup(key) {
                    Ok(jsonb)
                } else {
                    let result = value(key);
                    match result {
                        Ok(json) => {
                            cache.insert(key, &json);
                            Ok(json)
                        }
                        Err(e) => Err(e),
                    }
                }
            } else {
                let result = value(key);
                result
            }
        };
        self.accessed.set(false);

        result
    }
}
