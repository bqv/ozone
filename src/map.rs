
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::cmp::Eq;
use std::io::Result;
use std::marker::PhantomData;
use std::path::Path;
use std::{mem, fmt};

use buffer::{Buffer, AnonymousBuffer, FileBuffer};

const INITIAL_SIZE: usize = 256usize; // Must be a power of 2
const LOAD_FACTOR_PERCENT: usize = 90usize;

pub struct Elem<K, V>
    where K: Eq + Hash + Sized,
          V: Sized
{
    key: K,
    value: V,
    hash: u64,
}

pub struct HashMap<K, V, B = AnonymousBuffer<Elem<K, V>>>
    where K: Eq + Hash + Sized,
          V: Sized,
          B: Buffer<Elem<K, V>>
{
    buffer: B,
    num_elems: usize,
    capacity: usize,
    resize_threshold: usize,
    mask: u64,
    phantom_k: PhantomData<K>,
    phantom_v: PhantomData<V>,
}

pub enum Entry<'a, K: 'a, V: 'a>
    where K: Eq + Hash + Sized,
          V: Sized,
{
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}

pub struct OccupiedEntry<'a, K: 'a, V: 'a>
    where K: Eq + Hash + Sized,
          V: Sized,
{
    elem: &'a mut Elem<K, V>,
}

pub struct VacantEntry<'a, K: 'a, V: 'a>
    where K: Eq + Hash + Sized,
          V: Sized,
{
    elem: &'a mut Elem<K, V>,
}

impl<'a, K: 'a, V: 'a> Entry<'a, K, V>
    where K: Eq + Hash + Sized,
          V: Sized,
{
    pub fn or_insert(self, default: V) -> &'a mut V {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(default)
        }
    }

    pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(default())
        }
    }

    pub fn key(&self) -> &K {
        match *self {
            Entry::Occupied(ref entry) => entry.key(),
            Entry::Vacant(ref entry) => entry.key()
        }
    }
}

impl<'a, K: 'a, V: 'a> OccupiedEntry<'a, K, V>
    where K: Eq + Hash + Sized,
          V: Sized,
{
    pub fn key(&self) -> &K {
        &self.elem.key
    }

    pub fn remove_entry(self) -> (K, V) {
        let mut key: K = unsafe { mem::uninitialized() };
        let mut value: V = unsafe { mem::uninitialized() };
        self.elem.hash |= 0x8000000000000000u64;
        mem::swap(&mut key, &mut self.elem.key);
        mem::swap(&mut value, &mut self.elem.value);
        (key, value)
    }

    pub fn get(&self) -> &V {
        &self.elem.value
    }

    pub fn get_mut(&mut self) -> &mut V {
        &mut self.elem.value
    }

    pub fn into_mut(self) -> &'a mut V {
        &mut self.elem.value
    }

    pub fn insert(&mut self, value: V) -> V {
        let mut value = value;
        mem::swap(&mut value, &mut self.elem.value);
        value
    }

    pub fn remove(&mut self) -> V {
        let mut value: V = unsafe { mem::uninitialized() };
        self.elem.hash |= 0x8000000000000000u64;
        mem::swap(&mut value, &mut self.elem.value);
        value
    }
}

impl<'a, K: 'a, V: 'a> VacantEntry<'a, K, V>
    where K: Eq + Hash + Sized,
          V: Sized,
{
    pub fn key(&self) -> &K {
        &self.elem.key
    }

    pub fn into_key(self) -> K {
        let mut key: K = unsafe { mem::uninitialized() };
        mem::swap(&mut key, &mut self.elem.key);
        key
    }

    pub fn insert(self, value: V) -> &'a mut V {
        let mut value = value;
        mem::swap(&mut self.elem.value, &mut value); 
        mem::forget(value);
        self.elem.hash = HashMap::<K, V>::hash_key(&self.elem.key);
        &mut self.elem.value
    }
}

impl<K, V> HashMap<K, V, AnonymousBuffer<Elem<K, V>>>
    where K: Eq + Hash + Sized,
          V: Sized,
{
    pub fn new() -> Self {
        Self::try_new().unwrap()
    }

    pub fn try_new() -> Result<Self> {
        let buffer = AnonymousBuffer::try_new(INITIAL_SIZE * mem::size_of::<Elem<K, V>>())?;
        let mut h = HashMap {
            buffer: buffer,
            num_elems: 0,
            capacity: INITIAL_SIZE,
            resize_threshold: ((INITIAL_SIZE * LOAD_FACTOR_PERCENT) as f64 / 100f64) as usize,
            mask: INITIAL_SIZE as u64 - 1,
            phantom_k: PhantomData,
            phantom_v: PhantomData,
        };
        for i in 0..h.capacity {
            let mut hash = h.elem_hash_mut(i);
            *hash = 0;
        }
        Ok(h)
    }
}

impl<K, V> HashMap<K, V, FileBuffer<Elem<K, V>>>
    where K: Eq + Hash + Sized,
          V: Sized,
{
    pub fn new_with_file<P>(path: P) -> Self
        where P: AsRef<Path> + Clone
    {
        Self::try_new_with_file(path).unwrap()
    }

    pub fn try_new_with_file<P>(path: P) -> Result<Self>
        where P: AsRef<Path> + Clone
    {
        let buffer = FileBuffer::try_new(path, INITIAL_SIZE * mem::size_of::<Elem<K, V>>())?;
        let mut h = HashMap {
            buffer: buffer,
            num_elems: 0,
            capacity: INITIAL_SIZE,
            resize_threshold: ((INITIAL_SIZE * LOAD_FACTOR_PERCENT) as f64 / 100f64) as usize,
            mask: INITIAL_SIZE as u64 - 1,
            phantom_k: PhantomData,
            phantom_v: PhantomData,
        };
        for i in 0..h.capacity {
            let mut hash = h.elem_hash_mut(i);
            *hash = 0;
        }
        Ok(h)
    }
}

impl<K, V, B> HashMap<K, V, B>
    where K: Eq + Hash + Sized,
          V: Sized,
          B: Buffer<Elem<K, V>>
{
    pub fn insert(&mut self, key: K, value: V) {
        self.try_insert(key, value).unwrap()
    }

    pub fn try_insert(&mut self, key: K, value: V) -> Result<()> {
        self.try_insert_with_hash(Self::hash_key(&key), key, value)?;
        Ok(())
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if let Some(ix) = self.lookup_index(key) {
            Some(&self.buffer[ix].value)
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if let Some(ix) = self.lookup_index(key) {
            Some(&mut self.buffer[ix].value)
        } else {
            None
        }
    }

    pub fn get_key(&self, key: &K) -> Option<&K> {
        if let Some(ix) = self.lookup_index(key) {
            Some(&self.buffer[ix].key)
        } else {
            None
        }
    }

    pub fn remove(&mut self, key: &K) -> bool {
        if let Some(ix) = self.lookup_index(key) {
            self.num_elems -= 1;
            let elem_hash = self.elem_hash_mut(ix);
            *elem_hash |= 0x8000000000000000u64;
            true
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.num_elems
    }

    pub fn entry(&mut self, key: K) -> Entry<K, V> {
        if let Some(ix) = self.lookup_index(&key) {
            Entry::Occupied(OccupiedEntry { elem: &mut self.buffer[ix] })
        } else {
            let hash = Self::hash_key(&key);
            let value = unsafe { mem::uninitialized() };
            let pos = self.insert_with_hash(hash, key, value);
            self.buffer[pos].hash = 0;
            Entry::Vacant(VacantEntry { elem: &mut self.buffer[pos] })
        }
    }

    pub fn is_empty(&self) -> bool {
        self.num_elems == 0
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub fn iter<'a>(&'a self) -> Iter<'a, K, V, B> {
        Iter { map: &self, ix: 0 }
    }

    pub fn keys<'a>(&'a self) -> Keys<'a, K, V, B> {
        Keys { map: &self, ix: 0 }
    }

    pub fn values<'a>(&'a self) -> Values<'a, K, V, B> {
        Values { map: &self, ix: 0 }
    }

    //pub fn drain<'a>(&'a mut self) -> Drain<'a, K, V, B> { }

    //pub fn clear(&mut self) { }
}

impl<K, V, B> HashMap<K, V, B>
    where K: Eq + Hash + Sized,
          V: Sized,
          B: Buffer<Elem<K, V>>
{
    fn hash_key(key: &K) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let mut hash = hasher.finish();

        // Clear MSB - Used to indicate deletion
        hash &= 0x7FFFFFFFFFFFFFFFu64;

        // Shift 0 to 1 - Zero indicates emptiness
        hash | (hash == 0u64) as u64
    }

    fn is_deleted(hash: u64) -> bool {
        // MSB determines if this hash is a tombstone
        (hash >> 63) != 0
    }

    fn desired_pos(&self, hash: u64) -> usize {
        (hash & self.mask) as usize
    }

    fn probe_distance(&self, hash: u64, slot_index: u64) -> usize {
        let distance = slot_index + self.capacity as u64 - self.desired_pos(hash) as u64;
        (distance & self.mask) as usize
    }

    fn elem_hash(&self, ix: usize) -> &u64 {
        &self.buffer[ix].hash
    }

    fn elem_hash_mut(&mut self, ix: usize) -> &mut u64 {
        &mut self.buffer[ix].hash
    }

    fn insert_with_hash(&mut self, hash: u64, key: K, value: V) -> usize {
        self.try_insert_with_hash(hash, key, value).unwrap()
    }

    fn try_insert_with_hash(&mut self, hash: u64, key: K, value: V) -> Result<usize> {
        self.num_elems += 1;
        if self.num_elems >= self.resize_threshold {
            self.grow()?;
        }
        Ok(self.insert_helper(hash, key, value))
    }

    fn alloc(&mut self) -> Result<()> {
        self.buffer = self.buffer.new_sized(self.capacity * mem::size_of::<Elem<K, V>>())?;

        for i in 0..self.capacity {
            let mut hash = self.elem_hash_mut(i);
            *hash = 0;
        }
        
        self.resize_threshold = ((self.capacity * LOAD_FACTOR_PERCENT) as f64 / 100f64) as usize;
        self.mask = self.capacity as u64 - 1;

        Ok(())
    }

    fn grow(&mut self) -> Result<()> {
        println!("Growing...");
        let mut old_buffer = self.buffer.clone();
        let old_capacity = self.capacity;

        self.capacity *= 2;
        self.alloc()?;

        for i in 0..old_capacity {
            let old_elem = &mut old_buffer[i];
            let hash = old_elem.hash;
            if hash != 0 && !Self::is_deleted(hash) {
                let k: K = unsafe { mem::uninitialized() };
                let v: V = unsafe { mem::uninitialized() };
                let ix = self.insert_helper(hash, k, v);
                let new_elem = &mut self.buffer[ix];
                mem::swap(old_elem, new_elem);
            }
        }

        Ok(())
    }

    fn construct(&mut self, ix: usize, hash: u64, key: K, val: V) {
        self.buffer[ix] = Elem { key: key, value: val, hash: hash };
    }

    fn insert_helper(&mut self, mut hash: u64, mut key: K, mut val: V) -> usize {
        let mut pos = self.desired_pos(hash);
        let mut dist = 0;
        let mut ix = 0;
        let mut first = true;
        loop {
            let elem_hash = *self.elem_hash(pos);
            if elem_hash == 0u64 {
                self.construct(pos, hash, key, val);
                if first {
                    ix = pos;
                }
                break;
            }

            let existing_elem_probe_dist = self.probe_distance(elem_hash, pos as u64);
            if existing_elem_probe_dist < dist {
                if Self::is_deleted(elem_hash) {
                    self.construct(pos, hash, key, val);
                    if first {
                        ix = pos;
                    }
                    break;
                }

                if first {
                    ix = pos;
                    first = false;
                }
                mem::swap(&mut hash, self.elem_hash_mut(pos));
                mem::swap(&mut key, &mut self.buffer[pos].key);
                mem::swap(&mut val, &mut self.buffer[pos].value);
                dist = existing_elem_probe_dist;
            }

            pos = (pos + 1) & self.mask as usize;
            dist += 1;
        }
        ix
    }

    fn lookup_index(&self, key: &K) -> Option<usize> {
        let hash = Self::hash_key(key);
        let mut pos = self.desired_pos(hash);
        let mut dist = 0;
        loop {
            let elem_hash = *self.elem_hash(pos);
            if elem_hash == 0 {
                return None;
            } else if dist > self.probe_distance(elem_hash, pos as u64) {
                return None;
            } else if elem_hash == hash && self.buffer[pos].key == *key {
                return Some(pos);
            }

            pos = (pos + 1) & self.mask as usize;
            dist += 1;
        }
    }

    fn average_probe_count(&self) -> f64 {
        let mut probe_total = 0.;
        for i in 0..self.capacity {
            let hash = *self.elem_hash(i);
            if hash != 0 && !Self::is_deleted(hash) {
                probe_total += self.probe_distance(hash, i as u64) as f64;
            }
        }
        probe_total / self.num_elems as f64 + 1.
    }
}

pub struct Iter<'a, K, V, B>
    where K: 'a + Eq + Hash + Sized,
          V: 'a + Sized,
          B: 'a + Buffer<Elem<K, V>>
{
    map: &'a HashMap<K, V, B>,
    ix: usize,
}

impl<'a, K, V, B> Iterator for Iter<'a, K, V, B>
    where K: 'a + Eq + Hash + Sized,
          V: 'a + Sized,
          B: 'a + Buffer<Elem<K, V>>
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.ix < self.map.capacity {
            let hash = *self.map.elem_hash(self.ix);
            if hash != 0 && !HashMap::<K, V>::is_deleted(hash) {
                let ref entry = self.map.buffer[self.ix];
                self.ix += 1;
                return Some((&entry.key, &entry.value));
            }
            self.ix += 1;
        }
        None
    }
}

pub struct Keys<'a, K, V, B>
    where K: 'a + Eq + Hash + Sized,
          V: 'a + Sized,
          B: 'a + Buffer<Elem<K, V>>
{
    map: &'a HashMap<K, V, B>,
    ix: usize,
}

impl<'a, K, V, B> Iterator for Keys<'a, K, V, B>
    where K: 'a + Eq + Hash + Sized,
          V: 'a + Sized,
          B: 'a + Buffer<Elem<K, V>>
{
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        while self.ix < self.map.capacity {
            let hash = *self.map.elem_hash(self.ix);
            if hash != 0 && !HashMap::<K, V>::is_deleted(hash) {
                let ref entry = self.map.buffer[self.ix];
                self.ix += 1;
                return Some(&entry.key);
            }
            self.ix += 1;
        }
        None
    }
}

pub struct Values<'a, K, V, B>
    where K: 'a + Eq + Hash + Sized,
          V: 'a + Sized,
          B: 'a + Buffer<Elem<K, V>>
{
    map: &'a HashMap<K, V, B>,
    ix: usize,
}

impl<'a, K, V, B> Iterator for Values<'a, K, V, B>
    where K: 'a + Eq + Hash + Sized,
          V: 'a + Sized,
          B: 'a + Buffer<Elem<K, V>>
{
    type Item = &'a V;

    fn next(&mut self) -> Option<Self::Item> {
        while self.ix < self.map.capacity {
            let hash = *self.map.elem_hash(self.ix);
            if hash != 0 && !HashMap::<K, V>::is_deleted(hash) {
                let ref entry = self.map.buffer[self.ix];
                self.ix += 1;
                return Some(&entry.value);
            }
            self.ix += 1;
        }
        None
    }
}

impl<K, V, B> fmt::Debug for HashMap<K, V, B>
    where K: Eq + Hash + Sized + fmt::Debug,
          V: Sized + fmt::Debug,
          B: Buffer<Elem<K, V>>
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_map().entries(self.iter()).finish()
    }
}

#[test]
fn create_hashmap() {
    let _h: HashMap<String, String> = HashMap::new();
}

#[test]
fn insert_and_get_hashmap() {
    let mut h: HashMap<String, String> = HashMap::new();
    let k = "Test".to_string();
    let v = "ing".to_string();
    h.insert(k.clone(), v.clone());
    assert_eq!(&v, h.get(&k).unwrap());
    println!("{:#?}", h);
}

#[test]
fn insert_and_iter_hashmap() {
    let mut h: HashMap<String, String> = HashMap::new();
    let k = "Test".to_string();
    let v = "ing".to_string();
    h.insert(k.clone(), v.clone());
    let iter = h.iter().collect::<Vec<_>>();
    assert_eq!(vec![(&k, &v)], iter);
}

#[test]
fn entry_or_insert_and_get_hashmap() {
    let mut h: HashMap<String, String> = HashMap::new();
    let k = "Test".to_string();
    let v = "ing".to_string();
    {
        let entry = h.entry(k.clone()).or_insert(v.clone());
        assert_eq!(entry, &v);
    }
    assert_eq!(&v, h.get(&k).unwrap());
}

#[test]
fn insert_and_remove_hashmap() {
    let mut h: HashMap<String, String> = HashMap::new();
    let k = "Test".to_string();
    let v = "ing".to_string();
    h.insert(k.clone(), v.clone());
    h.remove(&k);
    assert!(h.is_empty());
    println!("{:#?}", h);
}

#[test]
fn entry_or_insert_and_iter_300_hashmap() {
    let mut h: HashMap<usize, String> = HashMap::new();
    let v = "Testing".to_string();
    for k in 0..300 {
        let entry = h.entry(k).or_insert(v.clone());
        assert_eq!(entry, &v);
    }
    for (_, b) in h.iter() {
        assert_eq!(b, &v);
    }
}

