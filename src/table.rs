
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::cmp::Eq;
use std::io::Result;
use std::marker::PhantomData;
use std::path::Path;
use std::mem;

use buffer::{Buffer, AnonymousBuffer, FileBuffer};

const INITIAL_SIZE: usize = 256usize;
const LOAD_FACTOR_PERCENT: usize = 90usize;

struct Elem<K, V>
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
    phantomK: PhantomData<K>,
    phantomV: PhantomData<V>,
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
        Ok(HashMap {
            buffer: buffer,
            num_elems: 0,
            capacity: INITIAL_SIZE,
            resize_threshold: ((INITIAL_SIZE * LOAD_FACTOR_PERCENT) as f64 / 100f64) as usize,
            mask: INITIAL_SIZE as u64 - 1,
            phantomK: PhantomData,
            phantomV: PhantomData,
        })
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
        Ok(HashMap {
            buffer: buffer,
            num_elems: 0,
            capacity: INITIAL_SIZE,
            resize_threshold: ((INITIAL_SIZE * LOAD_FACTOR_PERCENT) as f64 / 100f64) as usize,
            mask: INITIAL_SIZE as u64 - 1,
            phantomK: PhantomData,
            phantomV: PhantomData,
        })
    }
}

impl<K, V, B> HashMap<K, V, B>
    where K: Eq + Hash + Sized,
          V: Sized,
          B: Buffer<Elem<K, V>>
{
    pub fn insert(&mut self, key: K, value: V) {
        self.num_elems += 1;
        if self.num_elems >= self.resize_threshold {
            self.grow();
        }
        self.insert_helper(Self::hash_key(&key), key, value);
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

    fn alloc(&mut self) -> Result<()> {
        self.buffer = self.buffer.resize(self.capacity * mem::size_of::<Elem<K, V>>())?;

        for i in 0..self.capacity {
            let mut hash = self.elem_hash_mut(i);
            *hash = 0;
        }
        
        self.resize_threshold = ((self.capacity * LOAD_FACTOR_PERCENT) as f64 / 100f64) as usize;
        self.mask = self.capacity as u64 - 1;

        Ok(())
    }

    fn grow(&mut self) -> Result<()> {
        let mut old_buffer = self.buffer.clone();
        let old_capacity = self.capacity;

        self.capacity *= 2;
        self.alloc()?;

        for i in 0..old_capacity {
            let ref mut old_elem = old_buffer[i];
            let mut e = Elem { key: unsafe { mem::uninitialized() }, value: unsafe { mem::uninitialized() }, hash: 0 };
            mem::swap(old_elem, &mut e);
            let hash = e.hash;
            if hash != 0 && !Self::is_deleted(hash) {
                self.insert_helper(hash, e.key, e.value);
            }
        }

        Ok(())
    }

    fn construct(&mut self, ix: usize, hash: u64, key: K, val: V) {
        self.buffer[ix] = Elem { key: key, value: val, hash: hash };
    }

    fn insert_helper(&mut self, mut hash: u64, mut key: K, mut val: V) {
        let mut pos = self.desired_pos(hash);
        let mut dist = 0;
        loop {
            let elem_hash = *self.elem_hash(pos);
            if elem_hash == 0u64 {
                self.construct(pos, hash, key, val);
                break;
            }

            let existing_elem_probe_dist = self.probe_distance(elem_hash, pos as u64);
            if existing_elem_probe_dist < dist {
                if Self::is_deleted(elem_hash) {
                    self.construct(pos, hash, key, val);
                    break;
                }

                mem::swap(&mut hash, self.elem_hash_mut(pos));
                mem::swap(&mut key, &mut self.buffer[pos].key);
                mem::swap(&mut val, &mut self.buffer[pos].value);
                dist = existing_elem_probe_dist;
            }

            pos = (pos + 1) & self.mask as usize;
            dist += 1;
        }
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

#[test]
fn create_hashmap() {
    let h: HashMap<String, String> = HashMap::new();
}

#[test]
fn insert_and_get_hashmap() {
    let mut h: HashMap<String, String> = HashMap::new();
    let k = "Test".to_string();
    let v = "ing".to_string();
    h.insert(k.clone(), v.clone());
    assert_eq!(&v, h.get(&k).unwrap());
}
