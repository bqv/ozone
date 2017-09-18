
use std::hash::Hash;
use std::cmp::Eq;
use std::io::Result;
use std::path::Path;
use std::{fmt};

use map::{HashMap, Elem};
use buffer::{Buffer, AnonymousBuffer, FileBuffer};

pub struct HashSet<T, B>
    where T: Eq + Hash + Sized,
          B: Buffer<Elem<T, ()>>
{
    map: HashMap<T, (), B>
}

impl<T> HashSet<T, AnonymousBuffer<Elem<T, ()>>>
    where T: Eq + Hash + Sized,
{
    pub fn new() -> Self {
        let map = HashMap::<T, (), AnonymousBuffer<Elem<T, ()>>>::new();
        Self { map: map }
    }

    pub fn try_new() -> Result<Self> {
        let map = HashMap::<T, (), AnonymousBuffer<Elem<T, ()>>>::try_new()?;
        Ok(Self { map: map })
    }
}

impl<T> HashSet<T, FileBuffer<Elem<T, ()>>>
    where T: Eq + Hash + Sized,
{
    pub fn new<P>(path: P) -> Self
        where P: AsRef<Path> + Clone
    {
        let map = HashMap::<T, (), FileBuffer<Elem<T, ()>>>::new(path);
        Self { map: map }
    }

    pub fn try_new<P>(path: P) -> Result<Self>
        where P: AsRef<Path> + Clone
    {
        let map = HashMap::<T, (), FileBuffer<Elem<T, ()>>>::try_new(path)?;
        Ok(Self { map: map })
    }
}

impl<T, B> HashSet<T, B>
    where T: Eq + Hash + Sized,
          B: Buffer<Elem<T, ()>>
{
    pub fn iter<'a>(&'a self) -> Iter<'a, T, B> {
        Iter { iter: self.map.iter() }
    }

    //pub fn difference<'a>(&'a self, other: &'a HashSet<T, B>) -> Difference<'a, T, B> { }

    //pub fn symmetric_difference<'a>(&'a self, other: &'a HashSet<T, B>) -> SymmetricDifference<'a, T, B> { }
    
    //pub fn intersection<'a>(&'a self, other: &'a HashSet<T, B>) -> Intersection<'a, T, B> { }
    
    //pub fn union<'a>(&'a self, other: &'a HashSet<T, B>) -> Union<'a, T, B> { }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    //pub fn drain(&mut self) -> Drain<T> { }

    pub fn contains(&self, value: &T) -> bool {
        self.map.contains_key(value)
    }

    pub fn get(&self, value: &T) -> Option<&T> {
        self.map.get_key(value)
    }

    //pub fn is_disjoint(&self, other: &HashSet<T, B>) -> bool { }

    //pub fn is_subset(&self, other: &HashSet<T, B>) -> bool { }

    //pub fn is_superset(&self, other: &HashSet<T, B>) -> bool { }

    pub fn insert(&mut self, value: T) -> bool {
        if self.contains(&value) {
            false
        } else {
            self.map.insert(value, ());
            true
        }
    }

    pub fn remove(&mut self, value: T) -> bool {
        self.map.remove(&value)
    }
}

pub struct Iter<'a, T, B>
    where T: 'a + Eq + Hash + Sized,
          B: 'a + Buffer<Elem<T, ()>>
{
    iter: ::map::Iter<'a, T, (), B>,
}

impl<'a, T, B> Iterator for Iter<'a, T, B>
    where T: 'a + Eq + Hash + Sized,
          B: 'a + Buffer<Elem<T, ()>>
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|x| Some(x.0))
    }
}

impl<T, B> fmt::Debug for HashSet<T, B>
    where T: Eq + Hash + Sized + fmt::Debug,
          B: Buffer<Elem<T, ()>>
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_set().entries(self.iter()).finish()
    }
}

