
extern crate memmap;
extern crate rand;

mod set;
mod map;
mod table;
mod buffer;

use set::{HashSet};
use map::{HashMap, Elem};
use buffer::{AnonymousBuffer, FileBuffer};

pub type SwapBackedHashMap<K, V> = HashMap<K, V, AnonymousBuffer<Elem<K, V>>>;
pub type FileBackedHashMap<K, V> = HashMap<K, V, FileBuffer<Elem<K, V>>>;
pub type SwapBackedHashSet<T> = HashSet<T, AnonymousBuffer<Elem<T, ()>>>;
pub type FileBackedHashSet<T> = HashSet<T, FileBuffer<Elem<T, ()>>>;

