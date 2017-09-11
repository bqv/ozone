
use std::fs::OpenOptions;

use std::io::{Error, ErrorKind, Result, Write};
use std::path::Path;
use memmap::{Mmap, Protection};

pub struct Datastore {
    data: Mmap,
}

impl Datastore {
    pub fn new<P>(path: P) -> Result<Datastore>
        where P: AsRef<Path>
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        file.set_len(4096);
        let map = Mmap::open_path(path, Protection::ReadWrite)?;
        Ok(Datastore { data: map })
    }
}

#[test]
fn write_mmap() {
    let mut anon_map = Mmap::anonymous(4096, Protection::ReadWrite).unwrap();
    {
        let mut slice: &mut[u8] = unsafe { anon_map.as_mut_slice() };
        slice.write(b"hello world").unwrap();
    } // Lexical lifetime
    assert_eq!(b"hello world\0\0\0", unsafe { &anon_map.as_slice()[0..14] });
}

#[test]
fn create_datastore() {
    let ds = Datastore::new("test.db").expect("Failed creating file");
}

