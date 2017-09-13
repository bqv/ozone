
#[allow(unused_imports)] use std::io::{Result, Write};
use std::fs::{OpenOptions, remove_file};
use std::path::{Path, PathBuf};
use memmap::{Mmap, Protection};
use std::marker::PhantomData;
use std::ops::{Index, IndexMut};
use std::sync::{Arc, Mutex};

pub trait Buffer<T>: Index<usize, Output = T> + IndexMut<usize> + Clone + Sized
{
    fn new_sized(&self, usize) -> Result<Self>;
}

pub struct AnonymousBuffer<T>
    where T: Sized
{
    data: Arc<Mutex<Mmap>>,
    phantom: PhantomData<T>,
}

impl<T> AnonymousBuffer<T>
    where T: Sized
{
    pub fn try_new(size: usize) -> Result<Self>
    {
        let map = Mmap::anonymous(size, Protection::ReadWrite)?;
        Ok(Self { data: Arc::new(Mutex::new(map)), phantom: PhantomData })
    }
}

impl<T> Index<usize> for AnonymousBuffer<T>
    where T: Sized
{
    type Output = T;

    fn index(&self, idx: usize) -> &Self::Output {
        unsafe {
            let mut count = idx;
            let mut p: *const T = self.data.lock().unwrap().ptr() as *const T;
            while count > 0 {
                count -= 1;
                p = p.offset(1);
            }
            &*p
        }
    }
}

impl<T> IndexMut<usize> for AnonymousBuffer<T>
    where T: Sized
{
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        unsafe {
            let mut count = idx;
            let mut p: *mut T = self.data.lock().unwrap().mut_ptr() as *mut T;
            while count > 0 {
                count -= 1;
                p = p.offset(1);
            }
            &mut*p
        }
    }
}

impl<T> Clone for AnonymousBuffer<T>
    where T: Sized
{
    fn clone(&self) -> Self {
        AnonymousBuffer { data: self.data.clone(), phantom: PhantomData }
    }
}

impl<T> Buffer<T> for AnonymousBuffer<T>
    where T: Sized
{
    fn new_sized(&self, size: usize) -> Result<Self> {
        Self::try_new(size)
    }
}

pub struct FileBuffer<T>
    where T: Sized
{
    data: Arc<Mutex<Mmap>>,
    path: PathBuf,
    phantom: PhantomData<T>,
}

impl<T> FileBuffer<T>
    where T: Sized
{
    pub fn try_new<P>(path: P, size: usize) -> Result<Self>
        where P: AsRef<Path> + Clone
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        file.set_len(0)?;
        file.set_len(size as u64)?;
        let map = Mmap::open_path(path.clone(), Protection::ReadWrite)?;
        Ok(Self { data: Arc::new(Mutex::new(map)), path: path.as_ref().to_owned(), phantom: PhantomData })
    }
}

impl<T> Index<usize> for FileBuffer<T>
    where T: Sized
{
    type Output = T;

    fn index(&self, idx: usize) -> &Self::Output {
        unsafe {
            let mut count = idx;
            let mut p: *const T = self.data.lock().unwrap().ptr() as *const T;
            while count > 0 {
                count -= 1;
                p = p.offset(1);
            }
            &*p
        }
    }
}

impl<T> IndexMut<usize> for FileBuffer<T>
    where T: Sized
{
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        unsafe {
            let mut count = idx;
            let mut p: *mut T = self.data.lock().unwrap().mut_ptr() as *mut T;
            while count > 0 {
                count -= 1;
                p = p.offset(1);
            }
            &mut*p
        }
    }
}

impl<T> Clone for FileBuffer<T>
    where T: Sized
{
    fn clone(&self) -> Self {
        FileBuffer { data: self.data.clone(), path: self.path.clone(), phantom: PhantomData }
    }
}

impl<T> Buffer<T> for FileBuffer<T>
    where T: Sized
{
    fn new_sized(&self, size: usize) -> Result<Self> {
        remove_file(self.path.clone())?;
        Self::try_new(self.path.clone(), size)
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
fn create_file_buffer() {
    let _fb: FileBuffer<u8> = FileBuffer::try_new("test.db", 4096).expect("Failed creating file");
}

#[test]
fn create_anonymous_buffer() {
    let _ab: AnonymousBuffer<u8> = AnonymousBuffer::try_new(4096).expect("Failed creating file");
}

