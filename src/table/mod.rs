
/* Example Queries:
 *  --
 *  let expr1 = operators::eq(col2name, "foo");
 *  let expr2 = operators::between(col1name, &16, &32);
 *  let expr = operators::and(expr1, expr2);
 *  let where = operators::where(expr);
 *  db.select(
 *    &[col1name,col2name,col3name],
 *    tablename,
 *    &[where]
 *  )
 *  --
 */

mod btree;

use std::{mem, cmp, fmt};
use std::io::Result;
use std::path::Path;
use std::any::{Any, TypeId};

use buffer::{Buffer, AnonymousBuffer, FileBuffer};
use table::btree::BTree;

pub const PAGE_SIZE: usize = 4095;
pub const STRING_SIZE: usize = 256;

pub const PAGE_USED: u8 = !0;
pub const PAGE_AVAIL: u8 = 1;
pub const PAGE_FREE: u8 = 0;

#[derive(Copy)]
struct ByteString([u8; STRING_SIZE]);

#[macro_export]
macro_rules! bytestring {
    ($str:expr) => {{
        {
			let mut array = [0u8; STRING_SIZE];
            for (&x, p) in $str.as_bytes().iter().zip(array.iter_mut()) {
                *p = x;
            }
            ByteString(array)
        }
    }}
}

impl PartialEq for ByteString {
    fn eq(&self, other: &Self) -> bool {
        self.0[..] == other.0[..]
    }
}

impl PartialOrd for ByteString {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        PartialOrd::partial_cmp(&&self.0[..], &&other.0[..])
    }
}

impl fmt::Debug for ByteString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&&self.0[..], f)
    }
}

impl Clone for ByteString {
    fn clone(&self) -> Self {
        let mut array = [0u8; 256];
        for (&x, p) in self.0.iter().zip(array.iter_mut()) {
            *p = x;
        }
        ByteString(array)
    }
}

#[derive(Eq, PartialEq, Debug)]
enum PageType {
    Unallocated = 00,       // Free page: uninitialised data
    Metadata = 01,          // Metadata page: global variables
    PageTrunk = 02,         // Page data array: ArrayList<usize>(cursor)
    Directory = 03,         // Table directory: BPlusTree<ByteString, usize>(tblname, pageidx) -> ColumnDirectory
    IndexRoot = 04,         // Leaf pages tree: BPlusTree<Vn, usize>(keyval, pageidx) -> IndexLeaf
    IndexLeaf = 05,         // Index data tree: BPlusTree<Vn, (usize, usize)>(keyval, (pageidx,offset)) -> FreeListPage
    RowData = 06,           // Row values: FreeList<(size(V1),..,size(VN),V1,..,VN)>
}

/* Initial File Structure
 * ------------
 * Page0 - Metadata
 * Page1 - PageTrunk
 * Page2 - Directory
 * Page3 - Table0.name IndexRoot
 * Page4 -      ""     IndexLeaf
 * Page5 - RowData
 * Page6 - Unallocated
 * Page7 - Unallocated
 */

/* Table0 Schema
 * ------------
 * Column:  name     type   colnum  colnames       primary
 * Example: "root"   table  5       LinkedListPtr  0
 */

pub struct Page {
    data: [u8; PAGE_SIZE],
    typeid: PageType,
}

struct MetadataPage {
    journal: [u8; PAGE_SIZE], // Write ahead log
}

struct BPlusTreePage {
    btree: [u8; PAGE_SIZE - 4], // B+ Tree data
    cont: u32, // Next btree
}

struct ArrayListPage {
    data: [u8; PAGE_SIZE - 4], // Uninitialized space
    next: u8, // Next arraylist
}

struct FreeListPage {
    data: [u8; PAGE_SIZE - 8], // Uninitialized space
    cursor: u32, // Current cell
    next: u32, // Next freelist
}

pub struct Database<B>
    where B: Buffer<Page>
{
    buffer: B,
    size: usize,
}

#[derive(PartialOrd, PartialEq, Clone, Copy, Debug)]
pub enum TableType {
    Table,
    Index,
}

impl Database<AnonymousBuffer<Page>> {
    pub fn new() -> Self {
        Self::try_new().unwrap()
    }

    pub fn try_new() -> Result<Self> {
        let buffer = AnonymousBuffer::try_new(8 * mem::size_of::<Page>())?;
        let mut db = Database { buffer: buffer, size: 8 };
        db.init();
        Ok(db)
    }
}

impl Database<FileBuffer<Page>> {
    pub fn new<P>(path: P) -> Self
        where P: AsRef<Path> + Clone
    {
        Self::try_new(path).unwrap()
    }

    pub fn try_new<P>(path: P) -> Result<Self>
        where P: AsRef<Path> + Clone
    {
        let buffer = FileBuffer::try_new(path, 8 * mem::size_of::<Page>())?;
        let mut db = Database { buffer: buffer, size: 8 };
        db.init();
        Ok(db)
    }
}

#[test]
fn db_init() {
    let db = Database::<AnonymousBuffer<Page>>::new();
}

impl<B> Database<B>
    where B: Buffer<Page>
{
    fn metadata(&self) -> &'static MetadataPage {
        let metadata_page = &self.buffer[0];
        assert_eq!(metadata_page.typeid, PageType::Metadata);
        unsafe { &*(&metadata_page.data as *const _ as *const MetadataPage) }
    }

    fn metadata_mut(&mut self) -> &'static mut MetadataPage {
        let metadata_page = &mut self.buffer[0];
        metadata_page.typeid = PageType::Metadata;
        unsafe { &mut*(&mut metadata_page.data as *mut _ as *mut MetadataPage) }
    }

    fn arraylist(&self, page_ix: usize, pagetype: PageType) -> &'static ArrayListPage {
        let arraylist_page = &self.buffer[page_ix];
        assert_eq!(arraylist_page.typeid, pagetype);
        unsafe { &*(&arraylist_page.data as *const _ as *const ArrayListPage) }
    }

    fn arraylist_mut(&mut self, page_ix: usize, pagetype: PageType) -> &'static mut ArrayListPage {
        let arraylist_page = &mut self.buffer[page_ix];
        arraylist_page.typeid = pagetype;
        unsafe { &mut*(&mut arraylist_page.data as *mut _ as *mut ArrayListPage) }
    }

    fn freelist(&self, page_ix: usize, pagetype: PageType) -> &'static FreeListPage {
        let freelist_page = &self.buffer[page_ix];
        assert_eq!(freelist_page.typeid, pagetype);
        unsafe { &*(&freelist_page.data as *const _ as *const FreeListPage) }
    }

    fn freelist_mut(&mut self, page_ix: usize, pagetype: PageType) -> &'static mut FreeListPage {
        let freelist_page = &mut self.buffer[page_ix];
        freelist_page.typeid = pagetype;
        unsafe { &mut*(&mut freelist_page.data as *mut _ as *mut FreeListPage) }
    }

    fn bplustree(&self, page_ix: usize, pagetype: PageType) -> &'static BPlusTreePage {
        let bplustree_page = &self.buffer[page_ix];
        assert_eq!(bplustree_page.typeid, pagetype);
        unsafe { &*(&bplustree_page.data as *const _ as *const BPlusTreePage) }
    }

    fn bplustree_mut(&mut self, page_ix: usize, pagetype: PageType) -> &'static mut BPlusTreePage {
        let bplustree_page = &mut self.buffer[page_ix];
        bplustree_page.typeid = pagetype;
        unsafe { &mut*(&mut bplustree_page.data as *mut _ as *mut BPlusTreePage) }
    }

    fn init(&mut self) {
        let metadata = self.metadata_mut();
        for x in metadata.journal.iter_mut() {
            *x = 0;
        }

        let root_name = "ozone_root";
        let root_columns = [
            "name",
            "type",
            "colnum",
            "colnames",
            "primary"
        ];

        let trunk = self.arraylist_mut(1, PageType::PageTrunk);
        trunk.next = 0;
        for x in trunk.data.iter_mut() {
            *x = PAGE_FREE;
        }
        trunk.data[0] = PAGE_USED;
        trunk.data[1] = PAGE_USED;

        let page = self.bplustree_mut(2, PageType::Directory);
        let directory: &mut BTree<ByteString, usize> = BTree::create_from(&mut page.btree);
        trunk.data[2] = PAGE_USED;

        let page = self.bplustree_mut(3, PageType::IndexRoot);
        let col_name: &mut BTree<ByteString, usize> = BTree::create_from(&mut page.btree);
        directory.insert(bytestring!(root_name), 2);
        trunk.data[3] = PAGE_USED;

        let page = self.bplustree_mut(4, PageType::IndexLeaf);
        page.cont = 0;
        let idx_name: &mut BTree<ByteString, (usize, usize)> = BTree::create_from(&mut page.btree);
        col_name.insert(bytestring!(root_name), 3);
        trunk.data[4] = PAGE_USED;

        let row_data = self.freelist_mut(5, PageType::RowData);
        row_data.cursor = 0;
        trunk.data[5] = PAGE_AVAIL;

        let mut row = Vec::new();
        row.push(Entry::from(&root_name));
        row.push(Entry::from(&TableType::Table));
        row.push(Entry::from(&root_columns.len()));
        row.push(Entry::from(&root_columns));
        row.push(Entry::from(&0));
        let ptr = self.freelist_insert_row(&row);
        idx_name.insert(bytestring!(root_name), ptr);
    }

    fn freelist_insert(&mut self, data: &Entry) -> (usize, usize) {
        let ref data = Entry::Data(match *data {
            Entry::Data(ref vec) => vec.to_vec(),
            Entry::Entry(ref vec) => vec.iter().map(|x| self.freelist_insert(x))
                                           .fold(Vec::new(), |mut v, x| { v.extend(Entry::bytes(&x)); v })
        });
        let trunk = self.arraylist_mut(1, PageType::PageTrunk);
        let iter = trunk.data.iter().enumerate().filter(|&x| *x.1 != PAGE_USED);
        for (_, page_ix) in iter {
            if let Some(offset) = self.freelist_insert_into_page(*page_ix as usize, data) {
                return (*page_ix as usize, offset);
            }
        }
        unreachable!()
    }

    fn freelist_insert_into_page(&mut self, page_ix: usize, data: &Entry) -> Option<usize> {
        let row_data = self.freelist_mut(page_ix, PageType::RowData);
        let free_space = &row_data.data as *const _ as usize - &row_data.cursor as *const _ as usize;
        if data.size() > free_space {
            None
        } else {
            let offset = row_data.cursor as usize;
            let ref length = mem::size_of_val(data);
            let mut size = mem::size_of::<usize>();
            let mut ptr = length as *const _ as *const u8;
            while size > 0 {
                row_data.data[row_data.cursor as usize] = unsafe { *ptr };
                row_data.cursor += 1;
                ptr = unsafe { ptr.offset(1) };
                size -= 1;
            }
            let mut size = mem::size_of_val(data);
            let mut ptr = data as *const _ as *const u8;
            while size > 0 {
                row_data.data[row_data.cursor as usize] = unsafe { *ptr };
                row_data.cursor += 1;
                ptr = unsafe { ptr.offset(1) };
                size -= 1;
            }
            Some(offset)
        }
    }

    fn freelist_insert_row(&mut self, entry: &Vec<Entry>) -> (usize, usize) {
        let entry = entry.iter().map(|x| Entry::Data(match *x {
            Entry::Data(ref vec) => vec.to_vec(),
            Entry::Entry(ref vec) => vec.iter().map(|x| self.freelist_insert(x))
                                           .fold(Vec::new(), |mut v, x| { v.extend(Entry::bytes(&x)); v })
        })).collect::<Vec<_>>();
        let trunk = self.arraylist_mut(1, PageType::PageTrunk);
        let iter = trunk.data.iter().enumerate().filter(|&x| *x.1 != PAGE_USED);
        for (_, page_ix) in iter {
            if let Some(offset) = self.freelist_insert_row_into_page(*page_ix as usize, &entry) {
                return (*page_ix as usize, offset);
            }
        }
        unreachable!()
    }

    fn freelist_insert_row_into_page(&mut self, page_ix: usize, entry: &Vec<Entry>) -> Option<usize> {
        let row_data = self.freelist_mut(page_ix, PageType::RowData);
        let free_space = &row_data.data as *const _ as usize - &row_data.cursor as *const _ as usize;
        if entry.iter().fold(0, |sum, x| sum + x.size()) > free_space {
            None
        } else {
            let offset = row_data.cursor as usize;
            for value in entry {
                let ref length = mem::size_of_val(value);
                let mut size = mem::size_of::<usize>();
                let mut ptr = length as *const _ as *const u8;
                while size > 0 {
                    row_data.data[row_data.cursor as usize] = unsafe { *ptr };
                    row_data.cursor += 1;
                    ptr = unsafe { ptr.offset(1) };
                    size -= 1;
                }
            }
            for value in entry {
                let mut size = mem::size_of_val(value);
                let mut ptr = value as *const _ as *const u8;
                while size > 0 {
                    row_data.data[row_data.cursor as usize] = unsafe { *ptr };
                    row_data.cursor += 1;
                    ptr = unsafe { ptr.offset(1) };
                    size -= 1;
                }
            }
            Some(offset)
        }
    }

    fn create_page(&mut self, pagetype: PageType) -> usize {
        let iter = self.arraylist(1, PageType::PageTrunk).data.iter();
        for (i, &page_ix) in iter.enumerate() {
            match page_ix {
                PAGE_USED => { continue; },
                PAGE_AVAIL => { continue; },
                PAGE_FREE => {
                    if i > self.size {
                        unimplemented!();
                    } else {
                        let page = &mut self.buffer[i];
                        page.typeid = pagetype;
                        return i;
                    }
                },
                val => { panic!("Invalid value 0x{:0>8b} in page trunk", val); }
            }
        }
        unimplemented!()
    }
}

pub enum Entry {
    Entry(Vec<Entry>),
    Data(Vec<u8>),
}

impl Entry {
    pub fn new() -> Self {
        Entry::Entry(Vec::new())
    }

    fn size(&self) -> usize {
        match *self {
            Entry::Data(ref data) => data.len() + mem::size_of::<usize>(),
            Entry::Entry(ref entry) => {
                let mut size = 0;
                for value in entry {
                    size += mem::size_of::<usize>();
                    size += match *value {
                        Entry::Data(ref vec) => mem::size_of_val(value),
                        Entry::Entry(ref entry) => mem::size_of::<(usize, usize)>()
                    }
                }
                size
            }
        }
    }

    pub fn from<T>(value: &T) -> Self
        where T: Copy + Any
    {
        Entry::Data(Entry::bytes(value))
    }

    pub fn bytes<T>(value: &T) -> Vec<u8>
        where T: Copy + Any
    {
        let mut array = Vec::new();
        let mut size = mem::size_of_val(value);
        let mut ptr = value as *const _ as *const u8;
        while size > 0 {
            array.push(unsafe { *ptr });
            ptr = unsafe { ptr.offset(1) };
            size -= 1;
        }
        array
    }
}



