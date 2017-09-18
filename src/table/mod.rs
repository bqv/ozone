
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

use std::mem;
use std::io::Result;
use std::path::Path;

use buffer::{Buffer, AnonymousBuffer, FileBuffer};

pub const PAGE_SIZE: usize = 4096;

enum PageType {
    Unallocated = 00,       // Free page: uninitialised data
    Metadata = 01,          // Metadata page: global variables
    TableDirectory = 02,    // Table directory: B+Tree<str, usize>(tblidx, pageidx) -> ColumnDirectory
    ColumnDirectory = 03,   // Column directory: B+Tree<str, usize>(colidx, pageidx) -> IndexInterior
    IndexInterior = 04,     // Leaf pages tree: B+Tree<Kn, *const (K1,..,KN,V)>(keyval, pageidx) -> IndexLeaf
    IndexLeaf = 05,         // Index data: B+Tree<Kn, *const (K1,..,KN,V)>(keyval, (pageidx,offset)) -> Freelist
    Freelist = 06,          // Raw values: pointed to by indices
}

/* Page0 Schema
 * ------------
 * type     tblidx   name        colnum  colnames    rootpage
 * table    0        "root"      6       {ptr}       0
 */

pub struct Page {
    data: [u8; PAGE_SIZE],
    typeid: PageType,
    overflow: usize, // Index of overflow page
}

pub struct Metadata {
    next: usize, // Next free page
}

pub struct Database<B>
    where B: Buffer<Page>
{
    buffer: B,
}

impl Database<AnonymousBuffer<Page>> {
    pub fn new() -> Self {
        Self::try_new().unwrap()
    }

    pub fn try_new() -> Result<Self> {
        let buffer = AnonymousBuffer::try_new(2 * mem::size_of::<Page>())?;
        let mut db = Database { buffer: buffer };
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
        let buffer = FileBuffer::try_new(path, 2 * mem::size_of::<Page>())?;
        let mut db = Database { buffer: buffer };
        db.init();
        Ok(db)
    }
}

impl<B> Database<B>
    where B: Buffer<Page>
{
    fn init(&mut self) {
    }
}

