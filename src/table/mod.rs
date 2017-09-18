
/* Example Queries:
 *  --
 *  let expr1 = operators::eq(col2name, "foo");
 *  let expr2 = operators::lt(col1name, &32);
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

use buffer::{Buffer, AnonymousBuffer, FileBuffer};

pub const PAGE_SIZE: usize = 4096;

enum PageType {
    Unallocated = 00, // Free page: uninitialised data
    Directory = 01,   // BPTree<(&str,&str), usize>((tblidx,colidx), pageidx)
    Index = 02,       // BPTree<Kn, *const (K1,..,KN,V)>(keyval, record)
    Freelist = 03,    // Raw data pointed to by indices
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

pub struct Database<B>
    where B: Buffer<Page>
{
    buffer: B,
}

impl<B> Database<B>
    where B: Buffer<Page>
{
}

