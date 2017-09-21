
use std::{mem, fmt, panic};
use buffer::{Buffer, AnonymousBuffer, FileBuffer};
use table::{Page};
use rand::{Rng, StdRng};

const ORDER: usize = 3;

#[derive(Debug)]
pub struct Free {
    next: isize,
}

#[derive(Debug)]
pub struct Node<K>
    where K: PartialOrd + Copy + Sized + fmt::Debug,
{
    leaf: bool,
    key_count: u8,
	keys: [K; ORDER - 1],
    ptrs: [isize; ORDER],
	parent: Option<isize>,
}

#[derive(Debug)]
pub struct Bucket<V>
    where V: Copy + Sized + fmt::Debug,
{
    values: [V; 1]
}

#[derive(Debug)]
struct Meta<K, V>
    where K: PartialOrd + Copy + Sized + fmt::Debug,
          V: Copy + Sized + fmt::Debug,
{
    start: *mut Block<K, V>,
    next: isize,
    end: *mut Block<K, V>,
    root: isize,
}

#[derive(Debug)]
enum Block<K, V>
    where K: PartialOrd + Copy + Sized + fmt::Debug,
          V: Copy + Sized + fmt::Debug,
{
    Free(Free),
    Node(Node<K>),
    Bucket(Bucket<V>),
    Meta(Meta<K, V>),
}

impl<K, V> Block<K, V>
    where K: PartialOrd + Copy + Sized + fmt::Debug,
          V: Copy + Sized + fmt::Debug,
{
    fn as_free(&self) -> &Free {
        match *self {
            Block::Free(ref free) => free,
            Block::Node(_) => panic!("as_free() called on node"),
            Block::Bucket(_) => panic!("as_free() called on bucket"),
            Block::Meta(_) => panic!("as_free() called on meta")
        }
    }

    fn as_free_mut(&mut self) -> &mut Free {
        match *self {
            Block::Free(ref mut free) => free,
            Block::Node(_) => panic!("as_free_mut() called on node"),
            Block::Bucket(_) => panic!("as_free_mut() called on bucket"),
            Block::Meta(_) => panic!("as_free_mut() called on meta")
        }
    }

    fn as_node(&self) -> &Node<K> {
        match *self {
            Block::Free(_) => panic!("as_node() called on free"),
            Block::Node(ref node) => node,
            Block::Bucket(_) => panic!("as_node() called on bucket"),
            Block::Meta(_) => panic!("as_node() called on meta")
        }
    }

    fn as_node_mut(&mut self) -> &mut Node<K> {
        match *self {
            Block::Free(_) => panic!("as_node_mut() called on free"),
            Block::Node(ref mut node) => node,
            Block::Bucket(_) => panic!("as_node_mut() called on bucket"),
            Block::Meta(_) => panic!("as_node_mut() called on meta")
        }
    }

    fn as_bucket(&self) -> &Bucket<V> {
        match *self {
            Block::Free(_) => panic!("as_bucket() called on free"),
            Block::Node(_) => panic!("as_bucket() called on node"),
            Block::Bucket(ref bucket) => bucket,
            Block::Meta(_) => panic!("as_bucket() called on meta")
        }
    }

    fn as_bucket_mut(&mut self) -> &mut Bucket<V> {
        match *self {
            Block::Free(_) => panic!("as_bucket_mut() called on free"),
            Block::Node(_) => panic!("as_bucket_mut() called on node"),
            Block::Bucket(ref mut bucket) => bucket,
            Block::Meta(_) => panic!("as_bucket_mut() called on meta")
        }
    }

    fn as_meta(&self) -> &Meta<K, V> {
        match *self {
            Block::Free(_) => panic!("as_meta() called on free"),
            Block::Node(_) => panic!("as_meta() called on node"),
            Block::Bucket(_) => panic!("as_meta() called on bucket"),
            Block::Meta(ref meta) => meta
        }
    }

    fn as_meta_mut(&mut self) -> &mut Meta<K, V> {
        match *self {
            Block::Free(_) => panic!("as_meta_mut() called on free"),
            Block::Node(_) => panic!("as_meta_mut() called on node"),
            Block::Bucket(_) => panic!("as_meta_mut() called on bucket"),
            Block::Meta(ref mut meta) => meta
        }
    }
}

pub struct BTree<K, V>
    where K: PartialOrd + Copy + Sized + fmt::Debug,
          V: Copy + Sized + fmt::Debug,
{
    meta: Block<K, V>,
}

impl<K, V> BTree<K, V>
    where K: PartialOrd + Copy + Sized + fmt::Debug,
          V: Copy + Sized + fmt::Debug,
{
}

impl<K, V> BTree<K, V>
    where K: PartialOrd + Copy + Sized + fmt::Debug,
          V: Copy + Sized + fmt::Debug,
{
    #[inline]
    unsafe fn is_leaf(node: *const Block<K, V>) -> bool {
        match *node {
            Block::Free(_) => unreachable!(),
            Block::Node(ref node) => node.leaf,
            Block::Bucket(_) => unreachable!(),
            Block::Meta(_) => unreachable!()
        }
    }

    #[inline]
    unsafe fn parent(node: *const Block<K, V>) -> Option<isize> {
        match *node {
            Block::Free(_) => unreachable!(),
            Block::Node(ref node) => node.parent,
            Block::Bucket(_) => unreachable!(),
            Block::Meta(_) => unreachable!()
        }
    }

    #[inline]
    unsafe fn parent_mut(node: *mut Block<K, V>) -> *mut Option<isize> {
        match *node {
            Block::Free(_) => unreachable!(),
            Block::Node(ref mut node) => &mut node.parent,
            Block::Bucket(_) => unreachable!(),
            Block::Meta(_) => unreachable!()
        }
    }

    #[inline]
    unsafe fn num_keys(node: *const Block<K, V>) -> u8 {
        match *node {
            Block::Free(_) => unreachable!(),
            Block::Node(ref node) => node.key_count,
            Block::Bucket(_) => unreachable!(),
            Block::Meta(_) => unreachable!()
        }
    }

    #[inline]
    unsafe fn num_keys_mut(node: *mut Block<K, V>) -> *mut u8 {
        match *node {
            Block::Free(_) => unreachable!(),
            Block::Node(ref mut node) => &mut node.key_count,
            Block::Bucket(_) => unreachable!(),
            Block::Meta(_) => unreachable!()
        }
    }

    #[inline]
    unsafe fn nth_key(node: *const Block<K, V>, n: usize) -> *const K {
        match *node {
            Block::Free(_) => unreachable!(),
            Block::Node(ref node) => &node.keys[n],
            Block::Bucket(_) => unreachable!(),
            Block::Meta(_) => unreachable!()
        }
    }

    #[inline]
    unsafe fn nth_key_mut(node: *mut Block<K, V>, n: usize) -> *mut K {
        match *node {
            Block::Free(_) => unreachable!(),
            Block::Node(ref mut node) => &mut node.keys[n],
            Block::Bucket(_) => unreachable!(),
            Block::Meta(_) => unreachable!()
        }
    }

    #[inline]
    unsafe fn nth_ptr(node: *const Block<K, V>, n: usize) -> *const isize {
        match *node {
            Block::Free(_) => unreachable!(),
            Block::Node(ref node) => &node.ptrs[n],
            Block::Bucket(_) => unreachable!(),
            Block::Meta(_) => unreachable!()
        }
    }

    #[inline]
    unsafe fn nth_ptr_mut(node: *mut Block<K, V>, n: usize) -> *mut isize {
        match *node {
            Block::Free(_) => unreachable!(),
            Block::Node(ref mut node) => &mut node.ptrs[n],
            Block::Bucket(_) => unreachable!(),
            Block::Meta(_) => unreachable!()
        }
    }

    unsafe fn find_leaf(&self, key: &K) -> Option<*const Block<K, V>> {
        let mut i = 0usize;
        let mut c = self.meta.as_meta().start.offset(self.meta.as_meta().root) as *const _;
        while !Self::is_leaf(c) {
            i = 0;
            while i < Self::num_keys(c) as usize {
                if *key >= *Self::nth_key(c, i) {
                    i += 1;
                } else {
                    break;
                }
            }
            c = (self.meta.as_meta().start as *const _).offset(*Self::nth_ptr(c, i));
            if c == self.meta.as_meta().start {
                return None
            }
        }
        Some(c)
    }

    unsafe fn find_leaf_mut(&mut self, key: &K) -> Option<*mut Block<K, V>> {
        let mut i = 0usize;
        let mut c = self.meta.as_meta().start.offset(self.meta.as_meta().root);
        while !Self::is_leaf(c) {
            i = 0;
            while i < Self::num_keys(c) as usize {
                if *key >= *Self::nth_key(c, i) {
                    i += 1;
                } else {
                    break;
                }
            }
            c = self.meta.as_meta().start.offset(*Self::nth_ptr(c, i));
            if c == self.meta.as_meta().start {
                return None
            }
        }
        Some(c)
    }

    unsafe fn range_find_blocks<'a>(&self, key_start: &K, key_end: &K) -> Vec<(&K, &'a Block<K, V>)> {
        let mut found = Vec::new();
        if let Some(mut n) = self.find_leaf(key_start) {
            let mut i = 0;
            while i < Self::num_keys(n) as usize && *Self::nth_key(n, i) < *key_start {
                i += 1;
            }
            if i != Self::num_keys(n) as usize {
                loop {
                    for j in i..Self::num_keys(n) as usize {
                        let key = Self::nth_key(n, j);
                        if *key > *key_end {
                            return found;
                        }
                        let value = self.meta.as_meta().start.offset(*Self::nth_ptr(n, j));
                        found.push((&*key, &*value));
                    }
                    let ptr = *Self::nth_ptr(n, ORDER - 1);
                    if ptr == 0 {
                        break;
                    } else {
                        n = self.meta.as_meta().start.offset(ptr);
                    }
                    i = 0;
                }
            }
        }
        found

    }

    unsafe fn find_block<'a>(&self, key: &K) -> Option<&'a Block<K, V>> {
        let mut i = 0;
        if let Some(mut c) = self.find_leaf(key) {
            for j in 0..Self::num_keys(c) {
                i = j as usize;
                if *Self::nth_key(c, i) == *key {
                    break;
                }
            }
            if *Self::nth_key(c, i) == *key {
                Some(&*(self.meta.as_meta().start as *const _).offset(*Self::nth_ptr(c, i)))
            } else {
                None
            }
        } else {
            None
        }
    }

    unsafe fn find_block_mut<'a>(&mut self, key: &K) -> Option<&'a mut Block<K, V>> {
        let mut i = 0;
        if let Some(mut c) = self.find_leaf_mut(key) {
            for j in 0..Self::num_keys(c) {
                i = j as usize;
                if *Self::nth_key(c, i) == *key {
                    break;
                }
            }
            if *Self::nth_key(c, i) == *key {
                Some(&mut*(self.meta.as_meta().start as *mut _).offset(*Self::nth_ptr(c, i)))
            } else {
                None
            }
        } else {
            None
        }
    }

    #[inline]
    fn cut(length: usize) -> usize {
        if length % 2 == 0 {
            length / 2
        } else {
            length / 2 + 1
        }
    }

    #[inline]
    fn offset_to(from: *const Block<K, V>, to: *const Block<K, V>) -> isize {
        ((to as isize) - (from as isize)) / mem::size_of::<Block<K, V>>() as isize
    }

    unsafe fn make_node(&mut self) -> isize {
        let new_next = self.meta.as_meta_mut().start.offset(self.meta.as_meta_mut().next + 1);
        if new_next > self.meta.as_meta().end {
            unreachable!()
        } else {
            let next = self.meta.as_meta().start.offset(self.meta.as_meta().next);
            self.meta.as_meta_mut().next = (*next).as_free().next;
            let new_node = &mut*next;
            *new_node = Block::Node( Node {
                leaf: false,
                key_count: 0,
                keys: mem::uninitialized(),
                ptrs: [0; ORDER],
                parent: None,
            });
            Self::offset_to(self.meta.as_meta_mut().start as *const _, new_node as *const _)
        }
    }

    unsafe fn make_leaf(&mut self) -> isize {
        let new_next = self.meta.as_meta_mut().start.offset(self.meta.as_meta_mut().next + 1);
        if new_next > self.meta.as_meta().end {
            unreachable!()
        } else {
            let next = self.meta.as_meta().start.offset(self.meta.as_meta().next);
            self.meta.as_meta_mut().next = (*next).as_free().next;
            let new_node = &mut*next;
            *new_node = Block::Node( Node {
                leaf: true,
                key_count: 0,
                keys: mem::uninitialized(),
                ptrs: [0; ORDER],
                parent: None,
            });
            Self::offset_to(self.meta.as_meta_mut().start as *const _, new_node as *const _)
        }
    }

    unsafe fn make_bucket(&mut self) -> isize {
        let new_next = self.meta.as_meta_mut().start.offset(self.meta.as_meta_mut().next + 1);
        if new_next > self.meta.as_meta().end {
            unreachable!()
        } else {
            let next = self.meta.as_meta().start.offset(self.meta.as_meta().next);
            self.meta.as_meta_mut().next = (*next).as_free().next;
            let new_bucket = &mut*next;
            *new_bucket = Block::Bucket( Bucket {
                values: mem::uninitialized(),
            });
            Self::offset_to(self.meta.as_meta_mut().start as *const _, new_bucket as *const _)
        }
    }

    unsafe fn get_left_index(&self, parent: *const Block<K, V>, left: *const Block<K, V>) -> usize {
        let mut left_index = 0;
        while left_index <= Self::num_keys(parent) as usize && *Self::nth_ptr(parent, left_index) != Self::offset_to(self.meta.as_meta().start as *const _, left) {
            left_index += 1;
        }
        left_index
    }

    unsafe fn insert_into_leaf(&mut self, leaf: *mut Block<K, V>, key: K, value: *mut Block<K, V>) {
        let mut insertion_point = 0;
        while insertion_point < Self::num_keys(leaf) as usize && *Self::nth_key(leaf, insertion_point) < key {
            insertion_point += 1;
        }

        for i in (insertion_point..Self::num_keys(leaf) as usize).rev() {
            let k_old = Self::nth_key_mut(leaf, i+1);
            let k_new = Self::nth_key_mut(leaf, i);
            *k_old = *k_new;
            let p_old = Self::nth_ptr_mut(leaf, i+1);
            let p_new = Self::nth_ptr_mut(leaf, i);
            *p_old = *p_new;
        }
        let k_slot = Self::nth_key_mut(leaf, insertion_point);
        *k_slot = key;
        let p_slot = Self::nth_ptr_mut(leaf, insertion_point);
        *p_slot = Self::offset_to(self.meta.as_meta_mut().start, value);
        let n_keys = Self::num_keys_mut(leaf);
        *n_keys += 1;
    }

    unsafe fn split_and_insert_into_leaf(&mut self, leaf: *mut Block<K, V>, key: K, value: *mut Block<K, V>) {
        let mut temp_keys: [K; ORDER] = mem::uninitialized();
        let mut temp_ptrs: [isize; ORDER] = mem::uninitialized();

        let mut insertion_index = 0;
        let nk = Self::num_keys(leaf);
        while (insertion_index + 1) < ORDER as usize && *Self::nth_key(leaf, insertion_index) < key {
            insertion_index += 1;
        }

        for t in (0..(nk as usize)).zip((0..).filter(|&x| x != insertion_index)) {
            let k_old = &mut temp_keys[t.1];
            let k_new = Self::nth_key_mut(leaf, t.0);
            *k_old = *k_new;
            let p_old = &mut temp_ptrs[t.1];
            let p_new = Self::nth_ptr_mut(leaf, t.0);
            *p_old = *p_new;
        }
        temp_keys[insertion_index] = key;
        temp_ptrs[insertion_index] = Self::offset_to(self.meta.as_meta_mut().start, value);
        let mut n_keys = Self::num_keys_mut(leaf);
        *n_keys = 0;

        let split = ORDER / 2;
        for i in 0..split {
            let k_old = Self::nth_key_mut(leaf, i);
            let k_new = &mut temp_keys[i];
            *k_old = *k_new;
            let p_old = Self::nth_ptr_mut(leaf, i);
            let p_new = &mut temp_ptrs[i];
            *p_old = *p_new;
            *n_keys += 1;
        }

        let new_leaf = self.meta.as_meta_mut().start.offset(self.make_leaf());
        n_keys = Self::num_keys_mut(new_leaf);
        for t in (split..ORDER).zip(0..) {
            let k_old = Self::nth_key_mut(new_leaf, t.1);
            let k_new = &mut temp_keys[t.0];
            *k_old = *k_new;
            let p_old = Self::nth_ptr_mut(new_leaf, t.1);
            let p_new = &mut temp_ptrs[t.0];
            *p_old = *p_new;
            *n_keys += 1;
        }
        let p_next_old = Self::nth_ptr_mut(new_leaf, ORDER - 1);
        let p_next_new = Self::nth_ptr_mut(leaf, ORDER - 1);
        *p_next_old = *p_next_new;
        let p_next_old = Self::nth_ptr_mut(leaf, ORDER - 1);
        let p_next_new = Self::offset_to(self.meta.as_meta_mut().start, new_leaf);
        *p_next_old = p_next_new;

        for i in Self::num_keys(leaf) as usize..(ORDER - 1) {
            let ptr = Self::nth_ptr_mut(leaf, i);
            *ptr = 0;
        }
        for i in Self::num_keys(new_leaf) as usize..(ORDER - 1) {
            let ptr = Self::nth_ptr_mut(new_leaf, i);
            *ptr = 0;
        }

        let parent_ptr = Self::parent_mut(new_leaf);
        let leaf_parent = Self::parent(leaf);
        *parent_ptr = leaf_parent;
        let new_key = *Self::nth_key(new_leaf, 0);
        self.insert_into_parent(leaf, new_key, new_leaf)
    }

    unsafe fn insert_into_node(&mut self, n: *mut Block<K, V>, left_index: usize, key: K, right: *mut Block<K, V>) {
        for i in (left_index..Self::num_keys(n) as usize).rev() {
            let k_old = Self::nth_key_mut(n, i + 1);
            let k_new = Self::nth_key_mut(n, i + 0);
            *k_old = *k_new;
            let p_old = Self::nth_ptr_mut(n, i + 2);
            let p_new = Self::nth_ptr_mut(n, i + 1);
            *p_old = *p_new;
        }
        let p_old = Self::nth_ptr_mut(n, left_index + 1);
        *p_old = Self::offset_to(self.meta.as_meta_mut().start, right);
        let k_old = Self::nth_key_mut(n, left_index);
        *k_old = key;
        let n_keys = Self::num_keys_mut(n);
        *n_keys += 1;
    }

    unsafe fn split_and_insert_into_node(&mut self, old_node: *mut Block<K, V>, left_index: usize, key: K, right: *mut Block<K, V>) {
        let mut temp_ptrs: [isize; ORDER + 1] = mem::uninitialized();
        let mut temp_keys: [K; ORDER] = mem::uninitialized();
        let nk = Self::num_keys(old_node as *const _) as usize;
        for t in (0..(nk + 1)).zip((0..).filter(|&x| x != left_index + 1)) {
            let p = Self::nth_ptr_mut(old_node, t.0);
            temp_ptrs[t.1] = *p;
        }
        for t in (0..nk).zip((0..).filter(|&x| x != left_index)) {
            let k = Self::nth_key_mut(old_node, t.0);
            temp_keys[t.1] = *k;
        }
        temp_ptrs[left_index + 1] = Self::offset_to(self.meta.as_meta_mut().start, right);
        temp_keys[left_index] = key;

        let split = Self::cut(ORDER - 1);
        let new_node = self.meta.as_meta_mut().start.offset(self.make_node());
        let mut n_keys = Self::num_keys_mut(old_node);
        *n_keys = 0;
        for i in 0..split {
            let k_old = Self::nth_key_mut(old_node, i);
            let k_new = &mut temp_keys[i];
            *k_old = *k_new;
            let p_old = Self::nth_ptr_mut(old_node, i);
            let p_new = &mut temp_ptrs[i];
            *p_old = *p_new;
            *n_keys += 1;
        }
        {
            let p_old = Self::nth_ptr_mut(old_node, split);
            let p_new = &mut temp_ptrs[split];
            *p_old = *p_new;
        }
        let mut pivot = temp_keys[split];
        n_keys = Self::num_keys_mut(new_node);
        for t in ((split + 1)..ORDER).zip(0..) {
            let k_old = Self::nth_key_mut(new_node, t.1);
            let k_new = &mut temp_keys[t.0];
            *k_old = *k_new;
            let p_old = Self::nth_ptr_mut(new_node, t.1);
            let p_new = &mut temp_ptrs[t.0];
            *p_old = *p_new;
            *n_keys += 1;
        }
        {
            let p_old = Self::nth_ptr_mut(new_node, ORDER - split - 1);
            let p_new = &mut temp_ptrs[ORDER];
            *p_old = *p_new;
        }
        let parent_ptr = Self::parent_mut(new_node);
        let node_parent = Self::parent(old_node);
        *parent_ptr = node_parent;
        for i in 0..(Self::num_keys(new_node) as usize + 1) {
            let child = self.meta.as_meta_mut().start.offset(*Self::nth_ptr(new_node, i));
            let parent = Self::parent_mut(child);
            *parent = Some(Self::offset_to(self.meta.as_meta_mut().start, new_node));
        }

        self.insert_into_parent(old_node, pivot, new_node)
    }

    unsafe fn insert_into_parent(&mut self, left: *mut Block<K, V>, key: K, right: *mut Block<K, V>) {
        let parent = Self::parent(left);
        if parent.is_none() {
            self.insert_into_new_root(left, key, right)
        } else {
            let parent = self.meta.as_meta_mut().start.offset(parent.unwrap());
            let left_index = self.get_left_index(parent, left);
            
            if (Self::num_keys(parent) as usize + 1) < ORDER {
                self.insert_into_node(parent, left_index, key, right)
            } else {
                self.split_and_insert_into_node(parent, left_index, key, right)
            }
        }
    }

    unsafe fn insert_into_new_root(&mut self, left: *mut Block<K, V>, key: K, right: *mut Block<K, V>) {
        let root = self.make_node();
        let prnt = Self::parent_mut(left);
        *prnt = Some(root);
        let prnt = Self::parent_mut(right);
        *prnt = Some(root);
        self.meta.as_meta_mut().root = root;

        let root = self.meta.as_meta_mut().start.offset(root);
        let r_key0 = Self::nth_key_mut(root, 0);
        *r_key0 = key;
        let r_ptr0 = Self::nth_ptr_mut(root, 0);
        *r_ptr0 = Self::offset_to(self.meta.as_meta_mut().start, left);
        let r_ptr1 = Self::nth_ptr_mut(root, 1);
        *r_ptr1 = Self::offset_to(self.meta.as_meta_mut().start, right);
        let r_nkey = Self::num_keys_mut(root);
        *r_nkey += 1;
        let r_prnt = Self::parent_mut(root);
        *r_prnt = None;
    }

    unsafe fn start_new_tree(&mut self, key: K, value: *mut Block<K, V>) {
        let root = self.make_leaf();
        self.meta.as_meta_mut().root = root;
        let root = self.meta.as_meta_mut().start.offset(root);
        let r_key0 = Self::nth_key_mut(root, 0);
        *r_key0 = key;
        let r_ptr0 = Self::nth_ptr_mut(root, 0);
        *r_ptr0 = Self::offset_to(self.meta.as_meta_mut().start, value);
        let r_nkey = Self::num_keys_mut(root);
        *r_nkey = 1;
    }

    unsafe fn get_neighbour_index(&self, n: *mut Block<K, V>) -> Option<usize> {
        let nparent = self.meta.as_meta().start.offset(Self::parent(n).unwrap());
        for i in 0..(Self::num_keys(nparent) as usize + 1) {
            let neighbour = Self::nth_ptr(nparent, i);
            if self.meta.as_meta().start.offset(*neighbour) == n {
                return if i == 0 { None } else { Some(i - 1) };
            }
        }
        unreachable!("Search for nonexistent pointer to node in parent")
    }

    unsafe fn remove_entry_from_node(&mut self, n: *mut Block<K, V>, key: &K, value: *mut Block<K, V>) -> usize {
        let mut key_ix = 0;
        for i in 0..Self::num_keys(n) as usize {
            if *Self::nth_key(n, i) == *key {
                key_ix = i;
                break;
            }
        }
        for i in (key_ix + 1)..Self::num_keys(n) as usize {
            let k_old = Self::nth_key_mut(n, i - 1);
            let k_new = Self::nth_key_mut(n, i);
            *k_old = *k_new;
        }

        let num_pointers = if Self::is_leaf(n) { Self::num_keys(n) } else { Self::num_keys(n) + 1 } as usize;
        let mut ptr_ix = 0;
        for i in 0..(Self::num_keys(n) as usize + 1) {
            if *Self::nth_ptr(n, i) == Self::offset_to(self.meta.as_meta().start, value) {
                ptr_ix = i;
                break;
            }
        }
        for i in (ptr_ix + 1)..num_pointers {
            let p_old = Self::nth_ptr_mut(n, i - 1);
            let p_new = Self::nth_ptr_mut(n, i);
            *p_old = *p_new;
        }

        let n_keys = Self::num_keys_mut(n);
        *n_keys -= 1;

        if Self::is_leaf(n) {
            for i in Self::num_keys(n) as usize..(ORDER - 1) {
                let ptr = Self::nth_ptr_mut(n, i);
                *ptr = 0;
            }
        } else {
            for i in (Self::num_keys(n) as usize + 1)..ORDER {
                let ptr = Self::nth_ptr_mut(n, i);
                *ptr = 0;
            }
        }
        key_ix
    }

    unsafe fn adjust_root(&mut self) {
        let root = self.meta.as_meta_mut().start.offset(self.meta.as_meta().root);
        if Self::num_keys(root) == 0 {
            if Self::is_leaf(root) {
                self.meta.as_meta_mut().root = 0;
            } else {
                let new_root = Self::nth_ptr(root, 0);
                self.meta.as_meta_mut().root = *new_root;
                let parent = Self::parent_mut(self.meta.as_meta().start.offset(*new_root));
                *parent = None;
            }
            *root = Block::Free(Free { next: self.meta.as_meta().next });
            self.meta.as_meta_mut().next = Self::offset_to(self.meta.as_meta().start, root);
        }
    }

    unsafe fn merge_nodes(&mut self, mut n: *mut Block<K, V>, mut neighbour: *mut Block<K, V>, neighbour_index: Option<usize>, pivot: *const K) {
        for i in 0..Self::offset_to(self.meta.as_meta().start, self.meta.as_meta().end) {
        }
        if neighbour_index.is_none() {
            let tmp = n;
            n = neighbour;
            neighbour = tmp;
        }

        let neighbour_insertion_index = Self::num_keys(neighbour) as usize;

        if !Self::is_leaf(n) {
            let k_nei = Self::nth_key_mut(neighbour, neighbour_insertion_index);
            *k_nei = *pivot;
            let n_key = Self::num_keys_mut(neighbour);
            *n_key += 1;

            let n_end = Self::num_keys(n) as usize;

            for t in ((neighbour_insertion_index + 1)..).zip(0..n_end) {
                let k_old = Self::nth_key_mut(neighbour, t.0);
                let k_new = Self::nth_key_mut(n, t.1);
                *k_old = *k_new;
                let p_old = Self::nth_ptr_mut(neighbour, t.0);
                let p_new = Self::nth_ptr_mut(n, t.1);
                *p_old = *p_new;
                let nei_keys = Self::num_keys_mut(neighbour);
                let n_keys = Self::num_keys_mut(n);
                *nei_keys += 1;
                *n_keys -= 1;
            }

            let p_old = Self::nth_ptr_mut(neighbour, neighbour_insertion_index + n_end + 1);
            let p_new = Self::nth_ptr_mut(n, n_end);
            *p_old = *p_new;

            for i in 0..(Self::num_keys(neighbour) as usize + 1) {
                let offset = *Self::nth_ptr(neighbour, i);
                let node = self.meta.as_meta().start.offset(offset);
                let parent = Self::parent_mut(node);
                *parent = Some(Self::offset_to(self.meta.as_meta().start, neighbour));
            }
        } else {
            for t in (neighbour_insertion_index..).zip(0..Self::num_keys(n) as usize) {
                let k_old = Self::nth_key_mut(neighbour, t.0);
                let k_new = Self::nth_key_mut(n, t.1);
                *k_old = *k_new;
                let p_old = Self::nth_ptr_mut(neighbour, t.0);
                let p_new = Self::nth_ptr_mut(n, t.1);
                *p_old = *p_new;
                let nei_keys = Self::num_keys_mut(neighbour);
                *nei_keys += 1;
            }
            let p_old = Self::nth_ptr_mut(neighbour, ORDER - 1);
            let p_new = Self::nth_ptr_mut(n, ORDER - 1);
            *p_old = *p_new;
        }
        for i in 0..Self::offset_to(self.meta.as_meta().start, self.meta.as_meta().end) {
        }

        let parent = self.meta.as_meta().start.offset(Self::parent(n).unwrap());
        self.delete_entry(parent, &*pivot, n);
        *n = Block::Free(Free { next: self.meta.as_meta().next });
        self.meta.as_meta_mut().next = Self::offset_to(self.meta.as_meta().start, n);
        for i in 0..Self::offset_to(self.meta.as_meta().start, self.meta.as_meta().end) {
        }
    }

    unsafe fn redistribute_nodes(&mut self, n: *mut Block<K, V>, neighbour: *mut Block<K, V>, neighbour_index: Option<usize>, pivot_index: usize, pivot: *const K) {
        for i in 0..Self::offset_to(self.meta.as_meta().start, self.meta.as_meta().end) {
        }
        if neighbour_index.is_some() {
            let nk = Self::num_keys(n) as usize;
            if !Self::is_leaf(n) {
                let p_old = Self::nth_ptr_mut(n, nk + 1);
                let p_new = Self::nth_ptr_mut(n, nk);
                *p_old = *p_new;
            }
            for i in (1..(nk + 1)).rev() {
                let k_old = Self::nth_key_mut(n, i);
                let k_new = Self::nth_key_mut(n, i - 1);
                *k_old = *k_new;
                let p_old = Self::nth_ptr_mut(n, i);
                let p_new = Self::nth_ptr_mut(n, i - 1);
                *p_old = *p_new;
            }
        for i in 0..Self::offset_to(self.meta.as_meta().start, self.meta.as_meta().end) {
        }
            let nk = Self::num_keys(neighbour) as usize;
            if !Self::is_leaf(n) {
                let p_old = Self::nth_ptr_mut(n, 0);
                let p_new = Self::nth_ptr_mut(neighbour, nk);
                *p_old = *p_new;
                let offset = *Self::nth_ptr(n, 0);
                let node = self.meta.as_meta().start.offset(offset);
                let parent = Self::parent_mut(node);
                *parent = Some(Self::offset_to(self.meta.as_meta().start, n));

                let ptr = Self::nth_ptr_mut(neighbour, nk);
                *ptr = 0;
                let key0 = Self::nth_key_mut(n, 0);
                *key0 = *pivot;
                let nparent = self.meta.as_meta().start.offset(Self::parent(n).unwrap());
                let keyn = Self::nth_key_mut(nparent, pivot_index);
                let nkey = Self::nth_key_mut(neighbour, nk - 1);
                *keyn = *nkey;
            } else {
                let p_old = Self::nth_ptr_mut(n, 0);
                let p_new = Self::nth_ptr_mut(neighbour, nk - 1);
                *p_old = *p_new;
                let ptr = Self::nth_ptr_mut(neighbour, nk - 1);
                *ptr = 0;
                let key0 = Self::nth_key_mut(n, 0);
                let nkey = Self::nth_key_mut(neighbour, nk - 1);
                *key0 = *nkey;
                let nparent = self.meta.as_meta().start.offset(Self::parent(n).unwrap());
                let keyn = Self::nth_key_mut(nparent, pivot_index);
                *keyn = *key0;
            }
        } else {
            let nk = Self::num_keys(n) as usize;
            if Self::is_leaf(n) {
                let k_old = Self::nth_key_mut(n, nk);
                let k_new = Self::nth_key_mut(neighbour, 0);
                *k_old = *k_new;
                let p_old = Self::nth_ptr_mut(n, nk);
                let p_new = Self::nth_ptr_mut(neighbour, 0);
                *p_old = *p_new;
                let nparent = self.meta.as_meta().start.offset(Self::parent(n).unwrap());
                let keyn  = Self::nth_key_mut(nparent, pivot_index);
                let nkey1 = Self::nth_key_mut(neighbour, 1);
                *keyn = *nkey1;
            } else {
                let keyn = Self::nth_key_mut(n, nk);
                *keyn = *pivot;
                let p_old = Self::nth_ptr_mut(n, nk + 1);
                let p_new = Self::nth_ptr_mut(neighbour, 0);
                *p_old = *p_new;
                let nparent = self.meta.as_meta().start.offset(Self::parent(n).unwrap());
                let keyn = Self::nth_key_mut(nparent, pivot_index);
                let nkey0 = Self::nth_key_mut(neighbour, 0);
                *keyn = *nkey0
            }
        for i in 0..Self::offset_to(self.meta.as_meta().start, self.meta.as_meta().end) {
        }
            let nk = Self::num_keys(neighbour) as usize;
            for i in 0..(nk - 1) {
                let k_old = Self::nth_key_mut(neighbour, i);
                let k_new = Self::nth_key_mut(neighbour, i + 1);
                *k_old = *k_new;
                let p_old = Self::nth_ptr_mut(neighbour, i);
                let p_new = Self::nth_ptr_mut(neighbour, i + 1);
                *p_old = *p_new;
            }
            if !Self::is_leaf(n) {
                let p_old = Self::nth_ptr_mut(neighbour, nk - 1);
                let p_new = Self::nth_ptr_mut(neighbour, nk);
                *p_old = *p_new;
            }
        }

        let n_key = Self::num_keys_mut(n);
        *n_key += 1;
        let nei_key = Self::num_keys_mut(neighbour);
        *nei_key -= 1;
        if !Self::is_leaf(n) {
            let ptrn = Self::nth_ptr_mut(n, *n_key as usize);
            let oldparent = Self::parent_mut(self.meta.as_meta().start.offset(*ptrn));
            *oldparent = Some(Self::offset_to(self.meta.as_meta().start, n));
        }
        for i in 0..Self::offset_to(self.meta.as_meta().start, self.meta.as_meta().end) {
        }
    }

    unsafe fn delete_entry(&mut self, n: *mut Block<K, V>, key: &K, value: *mut Block<K, V>) {
        for i in 0..Self::offset_to(self.meta.as_meta().start, self.meta.as_meta().end) {
        }
        let ix = self.remove_entry_from_node(n, key, value);

        if n == self.meta.as_meta().start.offset(self.meta.as_meta().root) {
            self.adjust_root();
            return;
        } 
        for i in 0..Self::offset_to(self.meta.as_meta().start, self.meta.as_meta().end) {
        }

        if ix == 0 && n != self.meta.as_meta().start.offset(self.meta.as_meta().root) {
            let mut parent = self.meta.as_meta().start.offset(Self::parent(n).unwrap());
            let mut parent_ix = 0;
            while self.meta.as_meta().start.offset(*Self::nth_ptr(parent, parent_ix)) != n {
                parent_ix += 1;
            }
            let mut next_smallest = if Self::num_keys(n) == 0 {
                if parent_ix == Self::num_keys(parent) as usize {
                    None
                } else {
                    let next_child = self.meta.as_meta().start.offset(*Self::nth_ptr(parent, parent_ix + 1));
                    Some(*Self::nth_key(next_child, 0))
                }
            } else {
                Some(*Self::nth_key(n, 0))
            };
            if let Some(replacement) = next_smallest {
                loop {
                    if parent_ix > 0 {
                        let parent_key = Self::nth_key_mut(parent, parent_ix - 1);
                        if *parent_key == *key {
                            *parent_key = replacement;
                        }
                    }
                    if let Some(offset) = Self::parent(parent) {
                        let grandparent = self.meta.as_meta().start.offset(offset);
                        parent_ix = 0;
                        while self.meta.as_meta().start.offset(*Self::nth_ptr(grandparent, parent_ix)) != parent {
                            parent_ix += 1;
                        }
                        parent = grandparent;
                    } else {
                        break;
                    }
                }
            }
        }
        let min_keys = Self::cut(ORDER) - 1;
        if (Self::num_keys(n) as usize) < min_keys {
            let neighbour_index = self.get_neighbour_index(n);
            let pivot_idx = neighbour_index.unwrap_or(0);
            let nparent = self.meta.as_meta().start.offset(Self::parent(n).unwrap());
            let pivot = Self::nth_key_mut(nparent, pivot_idx);
            let neighbour_offset = if neighbour_index.is_none() { Self::nth_ptr_mut(nparent, 1) } else { Self::nth_ptr_mut(nparent, neighbour_index.unwrap()) };
            let neighbour = self.meta.as_meta().start.offset(*neighbour_offset);

            let min_keys = Self::cut(ORDER) - 1;
            if Self::num_keys(neighbour) as usize > min_keys {
                self.redistribute_nodes(n, neighbour, neighbour_index, pivot_idx, pivot);
            } else {
                self.merge_nodes(n, neighbour, neighbour_index, pivot);
            }
        }
        for i in 0..Self::offset_to(self.meta.as_meta().start, self.meta.as_meta().end) {
        }
    }
    
    pub fn pop_front(&mut self) -> Option<(K, V)> {
        unsafe {
            if self.is_empty() {
                None
            } else {
                let mut node = self.meta.as_meta().start.offset(self.meta.as_meta().root);
                while !Self::is_leaf(node) {
                    node = self.meta.as_meta().start.offset(*Self::nth_ptr(node, 0));
                }
                let key = *Self::nth_key(node, 0);
                let bucket = self.meta.as_meta().start.offset(*Self::nth_ptr(node, 0));
                let value = (*bucket).as_bucket().values[0];
                self.delete(&key);
                Some((key, value))
            }
        }
    }
    
    pub fn pop_back(&mut self) -> Option<(K, V)> {
        unsafe {
            if self.is_empty() {
                None
            } else {
                let mut node = self.meta.as_meta().start.offset(self.meta.as_meta().root);
                while !Self::is_leaf(node) {
                    node = self.meta.as_meta().start.offset(*Self::nth_ptr(node, Self::num_keys(node) as usize - 1));
                }
                let key = *Self::nth_key(node, 0);
                let bucket = self.meta.as_meta().start.offset(*Self::nth_ptr(node, 0));
                let value = (*bucket).as_bucket().values[0];
                self.delete(&key);
                Some((key, value))
            }
        }
    }

    pub fn delete(&mut self, key: &K) -> bool {
        unsafe {
            if let Some(key_record) = self.find_block_mut(key) {
                if let Some(key_leaf) = self.find_leaf_mut(key) {
                    let value = &mut*key_record as *mut _;
                    self.delete_entry(key_leaf, key, value);
                    *value = Block::Free(Free { next: self.meta.as_meta().next });
                    self.meta.as_meta_mut().next = Self::offset_to(self.meta.as_meta().start, value);
                    return true;
                }
            }
            return false;
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> bool {
        unsafe {
            if self.is_empty() {
                let block = self.meta.as_meta_mut().start.offset(self.make_bucket());
                (*block).as_bucket_mut().values[0] = value;
                self.start_new_tree(key, block);
                true
            } else if true || self.find_block(&key).is_none() {
                if self.is_full() {
                    false
                } else {
                    let block = self.meta.as_meta_mut().start.offset(self.make_bucket());
                    (*block).as_bucket_mut().values[0] = value;
                    let leaf = self.find_leaf_mut(&key).unwrap();
                    if (Self::num_keys(leaf) as usize + 1) < ORDER {
                        self.insert_into_leaf(leaf, key, block);
                    } else {
                        self.split_and_insert_into_leaf(leaf, key, block);
                    }
                    true
                }
            } else {
                true
            }
        }
    }

    pub fn find(&self, key: &K) -> Option<&V> {
        unsafe {
            self.find_block(key).and_then(|ref x| Some(&x.as_bucket().values[0] as *const _)).and_then(|x| x.as_ref())
        }
    }

    pub fn range_find(&self, key_start: &K, key_end: &K) -> Vec<(&K, &V)> {
        unsafe {
            self.range_find_blocks(key_start, key_end).iter().map(|x| (x.0, &x.1.as_bucket().values[0])).collect::<Vec<_>>()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.meta.as_meta().root == 0
    }

    pub fn is_full(&self) -> bool {
        unsafe {
            self.meta.as_meta().start.offset(self.meta.as_meta().next + 1) > self.meta.as_meta().end
        }
    }

    pub fn load_from<'a, T>(data: &mut T) -> &'a mut Self {
        let btree = unsafe { &mut*(data as *mut T as *mut Self) };
        unsafe {
            btree.meta.as_meta_mut().start = (&mut btree.meta.as_meta_mut() as *mut _ as *mut _).offset(0);
            btree.meta.as_meta_mut().end = (data as *mut _).offset(1) as *mut _;
        }
        btree
    }

    pub fn create_from<T>(data: &mut T) -> &mut Self {
        let btree = unsafe { &mut*(data as *mut T as *mut Self) };
        unsafe {
            btree.meta = Block::Meta(mem::zeroed());
            btree.meta.as_meta_mut().start = (&mut btree.meta as *mut _).offset(0);
            btree.meta.as_meta_mut().next = 1;
            btree.meta.as_meta_mut().end = (data as *mut _).offset(1) as *mut _;
            btree.meta.as_meta_mut().root = 0;
            for i in 1.. {
                let block = btree.meta.as_meta().start.offset(i);
                if block >= btree.meta.as_meta().end {
                    break;
                }
                *block = Block::Free(Free { next: i + 1 });
            }
        }
        btree
    }
}

impl<K, V> fmt::Debug for BTree<K, V>
    where K: PartialOrd + Copy + Sized + fmt::Debug,
          V: Copy + Sized + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unsafe {
            write!(f, "")
        }
    }
}

#[test]
fn insert_and_find_btree() {
    let mut buffer = AnonymousBuffer::<Page>::try_new(mem::size_of::<Page>()).unwrap();
    let btree: &mut BTree<i32, i32> = BTree::create_from(&mut buffer[0]);
    btree.insert(2, 20);
    unsafe {
        for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
            println!("{}: {:?}", i, *btree.meta.as_meta_mut().start.offset(i));
        }
    }
    assert_eq!(Some(&20), btree.find(&2));
}

#[test]
fn insert_and_find_15_btree() {
    let mut buffer = AnonymousBuffer::<Page>::try_new(mem::size_of::<Page>()).unwrap();
    let btree: &mut BTree<i32, i32> = BTree::create_from(&mut buffer[0]);
    for x in 1..16 {
        btree.insert(x, x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After insert {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
    for x in 1..16 {
        assert_eq!(Some(&x), btree.find(&x));
    }
}

#[test]
fn insert_and_find_range_15_btree() {
    let mut buffer = AnonymousBuffer::<Page>::try_new(mem::size_of::<Page>()).unwrap();
    let btree: &mut BTree<i32, i32> = BTree::create_from(&mut buffer[0]);
    for x in 1..16 {
        btree.insert(x, x);
    }
    assert_eq!(vec![(&4,&4), (&5,&5), (&6,&6), (&7,&7), (&8,&8)], btree.range_find(&4, &8));
}

#[test]
fn insert_and_delete_left_15_btree() {
    let mut buffer = AnonymousBuffer::<Page>::try_new(mem::size_of::<Page>()).unwrap();
    let btree: &mut BTree<i32, i32> = BTree::create_from(&mut buffer[0]);
    for x in 1..16 {
        btree.insert(x, x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After insert {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
    for x in 1..16 {
        btree.delete(&x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After delete {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
}

#[test]
fn insert_and_delete_right_15_btree() {
    let mut buffer = AnonymousBuffer::<Page>::try_new(mem::size_of::<Page>()).unwrap();
    let btree: &mut BTree<i32, i32> = BTree::create_from(&mut buffer[0]);
    for x in 1..16 {
        btree.insert(x, x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After insert {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
    for x in (1..16).rev() {
        btree.delete(&x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After delete {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
}

#[test]
fn insert_and_delete_inner_15_btree() {
    let mut buffer = AnonymousBuffer::<Page>::try_new(mem::size_of::<Page>()).unwrap();
    let btree: &mut BTree<i32, i32> = BTree::create_from(&mut buffer[0]);
    for x in 1..16 {
        btree.insert(x, x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After insert {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
    for x in vec![8,7,9,6,10,5,11,4,12,3,13,2,14,1,15] {
        btree.delete(&x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After delete {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
}

#[test]
fn insert_and_delete_outer_15_btree() {
    let mut buffer = AnonymousBuffer::<Page>::try_new(mem::size_of::<Page>()).unwrap();
    let btree: &mut BTree<i32, i32> = BTree::create_from(&mut buffer[0]);
    for x in 1..16 {
        btree.insert(x, x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After insert {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
    for x in vec![8,7,9,6,10,5,11,4,12,3,13,2,14,1,15].iter().rev() {
        btree.delete(&x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After delete {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
}

#[test]
fn insert_and_delete_random_300_btree() {
    let mut buffer = AnonymousBuffer::<[Page; 10]>::try_new(mem::size_of::<[Page; 10]>()).unwrap();
    let btree: &mut BTree<u8, u8> = BTree::create_from(&mut buffer[0]);
    let mut inp = (1..301).collect::<Vec<_>>();
    let mut out = (1..301).collect::<Vec<_>>();
    let mut rng = StdRng::new().unwrap();
    rng.shuffle(&mut inp);
    rng.shuffle(&mut out);
    println!("Input: vec!{:?}", inp);
    println!("Output: vec!{:?}", out);
    //inp = vec![14, 13, 6, 9, 3, 10, 7, 1, 11, 2, 8, 5, 4, 12];
    //out = vec![4, 11, 14, 7, 5, 8, 10, 1, 3, 6, 9, 13, 12, 2];
    for x in inp {
        btree.insert(x, x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After insert {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
    for x in out {
        btree.delete(&x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After delete {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
}

#[test]
fn insert_and_delete_random_300_duplicate_btree() {
    let mut buffer = AnonymousBuffer::<[Page; 10]>::try_new(mem::size_of::<[Page; 10]>()).unwrap();
    let btree: &mut BTree<u8, u8> = BTree::create_from(&mut buffer[0]);
    let mut inp = (1..151).collect::<Vec<_>>();
    let mut out = (1..151).collect::<Vec<_>>();
    inp.extend((1..151));
    out.extend((1..151));
    let mut rng = StdRng::new().unwrap();
    rng.shuffle(&mut inp);
    rng.shuffle(&mut out);
    println!("Input: vec!{:?}", inp);
    println!("Output: vec!{:?}", out);
    //inp = vec![14, 13, 6, 9, 3, 10, 7, 1, 11, 2, 8, 5, 4, 12];
    //out = vec![4, 11, 14, 7, 5, 8, 10, 1, 3, 6, 9, 13, 12, 2];
    for x in inp {
        btree.insert(x, x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After insert {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
    for x in out {
        btree.delete(&x);
        unsafe {
            for i in 0..BTree::offset_to(btree.meta.as_meta().start, btree.meta.as_meta().end) {
                println!("After delete {}: {}: {:?}", x, i, *btree.meta.as_meta_mut().start.offset(i));
            }
        }
    }
}

