ozone
=====
Ozone is a pure-[rust](http://www.rust-lang.org/) key/value store inspired by the language's built-in concept of memory ownership.


Goals
-----
The goal of this project is to build a perfectly idiomatic persistence layer initially using the standard library's [collections::HashMap](https://doc.rust-lang.org/collections/struct.HashMap.html) as a model, letting the language itself do as much as possible.

Specific features that will be implemented include:

- single-file databases
- copy-on-write, lock-free MVCC
- recycling of emptied pages



Proposed API
------------
The API will be identical to that provided by collections::HashMap, including an Entry API:

```rust
pub struct HashMap<K, V, B = AnonymousBuffer> {
    fn entry(&mut self, key: K) -> Entry<K, V>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn get(&self, k: &K) -> Option<&V>;
    fn contains_key(&self, k: &K) -> bool;
    fn get_mut(&mut self, k: &K) -> Option<&mut V>;
    fn insert(&mut self, k: K, v: V) -> Option<V>;
    fn remove(&mut self, k: &K) -> Option<V>;
}
```



Why The Name?
-------------

Ozone, or O<sub>3</sub> is a powerful oxidant (oxidation/reduction is the chemical process of rusting) that is naturally created from O<sub>2</sub> (the stuff we breathe) when a *bolt* of lightning strikes.


## License

Licensed under

 * Mozilla Public License 2.0 ([LICENSE](LICENSE) or http://www.mozilla.org/en-US/MPL/2.0/)

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the MPL-2.0
license, shall be licensed as above, without any additional terms or
conditions.
