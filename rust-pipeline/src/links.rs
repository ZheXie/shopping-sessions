//! Linked IDs: a lock-free union-find over ID hashes in an open-addressing
//! table, built by all threads at once.
//!
//! A root is only ever linked under a root with a byte-wise smaller name, so
//! the root of every tree is the person's name. After the unions each slot is
//! rewritten to `(id hash) -> (person key, person name)` for one-probe lookups.

use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};

use crate::sys::{anon, par_map};

/// An ID's bytes end at the first of these: IDs hold none of them and every
/// field is followed by one.
#[inline(always)]
pub unsafe fn id_len(p: *const u8) -> usize {
    let mut n = 0;
    loop {
        let c = *p.add(n);
        if c == b',' || c == b'"' || c == b'\n' {
            return n;
        }
        n += 1;
    }
}

#[inline(always)]
pub unsafe fn id_bytes<'a>(p: u64) -> &'a [u8] {
    let p = p as *const u8;
    std::slice::from_raw_parts(p, id_len(p))
}

#[repr(C)]
struct Node {
    /// Hash of this ID; 0 marks an empty slot.
    key: AtomicU64,
    /// While linking: pointer to this ID's bytes. Afterwards: the person's name.
    a: u64,
    /// While linking: first 8 name bytes, big-endian. Afterwards: the person key.
    b: u64,
    parent: AtomicU32,
    _pad: u32,
}

#[derive(Clone, Copy)]
pub struct Edge {
    pub ha: u64,
    pub pa: u64,
    pub xa: u64,
    pub hb: u64,
    pub pb: u64,
    pub xb: u64,
}

pub struct Table {
    nodes: *mut Node,
    mask: usize,
}
unsafe impl Send for Table {}
unsafe impl Sync for Table {}

#[inline(always)]
fn fix(h: u64) -> u64 {
    if h == 0 {
        0x9e37_79b9_7f4a_7c15
    } else {
        h
    }
}

/// First 8 bytes of a non-empty name, big-endian, zero padded.
#[inline(always)]
pub unsafe fn prefix(p: *const u8, len: usize) -> u64 {
    let v = u64::from_be((p as *const u64).read_unaligned());
    if len >= 8 {
        v
    } else {
        v & !(u64::MAX >> (8 * len))
    }
}

const AHEAD: usize = 8;
const GRAIN: usize = 4096;

impl Table {
    #[inline(always)]
    fn slot(&self, h: u64) -> usize {
        (h >> 20) as usize & self.mask
    }

    #[inline(always)]
    pub unsafe fn prefetch(&self, h: u64) {
        if !self.nodes.is_null() {
            _mm_prefetch(self.nodes.add(self.slot(h)) as *const i8, _MM_HINT_T0);
        }
    }

    /// `(person key, pointer to the person's name)` for the ID with hash `h`
    /// whose own bytes are at `own`.
    #[inline(always)]
    pub unsafe fn lookup(&self, h: u64, own: u64) -> (u64, u64) {
        if self.nodes.is_null() {
            return (h, own);
        }
        let key = fix(h);
        let mut i = self.slot(h);
        loop {
            let n = &*self.nodes.add(i);
            let k = n.key.load(Relaxed);
            if k == key {
                return (n.b, n.a);
            }
            if k == 0 {
                return (h, own);
            }
            i = (i + 1) & self.mask;
        }
    }

    unsafe fn insert(&self, h: u64, ptr: u64, pre: u64) -> u32 {
        let key = fix(h);
        let mut i = self.slot(h);
        loop {
            let n = self.nodes.add(i);
            let k = (*n).key.load(Relaxed);
            if k == key {
                return i as u32;
            }
            if k == 0 {
                match (*n).key.compare_exchange(0, key, Relaxed, Relaxed) {
                    Ok(_) => {
                        (*n).a = ptr;
                        (*n).b = pre;
                        (*n).parent.store(i as u32, Relaxed);
                        return i as u32;
                    }
                    Err(now) if now == key => return i as u32,
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    unsafe fn parent(&self, x: u32) -> &AtomicU32 {
        &(*self.nodes.add(x as usize)).parent
    }

    /// Root of `x`, halving the path. A non-root's parent only ever moves to
    /// one of its ancestors, so a stale write here still points up the tree.
    unsafe fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.parent(x).load(Relaxed);
            if p == x {
                return x;
            }
            let gp = self.parent(p).load(Relaxed);
            if gp == p {
                return p;
            }
            self.parent(x).store(gp, Relaxed);
            x = gp;
        }
    }

    /// Byte-wise `name(x) < name(y)` for two distinct IDs.
    unsafe fn less(&self, x: u32, y: u32) -> bool {
        let nx = &*self.nodes.add(x as usize);
        let ny = &*self.nodes.add(y as usize);
        if nx.b != ny.b {
            return nx.b < ny.b;
        }
        id_bytes(nx.a) < id_bytes(ny.a)
    }

    unsafe fn union(&self, a: u32, b: u32) {
        let (mut a, mut b) = (a, b);
        loop {
            a = self.find(a);
            b = self.find(b);
            if a == b {
                return;
            }
            let (child, root) = if self.less(a, b) { (b, a) } else { (a, b) };
            if self.parent(child).compare_exchange(child, root, Relaxed, Relaxed).is_ok() {
                return;
            }
        }
    }

    pub fn build(edge_lists: &[Vec<Edge>], threads: usize) -> Table {
        let total: usize = edge_lists.iter().map(|v| v.len()).sum();
        if total == 0 {
            return Table { nodes: std::ptr::null_mut(), mask: 0 };
        }
        // At most 2 * total IDs: the load factor stays at or under 0.75.
        let cap = (total * 8 / 3 + 16).next_power_of_two();
        let table = Table {
            nodes: anon(cap * std::mem::size_of::<Node>()) as *mut Node,
            mask: cap - 1,
        };
        // (list, first edge, last edge, index of the first edge overall)
        let mut tasks: Vec<(usize, usize, usize, usize)> = Vec::new();
        let mut seen = 0usize;
        for (l, list) in edge_lists.iter().enumerate() {
            let mut lo = 0;
            while lo < list.len() {
                let hi = (lo + GRAIN).min(list.len());
                tasks.push((l, lo, hi, seen + lo));
                lo = hi;
            }
            seen += list.len();
        }
        let threads = threads.min(tasks.len()).max(1);
        let mut slots: Vec<(u32, u32)> = vec![(0, 0); total];
        let slots_ptr = slots.as_mut_ptr() as usize;
        let t = &table;
        unsafe {
            // Every ID gets a slot before any union reads a name.
            let next = AtomicUsize::new(0);
            par_map(threads, |_| loop {
                let Some(&(l, lo, hi, at)) = tasks.get(next.fetch_add(1, Relaxed)) else {
                    break;
                };
                let edges = &edge_lists[l];
                for i in lo..hi {
                    if let Some(f) = edges.get(i + AHEAD) {
                        t.prefetch(f.ha);
                        t.prefetch(f.hb);
                    }
                    let e = edges.get_unchecked(i);
                    let sa = t.insert(e.ha, e.pa, e.xa);
                    let sb = t.insert(e.hb, e.pb, e.xb);
                    *(slots_ptr as *mut (u32, u32)).add(at + i - lo) = (sa, sb);
                }
            });
            let next = AtomicUsize::new(0);
            par_map(threads, |_| loop {
                let lo = next.fetch_add(GRAIN, Relaxed);
                if lo >= total {
                    break;
                }
                let hi = (lo + GRAIN).min(total);
                for i in lo..hi {
                    if i + AHEAD < total {
                        let (fa, fb) = *slots.get_unchecked(i + AHEAD);
                        _mm_prefetch(t.nodes.add(fa as usize) as *const i8, _MM_HINT_T0);
                        _mm_prefetch(t.nodes.add(fb as usize) as *const i8, _MM_HINT_T0);
                    }
                    let (sa, sb) = *slots.get_unchecked(i);
                    if sa != sb {
                        t.union(sa, sb);
                    }
                }
            });
            // Rewrite every slot to (person key, person name). Other slots
            // only read a root's `key` and `a`, which a root never rewrites.
            let next = AtomicUsize::new(0);
            par_map(threads, |_| loop {
                let lo = next.fetch_add(GRAIN * 4, Relaxed);
                if lo >= cap {
                    break;
                }
                let hi = (lo + GRAIN * 4).min(cap);
                for s in lo..hi {
                    let n = t.nodes.add(s);
                    let key = (*n).key.load(Relaxed);
                    if key == 0 {
                        continue;
                    }
                    let mut r = s as u32;
                    loop {
                        let p = t.parent(r).load(Relaxed);
                        if p == r {
                            break;
                        }
                        r = p;
                    }
                    if r as usize == s {
                        (*n).b = key;
                    } else {
                        let root = t.nodes.add(r as usize);
                        (*n).a = (*root).a;
                        (*n).b = (*root).key.load(Relaxed);
                    }
                }
            });
        }
        table
    }
}
