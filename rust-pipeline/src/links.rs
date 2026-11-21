//! Linked IDs: a lock-free union-find over ID hashes in an open-addressing
//! table, built by all threads at once.
//!
//! A root is only ever linked under a root with a byte-wise smaller name, so
//! the root of every tree is the person's name. After the unions each slot is
//! rewritten in place to `(id hash) -> (person key, person name)`, so a
//! lookup is one probe and never touches the name's bytes.

use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};

use crate::hashx::r8;
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

/// A name in 16 bytes. Up to 15 bytes are stored inline, zero padded, with
/// the length in the top byte of the second word. A longer name is its hash
/// and, under `LONG`, its address. Bit 63 is always clear.
pub type Name = (u64, u64);
pub const LONG: u64 = 1 << 62;
pub const ADDR: u64 = (1 << 48) - 1;

static KEEP: [(u64, u64); 16] = {
    let mut t = [(0u64, 0u64); 16];
    let mut len = 0;
    while len < 16 {
        t[len] = if len >= 8 {
            (u64::MAX, (1u64 << (8 * (len - 8))) - 1)
        } else {
            ((1u64 << (8 * len)) - 1, 0)
        };
        len += 1;
    }
    t
};

/// `hash` must be the name's hash. Reads 16 bytes at `p`.
#[inline(always)]
pub unsafe fn name_of(p: *const u8, len: usize, hash: u64) -> Name {
    if len < 16 {
        let (k0, k1) = *KEEP.get_unchecked(len);
        (r8(p) & k0, (r8(p.add(8)) & k1) | ((len as u64) << 56))
    } else {
        (hash, p as u64 | LONG)
    }
}

#[inline(always)]
pub fn same_name(a: Name, b: Name) -> bool {
    a.0 == b.0 && (a.1 == b.1 || a.1 & b.1 & LONG != 0)
}

/// A well-mixed word of a name, the same for every copy of it.
#[inline(always)]
pub fn name_hash(n: Name) -> u64 {
    let inline = ((n.1 >> 62) & 1).wrapping_sub(1);
    (n.0 ^ (n.1 & inline).rotate_left(29)).wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

/// While linking: `[id hash, address of the id, first 8 bytes big-endian,
/// parent slot]`. Afterwards: `[id hash, person key, name.0, name.1]`.
/// An id hash of 0 marks an empty slot.
#[repr(C, align(32))]
struct Node {
    key: AtomicU64,
    a: u64,
    b: u64,
    c: AtomicU64,
}

impl Node {
    #[inline(always)]
    fn parent(&self) -> &AtomicU32 {
        unsafe { &*(&self.c as *const AtomicU64 as *const AtomicU32) }
    }
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
    cap: usize,
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
        ((h as u128 * self.cap as u128) >> 64) as usize
    }

    #[inline(always)]
    pub unsafe fn prefetch(&self, h: u64) {
        if !self.nodes.is_null() {
            let p = self.nodes.add(self.slot(h)) as *const i8;
            _mm_prefetch(p, _MM_HINT_T0);
            _mm_prefetch(p.add(64), _MM_HINT_T0);
        }
    }

    /// `(person key, person name)` for the ID at `p` whose hash is `h`.
    #[inline(always)]
    pub unsafe fn lookup(&self, h: u64, p: *const u8, len: usize) -> (u64, Name) {
        if !self.nodes.is_null() {
            let key = fix(h);
            let mut i = self.slot(h);
            loop {
                let n = &*self.nodes.add(i);
                let k = n.key.load(Relaxed);
                if k == key {
                    return (n.a, (n.b, n.c.load(Relaxed)));
                }
                if k == 0 {
                    break;
                }
                i += 1;
                if i == self.cap {
                    i = 0;
                }
            }
        }
        (h, name_of(p, len, h))
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
                        (*n).parent().store(i as u32, Relaxed);
                        return i as u32;
                    }
                    Err(now) if now == key => return i as u32,
                    Err(_) => {}
                }
            }
            i += 1;
            if i == self.cap {
                i = 0;
            }
        }
    }

    #[inline(always)]
    unsafe fn parent(&self, x: u32) -> &AtomicU32 {
        (*self.nodes.add(x as usize)).parent()
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
            return Table { nodes: std::ptr::null_mut(), cap: 0 };
        }
        // At most 2 * total IDs, and usually far fewer.
        let cap = total * 9 / 4 + 16;
        assert!(cap < u32::MAX as usize, "too many links");
        let table = Table { nodes: anon(cap * std::mem::size_of::<Node>()) as *mut Node, cap };
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
            // Point every ID straight at its root. A walk that crosses a slot
            // being rewritten sees its old parent or its root, both above it.
            let next = AtomicUsize::new(0);
            let roots = par_map(threads, |_| {
                let mut roots: Vec<u32> = Vec::new();
                loop {
                    let lo = next.fetch_add(GRAIN * 4, Relaxed);
                    if lo >= cap {
                        break;
                    }
                    for s in lo..(lo + GRAIN * 4).min(cap) {
                        if (*t.nodes.add(s)).key.load(Relaxed) == 0 {
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
                            roots.push(r);
                        } else {
                            t.parent(s as u32).store(r, Relaxed);
                        }
                    }
                }
                roots
            });
            // Rewrite the other IDs from their roots, which stay as they are
            // until every other ID is done.
            let next = AtomicUsize::new(0);
            par_map(threads, |_| loop {
                let lo = next.fetch_add(GRAIN * 4, Relaxed);
                if lo >= cap {
                    break;
                }
                for s in lo..(lo + GRAIN * 4).min(cap) {
                    let n = t.nodes.add(s);
                    if (*n).key.load(Relaxed) == 0 {
                        continue;
                    }
                    let r = (*n).parent().load(Relaxed) as usize;
                    if r == s {
                        continue;
                    }
                    let root = t.nodes.add(r);
                    let key = (*root).key.load(Relaxed);
                    let p = (*root).a as *const u8;
                    let name = name_of(p, id_len(p), key);
                    (*n).a = key;
                    (*n).b = name.0;
                    (*n).c.store(name.1, Relaxed);
                }
            });
            par_map(roots.len(), |i| {
                for &r in &roots[i] {
                    let n = t.nodes.add(r as usize);
                    let key = (*n).key.load(Relaxed);
                    let p = (*n).a as *const u8;
                    let name = name_of(p, id_len(p), key);
                    (*n).a = key;
                    (*n).b = name.0;
                    (*n).c.store(name.1, Relaxed);
                }
            });
        }
        table
    }
}
