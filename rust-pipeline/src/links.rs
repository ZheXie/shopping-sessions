//! Intern linked IDs and resolve every alias to the byte-wise smallest name.
//! A compact name is also an exact lookup key and the event's grouping key.

use std::arch::x86_64::*;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};

use crate::hashx::{hash, r8};
use crate::sys::{anon, par_map, populate_write};

pub const ADDR: u64 = (1 << 48) - 1;
pub type Name = u64;

const INLINE: u64 = 0x80;
const ASCII: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const LENGTH_BITS: u64 = 0x0000_0080_8080_8000;

#[inline(always)]
pub unsafe fn id_bytes<'a>(p: u64) -> &'a [u8] {
    let p = p as *const u8;
    let mut n = 0;
    while !matches!(*p.add(n), b',' | b'"' | b'\n') {
        n += 1;
    }
    std::slice::from_raw_parts(p, n)
}

const PACK: [(u64, u64); 9] = {
    let mut out = [(0, 0); 9];
    let mut n = 0;
    while n <= 8 {
        let missing = 8 - n as u64;
        let tag = INLINE
            | ((missing & 1) << 15)
            | ((missing & 2) << 22)
            | ((missing & 4) << 29)
            | ((missing & 8) << 36);
        let mask = if n == 8 {
            u64::MAX
        } else {
            (1u64 << (8 * n)) - 1
        };
        out[n] = (mask, tag);
        n += 1;
    }
    out
};

/// `u` followed by up to eight ASCII bytes fits in one word. Spare high
/// bits record the exact length (including embedded NULs). Other IDs refer
/// to the original, unquoted bytes in the input mapping.
#[inline(always)]
pub unsafe fn name_of(p: *const u8, len: usize) -> Name {
    name_at(p, len, p)
}

/// The bytes at `p` may be in a reusable buffer; `source` lives until exit.
#[inline(always)]
pub unsafe fn name_at(p: *const u8, len: usize, source: *const u8) -> Name {
    if len.wrapping_sub(1) <= 8 && *p == b'u' {
        let (mask, tag) = *PACK.get_unchecked(len - 1);
        let word = r8(p.add(1)) & mask;
        if word & !ASCII == 0 {
            return word | tag;
        }
    }
    (source as u64) << 8
}

#[inline(always)]
fn inline_len(n: Name) -> usize {
    9 - unsafe { _pext_u64(n, LENGTH_BITS) } as usize
}

#[inline(always)]
pub fn same_name(a: Name, b: Name) -> bool {
    a == b
        || ((a | b) & INLINE == 0
            && a != 0
            && b != 0
            && unsafe { id_bytes(a >> 8) == id_bytes(b >> 8) })
}

#[inline(always)]
pub fn name_hash(n: Name) -> u64 {
    if n & INLINE != 0 {
        return (n ^ n.rotate_left(29)).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    }
    unsafe {
        let bytes = id_bytes(n >> 8);
        hash(bytes.as_ptr(), bytes.len())
    }
}

pub fn write_name(n: Name, out: &mut Vec<u8>) {
    if n & INLINE != 0 {
        let len = inline_len(n);
        let start = out.len();
        out.reserve(13);
        // The reserved tail fits a four-byte Parquet length and all nine
        // possible name bytes. Only the actual name becomes part of the page.
        unsafe {
            let dst = out.as_mut_ptr().add(start);
            (dst as *mut u32).write_unaligned((len as u32).to_le());
            *dst.add(4) = b'u';
            (dst.add(5) as *mut u64).write_unaligned((n & ASCII).to_le());
            out.set_len(start + 4 + len);
        }
    } else {
        let bytes = unsafe { id_bytes(n >> 8) };
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    }
}

#[inline]
unsafe fn compare_names(a: Name, b: Name) -> Ordering {
    if a & b & INLINE != 0 {
        return (a & ASCII)
            .swap_bytes()
            .cmp(&(b & ASCII).swap_bytes())
            .then_with(|| inline_len(a).cmp(&inline_len(b)));
    }
    if a & INLINE != 0 {
        let right = id_bytes(b >> 8);
        return b'u'
            .cmp(&right[0])
            .then_with(|| (a & ASCII).to_le_bytes()[..inline_len(a) - 1].cmp(&right[1..]));
    }
    if b & INLINE != 0 {
        return compare_names(b, a).reverse();
    }
    id_bytes(a >> 8).cmp(id_bytes(b >> 8))
}

pub type Edge = (Name, Name);

// Group keys and canonical values in the same cache line. Every probe
// starts at the beginning of a group and advances only after all four
// slots are occupied. Lookup can compare four inline IDs together.
#[repr(C, align(64))]
struct Group {
    keys: [AtomicU64; 4],
    people: [AtomicU64; 4],
}

pub struct Table {
    groups: *mut Group,
    cap: usize,
}
unsafe impl Send for Table {}
unsafe impl Sync for Table {}

// The final lane's person also records whether an insertion continued into
// the next group. Inline names have seven-bit suffix bytes; indirect names
// store an input pointer (ADDR) shifted by eight, leaving bit 63 available.
const OVERFLOW: u64 = 1 << 63;
const GRAIN: usize = 4096;
const AHEAD: usize = 8;

impl Table {
    #[inline(always)]
    fn first_slot(&self, hash: u64) -> usize {
        ((hash as u128 * self.cap as u128 >> 64) as usize) & !3
    }

    #[inline(always)]
    pub unsafe fn prefetch(&self, h: u64) -> usize {
        let group = self.first_slot(h) / 4;
        if self.cap != 0 {
            _mm_prefetch(self.groups.add(group).cast(), _MM_HINT_T0);
        }
        group
    }

    #[inline(always)]
    pub unsafe fn lookup(&self, mut group: usize, name: Name) -> Name {
        if self.cap == 0 {
            return name;
        }
        loop {
            let g = &*self.groups.add(group);
            if name & INLINE != 0 {
                let keys = _mm256_load_si256(g.keys.as_ptr().cast());
                let equal = _mm256_cmpeq_epi64(keys, _mm256_set1_epi64x(name as i64));
                let mask = _mm256_movemask_pd(_mm256_castsi256_pd(equal));
                // Lane zero is a readable placeholder when the mask is empty.
                let lane = mask.trailing_zeros() as usize & 3;
                if (mask != 0) | (g.people[3].load(Relaxed) & OVERFLOW == 0) {
                    let found = g.people.get_unchecked(lane).load(Relaxed) & !OVERFLOW;
                    return if mask != 0 { found } else { name };
                }
            } else {
                for lane in 0..4 {
                    let key = g.keys[lane].load(Relaxed);
                    if key == 0 {
                        return name;
                    }
                    if same_name(key, name) {
                        return g.people[lane].load(Relaxed) & !OVERFLOW;
                    }
                }
            }
            group += 1;
            if group == self.cap / 4 {
                group = 0;
            }
        }
    }

    #[inline(always)]
    unsafe fn key(&self, slot: usize) -> &AtomicU64 {
        (*self.groups.add(slot / 4)).keys.get_unchecked(slot % 4)
    }

    #[inline(always)]
    unsafe fn insert(&self, name: Name) -> u32 {
        let mut i = self.first_slot(name_hash(name));
        loop {
            let key = self.key(i);
            let mut seen = key.load(Relaxed);
            if seen == 0 {
                match key.compare_exchange(0, name, Relaxed, Relaxed) {
                    Ok(_) => return i as u32,
                    Err(other) => seen = other,
                }
            }
            if same_name(seen, name) {
                return i as u32;
            }
            if i % 4 == 3 {
                let flag = &(*self.groups.add(i / 4)).people[3];
                if flag.load(Relaxed) == 0 {
                    flag.store(OVERFLOW, Relaxed);
                }
            }
            i += 1;
            if i == self.cap {
                i = 0;
            }
        }
    }

    // Zero is a root, so a newly claimed slot is ready to union immediately.
    #[inline(always)]
    unsafe fn find(parents: &[AtomicU32], mut x: u32) -> u32 {
        loop {
            let px = parents.get_unchecked(x as usize);
            let p = px.load(Relaxed);
            if p == 0 {
                return x;
            }
            let gp = parents.get_unchecked((p - 1) as usize).load(Relaxed);
            if gp == 0 {
                return p - 1;
            }
            px.store(gp, Relaxed);
            x = gp - 1;
        }
    }

    unsafe fn union(&self, parents: &[AtomicU32], mut a: u32, mut b: u32) {
        loop {
            a = Self::find(parents, a);
            b = Self::find(parents, b);
            if a == b {
                return;
            }
            let ka = self.key(a as usize).load(Relaxed);
            let kb = self.key(b as usize).load(Relaxed);
            let (child, root) = if compare_names(ka, kb).is_lt() {
                (b, a)
            } else {
                (a, b)
            };
            if parents
                .get_unchecked(child as usize)
                .compare_exchange(0, root + 1, Relaxed, Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn build(edge_lists: &[Vec<Edge>], threads: usize) -> Table {
        let total: usize = edge_lists.iter().map(Vec::len).sum();
        if total == 0 {
            return Table {
                groups: std::ptr::null_mut(),
                cap: 0,
            };
        }
        let cap = (total * 9 / 4 + 16).next_multiple_of(4);
        assert!(cap < u32::MAX as usize, "too many links");
        let bytes = cap / 4 * std::mem::size_of::<Group>();
        let table = Table {
            groups: anon(bytes) as *mut Group,
            cap,
        };
        let parents: Vec<AtomicU32> = (0..cap).map(|_| AtomicU32::new(0)).collect();
        let tasks: Vec<&[Edge]> = edge_lists.iter().flat_map(|v| v.chunks(GRAIN)).collect();
        let threads = threads.min(tasks.len()).max(1);
        let base = table.groups as usize;
        par_map(threads, |i| {
            const HP: usize = 2 << 20;
            let from = (bytes * i / threads) & !(HP - 1);
            let to = if i + 1 == threads {
                bytes
            } else {
                (bytes * (i + 1) / threads) & !(HP - 1)
            };
            if to > from {
                populate_write((base + from) as *mut u8, to - from);
            }
        });
        let next = AtomicUsize::new(0);
        par_map(threads, |_| {
            while let Some(edges) = tasks.get(next.fetch_add(1, Relaxed)) {
                for (i, &(a, b)) in edges.iter().enumerate() {
                    unsafe {
                        if let Some(&(fa, fb)) = edges.get(i + AHEAD) {
                            table.prefetch(name_hash(fa));
                            table.prefetch(name_hash(fb));
                        }
                        let sa = table.insert(a);
                        let sb = table.insert(b);
                        if sa != sb {
                            table.union(&parents, sa, sb);
                        }
                    }
                }
            }
        });
        par_map(threads, |i| unsafe {
            for s in cap * i / threads..cap * (i + 1) / threads {
                if table.key(s).load(Relaxed) != 0 {
                    let root = Self::find(&parents, s as u32);
                    let group = &*table.groups.add(s / 4);
                    let dest = &group.people[s % 4];
                    let person = table.key(root as usize).load(Relaxed);
                    dest.store(dest.load(Relaxed) | person, Relaxed);
                }
            }
        });
        table
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linked_names_can_probe_across_the_end_of_the_table() {
        let edge_count = 11;
        let cap = (edge_count * 9usize / 4 + 16).next_multiple_of(4);
        let names: Vec<_> = (0..)
            .map(|i| {
                let id = format!("u{i:08}");
                unsafe { name_of(id.as_ptr(), id.len()) }
            })
            .filter(|&name| (name_hash(name) as u128 * cap as u128 >> 64) as usize >= cap - 4)
            .take(edge_count + 2)
            .collect();
        let edges = names[1..=edge_count].iter().map(|&name| (names[0], name)).collect();
        let table = Table::build(&[edges], 1);
        let resolve = |name| unsafe { table.lookup(table.prefetch(name_hash(name)), name) };
        for &name in &names[..=edge_count] {
            assert_eq!(resolve(name), names[0]);
        }
        let absent = names[edge_count + 1];
        assert_eq!(resolve(absent), absent);
    }
}
