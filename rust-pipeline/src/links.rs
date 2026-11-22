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

/// For an inline name, the bytes after its leading `u` (little-endian,
/// zero-padded) and the whole ID's length.
#[inline(always)]
pub fn inline_chars(n: Name) -> Option<(u64, usize)> {
    if n & INLINE == 0 {
        None
    } else {
        Some((n & ASCII, inline_len(n)))
    }
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

// Keep four immutable alias keys, their root indices, and two full names
// of otherwise-unlinked people in the same cache line. The keys can still
// be loaded in one AVX2 operation while workers claim the two extra slots.
#[repr(C, align(64))]
struct Group {
    keys: [AtomicU64; 4],
    roots: [AtomicU32; 4],
    unlinked: [AtomicU64; 2],
}

pub struct Table {
    groups: *mut Group,
    cap: usize,
    slot_bits: u32,
}
unsafe impl Send for Table {}
unsafe impl Sync for Table {}

// The second unlinked slot also records whether an alias insertion continued
// into the next group. Both inline and indirect names leave bit 63 clear.
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
    fn split(&self, root: usize) -> (Name, usize) {
        let local = (root & ((1 << self.slot_bits) - 1)) as u64 + 1;
        (local, root >> self.slot_bits)
    }

    // The terminal probe group is stable after the links freeze. A claimed
    // name is never evicted, so a miss on two occupied slots always follows
    // the same ordinary name partition, even across concurrent parsers.
    #[inline(always)]
    unsafe fn unlinked(&self, group: usize, raw: Name, fallback: usize) -> (Name, usize) {
        let g = &*self.groups.add(group);
        for lane in 0..2 {
            let cell = &g.unlinked[lane];
            let mut full = cell.load(Relaxed);
            if full & !OVERFLOW == 0 {
                match cell.compare_exchange(full, full | raw, Relaxed, Relaxed) {
                    Ok(_) => full |= raw,
                    Err(other) => full = other,
                }
            }
            if same_name(full & !OVERFLOW, raw) {
                return self.split(self.cap + 2 * group + lane);
            }
        }
        (raw, fallback)
    }

    #[inline(always)]
    pub unsafe fn route(
        &self, mut group: usize, name: Name, fallback: usize,
    ) -> (Name, usize) {
        if self.cap == 0 {
            return (name, fallback);
        }
        loop {
            let g = &*self.groups.add(group);
            if name & INLINE != 0 {
                // All insert workers have joined before immutable lookups begin.
                let keys = _mm256_load_si256(g.keys.as_ptr().cast());
                let equal = _mm256_cmpeq_epi64(keys, _mm256_set1_epi64x(name as i64));
                let mask = _mm256_movemask_pd(_mm256_castsi256_pd(equal));
                if mask != 0 {
                    let lane = mask.trailing_zeros() as usize;
                    return self.split(g.roots.get_unchecked(lane).load(Relaxed) as usize);
                }
                if g.unlinked[1].load(Relaxed) & OVERFLOW == 0 {
                    return self.unlinked(group, name, fallback);
                }
            } else {
                for lane in 0..4 {
                    let key = g.keys[lane].load(Relaxed);
                    if key == 0 {
                        return self.unlinked(group, name, fallback);
                    }
                    if same_name(key, name) {
                        return self.split(g.roots[lane].load(Relaxed) as usize);
                    }
                }
                if g.unlinked[1].load(Relaxed) & OVERFLOW == 0 {
                    return self.unlinked(group, name, fallback);
                }
            }
            group += 1;
            if group == self.cap / 4 {
                group = 0;
            }
        }
    }

    /// The root slot of a linked name. Unlike `route`, this never claims a
    /// slot for an unlinked name, so the table stays unchanged.
    #[inline(always)]
    pub unsafe fn root_of(&self, name: Name) -> Option<usize> {
        if self.cap == 0 {
            return None;
        }
        let mut group = self.first_slot(name_hash(name)) / 4;
        loop {
            let g = &*self.groups.add(group);
            for lane in 0..4 {
                let key = g.keys[lane].load(Relaxed);
                if key == 0 {
                    return None;
                }
                if same_name(key, name) {
                    return Some(g.roots[lane].load(Relaxed) as usize);
                }
            }
            if g.unlinked[1].load(Relaxed) & OVERFLOW == 0 {
                return None;
            }
            group += 1;
            if group == self.cap / 4 {
                group = 0;
            }
        }
    }

    /// Alias slots, occupied or not.
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// The name in alias slot `s` (0 if free) and its root slot.
    #[inline(always)]
    pub unsafe fn slot(&self, s: usize) -> (Name, usize) {
        let g = &*self.groups.add(s / 4);
        (
            g.keys[s % 4].load(Relaxed),
            g.roots[s % 4].load(Relaxed) as usize,
        )
    }

    pub fn groups(&self) -> usize {
        self.cap / 4
    }

    pub fn parts(&self) -> usize {
        (self.cap + self.cap / 2).div_ceil(1 << self.shift())
    }

    pub fn shift(&self) -> u32 {
        self.slot_bits
    }

    // Bound the number of parser cursors, including the two independent
    // people that each four-slot alias group can now accommodate.
    fn shift_for(cap: usize) -> u32 {
        (cap + cap / 2)
            .div_ceil(4096)
            .next_power_of_two()
            .max(8192)
            .trailing_zeros()
    }

    pub unsafe fn canonical(&self, root: usize) -> Name {
        if root < self.cap {
            self.key(root).load(Relaxed)
        } else {
            let local = root - self.cap;
            (*self.groups.add(local / 2)).unlinked[local % 2].load(Relaxed) & !OVERFLOW
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
                let flag = &(*self.groups.add(i / 4)).unlinked[1];
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
                slot_bits: Self::shift_for(0),
            };
        }
        let cap = (total * 9 / 4 + 16).next_multiple_of(4);
        assert!(cap < u32::MAX as usize, "too many links");
        let bytes = cap / 4 * std::mem::size_of::<Group>();
        let table = Table {
            groups: anon(bytes) as *mut Group,
            cap,
            slot_bits: Self::shift_for(cap),
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
                    group.roots[s % 4].store(root, Relaxed);
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
        let edges = names[1..=edge_count]
            .iter()
            .map(|&name| (names[0], name))
            .collect();
        let table = Table::build(&[edges], 1);
        let resolve = |name| unsafe {
            let (local, part) = table.route(table.prefetch(name_hash(name)), name, usize::MAX);
            if part == usize::MAX {
                name
            } else {
                table.canonical((part << table.shift()) + local as usize - 1)
            }
        };
        for &name in &names[..=edge_count] {
            assert_eq!(resolve(name), names[0]);
        }
        let absent = names[edge_count + 1];
        assert_eq!(resolve(absent), absent);
    }

    #[test]
    fn independent_names_share_slots_by_bytes_and_overflow_consistently() {
        let linked = unsafe { name_of(b"u00000000".as_ptr(), 9) };
        let table = Table::build(&[vec![(linked, linked)]], 1);
        let copies: Vec<Vec<u8>> = (0..8).map(|_| b"long individual name,".to_vec()).collect();
        let barrier = std::sync::Barrier::new(copies.len());
        let routes = par_map(copies.len(), |i| unsafe {
            let raw = name_of(copies[i].as_ptr(), copies[i].len() - 1);
            barrier.wait();
            table.unlinked(0, raw, usize::MAX)
        });
        let (local, part) = routes[0];
        assert!(part < table.parts());
        assert!(routes.iter().all(|r| *r == routes[0]));
        unsafe {
            let canonical = table.canonical((part << table.shift()) + local as usize - 1);
            assert_eq!(id_bytes(canonical >> 8), b"long individual name");
            let second = name_of(b"u00000001".as_ptr(), 9);
            let third = name_of(b"u00000002".as_ptr(), 9);
            // An alias probe can cross this group without affecting the saved
            // independent name or the decision when both slots are occupied.
            (*table.groups).unlinked[1].fetch_or(OVERFLOW, Relaxed);
            let (local2, part2) = table.unlinked(0, second, usize::MAX);
            assert!(part2 < table.parts());
            assert_ne!((local2, part2), (local, part));
            assert_eq!(
                table.canonical((part2 << table.shift()) + local2 as usize - 1),
                second
            );
            for _ in 0..3 {
                assert_eq!(table.unlinked(0, third, usize::MAX), (third, usize::MAX));
                assert_eq!(table.unlinked(0, second, usize::MAX), (local2, part2));
            }
        }
    }
}
