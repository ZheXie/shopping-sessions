//! Exact event identifiers and minimum-ingest comparisons for partitioned dedup.

use crate::hashx::{hash, r8};
use crate::links::{id_bytes, ADDR};
use std::arch::x86_64::_pext_u64;

const INDIRECT: u64 = 1 << 63;
const ASCII: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const MIX: u64 = 0x9e37_79b9_7f4a_7c15;

// Pad short IDs with commas, which the CSV dialect excludes from field values.
const PAD: [u64; 9] = {
    let mut table = [0; 9];
    let mut n = 0;
    while n < 8 {
        table[n] = u64::MAX << (8 * n);
        n += 1;
    }
    table
};

/// Up to nine ASCII bytes fit losslessly in 63 bits. The mixing steps are
/// bijections on those bits, so the mixed key doubles as its own hash.
/// Longer / non-ASCII IDs use a tagged pointer and a proper byte comparison.
#[inline(always)]
unsafe fn encode(p: *const u8, len: usize, stable: impl FnOnce() -> *const u8) -> (u64, u64) {
    let bytes = r8(p);
    let (head, last) = if len == 9 {
        (bytes, *p.add(8))
    } else if len <= 8 {
        let pad = *PAD.get_unchecked(len);
        ((bytes & !pad) | (0x2c2c_2c2c_2c2c_2c2c & pad), b',')
    } else {
        return indirect(p, len, stable);
    };
    if (head & !ASCII) != 0 || last >= 128 {
        return indirect(p, len, stable);
    }
    let v = _pext_u64(head, ASCII) | ((last as u64) << 56);
    let mixed = (v ^ (v >> 31)).wrapping_mul(MIX) & !INDIRECT;
    let key = mixed ^ (mixed >> 30);
    (key, key)
}

#[inline(always)]
unsafe fn indirect(p: *const u8, len: usize, stable: impl FnOnce() -> *const u8) -> (u64, u64) {
    let h = hash(p, len);
    (INDIRECT | ((h & 0x7fff) << 48) | stable() as u64, h)
}

/// Hash bits for the partition-local table. An indirect key carries a
/// fingerprint so ordinary probes never have to revisit the mapped input.
#[inline(always)]
fn probe(key: u64) -> u64 {
    if key & INDIRECT == 0 {
        key
    } else {
        (key >> 48).wrapping_mul(MIX)
    }
}

#[inline(always)]
unsafe fn equal(a: u64, b: u64) -> bool {
    a == b
        || (a & b & INDIRECT != 0 && (a ^ b) >> 48 == 0 && id_bytes(a & ADDR) == id_bytes(b & ADDR))
}


// Skip the two common bits of each suffix byte. A comma, excluded from
// field values by the CSV dialect, pads the unused byte positions.
const MASK48: u64 = (1 << 48) - 1;
const SIX_BITS: u64 = 0x5f5f_5f5f_5f5f_5f5f;
const FIXED: u64 = 0xa0a0_a0a0_a0a0_a0a0;
const EXPECTED: u64 = 0x2020_2020_2020_2020;
const EXT: u32 = 1 << 31;
/// Where a generic ID's partition word keeps its probe hash (above the
/// partition index, which `MAX_PARTS` keeps below 16 bits).
const HASH_SHIFT: u32 = 16;

#[derive(Clone, Copy)]
pub struct Layout {
    quotient_bits: u32,
    quotient_mask: u64,
    time_range: u64,
    part_mask: usize,
}

#[repr(C, packed(4))]
#[derive(Clone, Copy)]
pub struct Key {
    body: u64,
    payload: u32,
}

#[derive(Clone, Copy)]
struct Extended {
    id: u64,
    ingest: i64,
}

/// `e` + 32 lowercase hex digits keeps its exact 128-bit value instead of a
/// copy of its bytes. The record starts like `Extended` (low half, ingest),
/// so ingest comparisons read both forms alike; the high half follows.
#[repr(C)]
#[derive(Clone, Copy)]
struct Hex {
    lo: u64,
    ingest: i64,
    hi: u64,
}

/// Marks a 128-bit hex ID in a partition word and in an extended key body.
const HEX: u64 = 1 << 62;

/// The 128-bit value of 32 lowercase hex digits at `p`, if they all are.
#[inline(always)]
unsafe fn hex128(p: *const u8) -> Option<(u64, u64)> {
    use std::arch::x86_64::*;
    let x = _mm256_loadu_si256(p.cast());
    let d = _mm256_sub_epi8(x, _mm256_set1_epi8(b'0' as i8));
    let l = _mm256_sub_epi8(x, _mm256_set1_epi8(b'a' as i8));
    let is_digit = _mm256_cmpeq_epi8(_mm256_min_epu8(d, _mm256_set1_epi8(9)), d);
    let is_letter = _mm256_cmpeq_epi8(_mm256_min_epu8(l, _mm256_set1_epi8(5)), l);
    if _mm256_movemask_epi8(_mm256_or_si256(is_digit, is_letter)) != -1 {
        return None;
    }
    let nibbles = _mm256_blendv_epi8(d, _mm256_add_epi8(l, _mm256_set1_epi8(10)), is_letter);
    // Each 16-bit lane: first digit * 16 + second digit.
    let pairs = _mm256_maddubs_epi16(nibbles, _mm256_set1_epi16(0x0110));
    let bytes = _mm256_packus_epi16(pairs, pairs);
    Some((
        _mm256_extract_epi64::<0>(bytes) as u64,
        _mm256_extract_epi64::<2>(bytes) as u64,
    ))
}

#[inline(always)]
fn hex_hash(hi: u64, lo: u64) -> u64 {
    let mut h = (hi ^ lo.rotate_left(32)).wrapping_mul(MIX);
    h ^= h >> 29;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^ (h >> 32)
}

impl Layout {
    pub fn new(parts: usize) -> Self {
        let quotient_bits = 48 - parts.trailing_zeros();
        Self {
            quotient_bits,
            quotient_mask: (1 << quotient_bits) - 1,
            time_range: 1 << (64 - quotient_bits),
            part_mask: parts - 1,
        }
    }

    pub fn min_parts(window_ms: u64) -> usize {
        // Leave room for the ordinary ingest seconds throughout the window,
        // including in the small datasets used for the profile-guided build.
        (window_ms.div_ceil(1000).saturating_mul(2).div_ceil(1 << 16) as usize)
            .clamp(1, crate::MAX_PARTS)
            .next_power_of_two()
    }

    pub fn padding(self, window_ms: u64) -> i64 {
        (self.time_range.saturating_sub(window_ms.div_ceil(1000)) / 2) as i64
    }

    /// The high partition bits plus the low quotient uniquely determine the
    /// mixed 48-bit key. Other IDs use the original exact representation and
    /// mark their partition with INDIRECT to distinguish the two alphabets.
    /// Returns (id, partition word, high half of a hex ID or 0).
    #[inline(always)]
    pub unsafe fn identify(
        self, p: *const u8, len: usize, stable: impl FnOnce() -> *const u8,
    ) -> (u64, u64, u64) {
        if *p == b'e' && len.wrapping_sub(1) <= 8 {
            let bytes = r8(p.add(1));
            let head = if len == 9 {
                bytes
            } else {
                let pad = *PAD.get_unchecked(len - 1);
                (bytes & !pad) | (0x2c2c_2c2c_2c2c_2c2c & pad)
            };
            if head & FIXED == EXPECTED {
                let bits = _pext_u64(head, SIX_BITS);
                let mixed = bits.wrapping_mul(MIX) & MASK48;
                let key = mixed ^ (mixed >> 24);
                return (key & self.quotient_mask, key >> self.quotient_bits, 0);
            }
        }
        if len == 33 && *p == b'e' {
            if let Some((lo, hi)) = hex128(p.add(1)) {
                let h = hex_hash(hi, lo);
                let probe_bits = u64::from(h as u32) << HASH_SHIFT;
                let part = (h >> crate::PART_SHIFT) & self.part_mask as u64;
                return (lo, INDIRECT | HEX | probe_bits | part, hi);
            }
        }
        let (id, part) = self.generic(p, len, stable);
        (id, part, 0)
    }

    #[cold]
    #[inline(never)]
    unsafe fn generic(
        self, p: *const u8, len: usize, stable: impl FnOnce() -> *const u8,
    ) -> (u64, u64) {
        let (id, hash) = encode(p, len, stable);
        // Bits 16..48 of the partition word carry 32 hash bits for the
        // dedup table: the 15-bit fingerprint in a long ID's word alone
        // made thousands of long keys in a partition share probe slots.
        let probe_bits = u64::from(hash as u32) << HASH_SHIFT;
        (id, INDIRECT | probe_bits | ((hash >> crate::PART_SHIFT) & self.part_mask as u64))
    }

    #[inline(always)]
    pub unsafe fn pack(
        self, id: u64, partition: u64, high: u64, ingest: i64, payload: u64,
        slab: &mut crate::sys::Slab,
    ) -> Key {
        if partition & INDIRECT == 0 && (ingest as u64) < self.time_range {
            Key {
                body: id | ((ingest as u64) << self.quotient_bits),
                payload: payload as u32,
            }
        } else {
            self.extra(id, partition, high, ingest, payload, slab)
        }
    }

    #[cold]
    unsafe fn extra(
        self, id: u64, partition: u64, high: u64, ingest: i64, payload: u64,
        slab: &mut crate::sys::Slab,
    ) -> Key {
        let full = if partition & HEX != 0 {
            let full = slab.alloc(std::mem::size_of::<Hex>()) as *mut Hex;
            full.write(Hex { lo: id, ingest, hi: high });
            full as usize
        } else {
            let full = slab.alloc(std::mem::size_of::<Extended>()) as *mut Extended;
            full.write(Extended { id, ingest });
            full as usize
        };
        let offset = ((full - slab.base()) / crate::sys::Arena::UNIT) as u64;
        // Bit 62 of the body marks a hex record, so offsets stay below 2^30.
        assert!(offset < 1 << 30, "event key arena too large");
        // Keep the same 32-bit hash in both record forms. The high bit of
        // the offset distinguishes a generic ID from a compact ID whose
        // outlying ingest second alone needed an extension.
        let hash = if partition & INDIRECT != 0 {
            (partition >> HASH_SHIFT) as u32
        } else {
            probe(id) as u32
        };
        Key {
            body: (offset << 32) | u64::from(hash) | (partition & (INDIRECT | HEX)),
            payload: EXT | payload as u32,
        }
    }

    #[inline(always)]
    pub unsafe fn equals(self, a: Key, b: Key, base: usize) -> bool {
        if (a.payload | b.payload) & EXT == 0 {
            return (a.body ^ b.body) & self.quotient_mask == 0;
        }
        let generic = |k: Key| k.payload & EXT != 0 && k.body & INDIRECT != 0;
        if generic(a) != generic(b) {
            return false;
        }
        if generic(a) && (a.body | b.body) & HEX != 0 {
            // Only two hex IDs can be equal; their hash bits match first.
            if (a.body ^ b.body) & (HEX | 0xffff_ffff) != 0 {
                return false;
            }
            let (x, y) = (a.hex(base), b.hex(base));
            return x.lo == y.lo && x.hi == y.hi;
        }
        let id = |k: Key| {
            if k.payload & EXT == 0 {
                k.body & self.quotient_mask
            } else {
                k.full(base).id
            }
        };
        equal(id(a), id(b))
    }

    #[inline(always)]
    pub unsafe fn earlier(self, a: Key, b: Key, base: usize) -> bool {
        if (a.payload | b.payload) & EXT == 0 {
            return a.body < b.body;
        }
        let time = |k: Key| {
            if k.payload & EXT == 0 {
                (k.body >> self.quotient_bits) as i64
            } else {
                k.full(base).ingest
            }
        };
        time(a) < time(b)
    }
}

impl Key {
    #[inline(always)]
    fn record(self, base: usize) -> usize {
        let offset = ((self.body & !(INDIRECT | HEX)) >> 32) as usize;
        base + offset * crate::sys::Arena::UNIT
    }

    #[inline(always)]
    unsafe fn full(self, base: usize) -> Extended {
        *(self.record(base) as *const Extended)
    }

    #[inline(always)]
    unsafe fn hex(self, base: usize) -> Hex {
        *(self.record(base) as *const Hex)
    }

    #[inline(always)]
    pub fn hash(self) -> u32 {
        self.body as u32
    }

    #[inline(always)]
    pub fn payload(self) -> usize {
        (self.payload & !EXT) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sys::{Arena, Slab};

    #[test]
    fn ordinary_and_extended_ingest_seconds_compare_across_record_forms() {
        assert_eq!(std::mem::size_of::<Key>(), 12);
        let arena = Arena::new();
        let mut slab = Slab::new(&arena);
        let ids: &[&[u8]] = &[
            b"e", b"e0", b"e00", b"e12345678", b"e `~\x7f", b"eA", b"e\0", b"e123456789",
            "猫".as_bytes(),
            b"e0123456789abcdef0123456789abcdef",
            b"e0123456789abcdef0123456789abcdee",
            b"f0123456789abcdef0123456789abcdef",
            b"e00000000000000000000000000000000",
            b"e00000000000000000000000000000001",
            b"e10000000000000000000000000000000",
            b"effffffffffffffffffffffffffffffff",
            b"e0123456789ABCDEF0123456789abcdef",
            b"e0123456789abcdef0123456789abcdeg",
            b"e0123456789abcdef0123456789abcde",
            b"e0123456789abcdef0123456789abcdef0",
        ];
        let buffers: Vec<_> = ids.iter().map(|id| {
            let mut buffer = id.to_vec();
            buffer.push(b',');
            buffer.resize(buffer.len() + 64, 0);
            buffer
        }).collect();
        for parts in [1, 32, 512, 4096] {
            let layout = Layout::new(parts);
            let times = [-17, 0, layout.time_range as i64 - 1, layout.time_range as i64];
            let mut rows = Vec::new();
            for (i, id) in ids.iter().enumerate() {
                let p = buffers[i].as_ptr();
                let (key, partition, high) = unsafe { layout.identify(p, id.len(), || p) };
                assert_eq!(partition & HEX != 0, i >= 9 && i < 16 && i != 11, "{i}");
                for &time in &times {
                    let payload = 0x7fff_ffff - i as u64;
                    let row =
                        unsafe { layout.pack(key, partition, high, time, payload, &mut slab) };
                    assert_eq!(row.payload(), payload as usize);
                    rows.push((i, partition as usize & (parts - 1), time, row));
                }
            }
            for &(a, part, time, row) in &rows {
                for &(b, other_part, other_time, other) in &rows {
                    if part != other_part {
                        continue;
                    }
                    assert_eq!(unsafe { layout.equals(row, other, arena.base) }, a == b);
                    if a == b {
                        assert_eq!(row.hash(), other.hash());
                        assert_eq!(unsafe { layout.earlier(row, other, arena.base) }, time < other_time);
                    }
                }
            }
        }
    }
}
