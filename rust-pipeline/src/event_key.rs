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
pub unsafe fn encode(p: *const u8, len: usize, source: *const u8) -> (u64, u64) {
    let bytes = r8(p);
    let (head, last) = if len == 9 {
        (bytes, *p.add(8))
    } else if len <= 8 {
        let pad = *PAD.get_unchecked(len);
        ((bytes & !pad) | (0x2c2c_2c2c_2c2c_2c2c & pad), b',')
    } else {
        return indirect(p, len, source);
    };
    if (head & !ASCII) != 0 || last >= 128 {
        return indirect(p, len, source);
    }
    let v = _pext_u64(head, ASCII) | ((last as u64) << 56);
    let mixed = (v ^ (v >> 31)).wrapping_mul(MIX) & !INDIRECT;
    let key = mixed ^ (mixed >> 30);
    (key, key)
}

#[cold]
unsafe fn indirect(p: *const u8, len: usize, source: *const u8) -> (u64, u64) {
    let h = hash(p, len);
    (INDIRECT | ((h & 0x7fff) << 48) | source as u64, h)
}

/// Hash bits for the partition-local table. An indirect key carries a
/// fingerprint so ordinary probes never have to revisit the mapped input.
#[inline(always)]
pub fn probe(key: u64) -> u64 {
    if key & INDIRECT == 0 {
        key
    } else {
        (key >> 48).wrapping_mul(MIX)
    }
}

#[inline(always)]
pub unsafe fn equal(a: u64, b: u64) -> bool {
    a == b
        || (a & b & INDIRECT != 0 && (a ^ b) >> 48 == 0 && id_bytes(a & ADDR) == id_bytes(b & ADDR))
}

/// F1 ingest times are whole seconds. Keep a 136-year interval inline;
/// exceptional dates carry the full signed time and payload offset.
pub fn pack(time: i64, pay: u64) -> u64 {
    if let Ok(t) = u32::try_from(time) {
        (pay << 32) | u64::from(t)
    } else {
        let ext = Box::into_raw(Box::new((time, pay)));
        INDIRECT | ext as u64
    }
}

#[inline(always)]
unsafe fn unpack(x: u64) -> (i64, u64) {
    if x & INDIRECT == 0 {
        (i64::from(x as u32), x >> 32)
    } else {
        *((x & !INDIRECT) as *const (i64, u64))
    }
}

#[inline(always)]
pub unsafe fn earlier(a: u64, b: u64) -> bool {
    if (a | b) & INDIRECT == 0 {
        (a as u32) < (b as u32)
    } else {
        unpack(a).0 < unpack(b).0
    }
}

#[inline(always)]
pub unsafe fn payload(x: u64) -> usize {
    unpack(x).1 as usize
}
