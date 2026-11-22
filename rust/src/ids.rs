//! Hashing and 8-byte ID keys.
//!
//! Every ID is one u64 key.
//!  * "Packed" (the generator's IDs: 1-10 chars of [0-9A-Za-z]): each char is
//!    a 6-bit symbol 1..62 ('0'-'9' -> 1-10, 'A'-'Z' -> 11-36, 'a'-'z' ->
//!    37-62, which keeps ASCII order), first char most significant, missing
//!    chars 0. So the key is exact, and comparing keys as integers IS
//!    byte-wise order (a prefix sorts first). Keys < 2^60.
//!  * "Wide" (anything else): bit 63 plus a 63-bit hash of the bytes; the
//!    bytes are kept in a registry for output and ordering. Equality of two
//!    wide IDs is by that hash (wide IDs are rare; collisions are detected).

use std::arch::x86_64::*;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

pub const K0: u64 = 0xa0761d6478bd642f;
pub const K1: u64 = 0xe7037ed1a0b428db;
pub const K2: u64 = 0x8ebc6af09c88c6e3;
pub const K3: u64 = 0x589965cc75374cc3;

#[inline(always)]
pub fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}
#[inline(always)]
pub unsafe fn ld64(p: *const u8) -> u64 {
    (p as *const u64).read_unaligned()
}
#[inline(always)]
pub unsafe fn ld32(p: *const u8) -> u64 {
    (p as *const u32).read_unaligned() as u64
}
pub fn pow2ge(x: u64) -> u64 {
    let mut c = 16u64;
    while c < x {
        c <<= 1;
    }
    c
}
/// wyhash-style hash of a byte string
pub fn hbytes(s: &[u8], seed0: u64) -> u64 {
    let n = s.len();
    let p = s.as_ptr();
    let mut seed = seed0 ^ (n as u64).wrapping_mul(K3);
    let (a, b);
    unsafe {
        if n <= 16 {
            if n >= 8 {
                a = ld64(p);
                b = ld64(p.add(n - 8));
            } else if n >= 4 {
                a = (ld32(p) << 32) | ld32(p.add(n - 4));
                b = 0;
            } else if n > 0 {
                a = ((s[0] as u64) << 16) | ((s[n >> 1] as u64) << 8) | s[n - 1] as u64;
                b = 0;
            } else {
                a = 0;
                b = 0;
            }
        } else {
            let mut i = n;
            let mut q = p;
            while i > 16 {
                seed = mum(ld64(q) ^ K1, ld64(q.add(8)) ^ seed);
                q = q.add(16);
                i -= 16;
            }
            a = ld64(p.add(n - 16));
            b = ld64(p.add(n - 8));
        }
    }
    let h = mum(a ^ K1, b ^ seed);
    mum(h ^ K2, (n as u64) ^ K1)
}
#[inline(always)]
pub fn khash(k: u64) -> u64 {
    mum(k ^ K1, K2)
}

pub const WIDE: u64 = 1 << 63;
const UNSYM: [u8; 64] = {
    let mut t = [0u8; 64];
    let mut c = 0;
    while c < 10 {
        t[1 + c] = b'0' + c as u8;
        c += 1;
    }
    c = 0;
    while c < 26 {
        t[11 + c] = b'A' + c as u8;
        t[37 + c] = b'a' + c as u8;
        c += 1;
    }
    t
};
static WIDE_REG: Mutex<Option<HashMap<u64, Vec<u8>>>> = Mutex::new(None);
thread_local! {
    static SEEN: RefCell<HashSet<u64>> = RefCell::new(HashSet::new());
}

/// p has >= 16 readable bytes; n >= 1
#[inline(always)]
pub unsafe fn pack_id(p: *const u8, n: u32) -> Option<u64> {
    if n > 10 {
        return None;
    }
    let v = _mm_loadu_si128(p as *const __m128i);
    let live = _mm_cmpgt_epi8(_mm_set1_epi8(n as i8), _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15));
    let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
    let u = _mm_sub_epi8(v, _mm_set1_epi8(b'A' as i8));
    let l = _mm_sub_epi8(v, _mm_set1_epi8(b'a' as i8));
    let isd = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
    let isu = _mm_cmpeq_epi8(_mm_min_epu8(u, _mm_set1_epi8(25)), u);
    let isl = _mm_cmpeq_epi8(_mm_min_epu8(l, _mm_set1_epi8(25)), l);
    if _mm_movemask_epi8(_mm_andnot_si128(_mm_or_si128(_mm_or_si128(isd, isu), isl), live)) != 0 {
        return None;
    }
    let mut sym = _mm_or_si128(
        _mm_or_si128(
            _mm_and_si128(isd, _mm_add_epi8(d, _mm_set1_epi8(1))),
            _mm_and_si128(isu, _mm_add_epi8(u, _mm_set1_epi8(11))),
        ),
        _mm_and_si128(isl, _mm_add_epi8(l, _mm_set1_epi8(37))),
    );
    sym = _mm_and_si128(sym, live);
    let t = _mm_maddubs_epi16(sym, _mm_set1_epi16(0x0140)); // s0*64 + s1
    let q = _mm_madd_epi16(t, _mm_set1_epi32(0x0001_1000)); // 4 symbols -> 24 bits
    let q01 = _mm_cvtsi128_si64(q) as u64;
    let q2 = _mm_extract_epi32::<2>(q) as u32 as u64;
    Some(((q01 & 0xFF_FFFF) << 36) | ((q01 >> 32) << 12) | (q2 >> 12))
}

/// The key of the n bytes at p (>= 16 readable bytes from p).
#[inline(always)]
pub unsafe fn make_key(p: *const u8, n: u32) -> u64 {
    if let Some(k) = pack_id(p, n) {
        return k;
    }
    make_wide(std::slice::from_raw_parts(p, n as usize))
}
#[cold]
fn make_wide(s: &[u8]) -> u64 {
    let k = WIDE | (hbytes(s, K0) >> 1);
    let fresh = SEEN.with(|seen| seen.borrow_mut().insert(k));
    if fresh {
        let mut g = WIDE_REG.lock().unwrap();
        let reg = g.get_or_insert_with(HashMap::new);
        match reg.get(&k) {
            None => {
                reg.insert(k, s.to_vec());
            }
            Some(v) if v.as_slice() != s => crate::sys::die("wide ID hash collision"),
            _ => {}
        }
    }
    k
}
/// The bytes of a key, appended to out.
pub fn key_bytes(k: u64, out: &mut Vec<u8>) {
    out.clear();
    if k & WIDE != 0 {
        let g = WIDE_REG.lock().unwrap();
        out.extend_from_slice(&g.as_ref().unwrap()[&k]);
        return;
    }
    for i in (0..10).rev() {
        let s = ((k >> (6 * i)) & 63) as usize;
        if s == 0 {
            break;
        }
        out.push(UNSYM[s]);
    }
}
/// Byte-wise order of the IDs behind two keys.
#[inline(always)]
pub fn key_less(a: u64, b: u64) -> bool {
    if (a | b) & WIDE == 0 {
        return a < b;
    }
    key_less_slow(a, b)
}
#[cold]
fn key_less_slow(a: u64, b: u64) -> bool {
    let (mut x, mut y) = (Vec::new(), Vec::new());
    key_bytes(a, &mut x);
    key_bytes(b, &mut y);
    x < y
}
