//! Fast 64-bit hashing of short byte strings (wyhash-style multiply-fold).

const S0: u64 = 0xa076_1d64_78bd_642f;
const S1: u64 = 0xe703_7ed1_a0b4_28db;
const S2: u64 = 0x8ebc_6af0_9c88_c6e3;
const S3: u64 = 0x5899_65cc_7537_4cc3;

#[inline(always)]
fn mul(a: u64, b: u64) -> (u64, u64) {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64, (r >> 64) as u64)
}

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let (lo, hi) = mul(a, b);
    lo ^ hi
}

#[inline(always)]
pub unsafe fn r8(p: *const u8) -> u64 {
    (p as *const u64).read_unaligned()
}

#[inline(always)]
pub unsafe fn r4(p: *const u8) -> u32 {
    (p as *const u32).read_unaligned()
}

/// Two words that, with `len`, determine the whole string (after folding the
/// head of strings longer than 16 bytes into `seed`).
#[inline(always)]
unsafe fn words(p: *const u8, len: usize) -> (u64, u64, u64) {
    if len <= 16 {
        if len >= 8 {
            (r8(p), r8(p.add(len - 8)), S0)
        } else if len >= 4 {
            (r4(p) as u64, r4(p.add(len - 4)) as u64, S0)
        } else if len > 0 {
            let a = ((*p as u64) << 16) | ((*p.add(len >> 1) as u64) << 8) | (*p.add(len - 1) as u64);
            (a, 0, S0)
        } else {
            (0, 0, S0)
        }
    } else {
        let mut seed = S0;
        let mut i = 0;
        while len - i > 16 {
            seed = mum(r8(p.add(i)) ^ S1, r8(p.add(i + 8)) ^ seed);
            i += 16;
        }
        (r8(p.add(len - 16)), r8(p.add(len - 8)), seed)
    }
}

#[inline(always)]
pub unsafe fn hash(p: *const u8, len: usize) -> u64 {
    let (a, b, seed) = words(p, len);
    let (lo, hi) = mul(a ^ S1, b ^ seed);
    mum(lo ^ S0 ^ (len as u64), hi ^ S1)
}

/// The same hash plus a second, independent one.
#[inline(always)]
pub unsafe fn hash2(p: *const u8, len: usize) -> (u64, u64) {
    let (a, b, seed) = words(p, len);
    let (lo, hi) = mul(a ^ S1, b ^ seed);
    let h = mum(lo ^ S0 ^ (len as u64), hi ^ S1);
    let c = mum(a ^ S2 ^ seed.rotate_left(23), b ^ S3 ^ (len as u64).wrapping_mul(S1));
    (h, c)
}
