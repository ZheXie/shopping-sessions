//! Word loads, decimal decoding and hashing for short CSV fields.

const S0: u64 = 0xa076_1d64_78bd_642f;
const S1: u64 = 0xe703_7ed1_a0b4_28db;

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

/// Decode eight digit values packed in little-endian byte lanes (first
/// digit least significant). Return `u64::MAX` if any lane exceeds nine.
#[inline(always)]
pub fn decimal8(lanes: u64) -> u64 {
    if (lanes.wrapping_add(0x7676_7676_7676_7676) | lanes) & 0x8080_8080_8080_8080 != 0 {
        return u64::MAX;
    }
    let pairs = lanes.wrapping_mul(2561) >> 8;
    let quads = (pairs & 0x00FF_00FF_00FF_00FF).wrapping_mul(6_553_601) >> 16;
    (quads & 0x0000_FFFF_0000_FFFF).wrapping_mul(42_949_672_960_001) >> 32
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
            let a =
                ((*p as u64) << 16) | ((*p.add(len >> 1) as u64) << 8) | (*p.add(len - 1) as u64);
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
