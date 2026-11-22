//! Timestamps: F1 `2026-10-03T23:58:10Z`, F2 `...-04:00`, F3 epoch ms.

use std::arch::x86_64::*;

use crate::ids::ld64;

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - (m <= 2) as i64;
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}
const MDAYS: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
#[inline(always)]
fn leap(y: u32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}
#[inline(always)]
fn d2(p: &[u8], i: usize) -> Option<u32> {
    let a = p[i].wrapping_sub(b'0') as u32;
    let b = p[i + 1].wrapping_sub(b'0') as u32;
    if a > 9 || b > 9 {
        None
    } else {
        Some(a * 10 + b)
    }
}

/// Plain parser: epoch ms, or None for a bad timestamp. An F3 value of 19+
/// significant digits is >= 1e18 ms, outside every window, so it is bad.
pub fn parse_ts(p: &[u8]) -> Option<i64> {
    let n = p.len();
    if n == 0 {
        return None;
    }
    if p[0].wrapping_sub(b'0') <= 9 && (n < 5 || p[4] != b'-') {
        let mut i = 0;
        while i < n && p[i] == b'0' {
            i += 1;
        }
        if n - i > 18 {
            return None;
        }
        let mut v: i64 = 0;
        for &c in &p[i..] {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v * 10 + d as i64;
        }
        return Some(v);
    }
    if n != 20 && n != 25 {
        return None;
    }
    if p[4] != b'-' || p[7] != b'-' || p[10] != b'T' || p[13] != b':' || p[16] != b':' {
        return None;
    }
    let y = d2(p, 0)? * 100 + d2(p, 2)?;
    let (mo, d, h, mi, se) = (d2(p, 5)?, d2(p, 8)?, d2(p, 11)?, d2(p, 14)?, d2(p, 17)?);
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || d < 1 {
        return None;
    }
    let md = if mo == 2 && leap(y) { 29 } else { MDAYS[mo as usize - 1] };
    if d > md {
        return None;
    }
    let mut off: i64 = 0;
    if n == 20 {
        if p[19] != b'Z' {
            return None;
        }
    } else {
        let sg = p[19];
        if (sg != b'+' && sg != b'-') || p[22] != b':' {
            return None;
        }
        let (oh, om) = (d2(p, 20)?, d2(p, 23)?);
        if oh > 23 || om > 59 {
            return None;
        }
        off = (oh * 60 + om) as i64 * 60000;
        if sg == b'-' {
            off = -off;
        }
    }
    Some(((days_from_civil(y as i64, mo as i64, d as i64) * 24 + h as i64) * 60 + mi as i64) * 60000 + se as i64 * 1000 - off)
}

pub struct TsTables {
    year_days: [i32; 131], // days since epoch of Jan 1, 1970..2100
    mstart: [[i32; 13]; 2],
    mlen: [[u8; 13]; 2],
    shuf: [[u8; 16]; 17], // right-align n bytes in a 16-byte lane
}
pub fn ts_tables() -> TsTables {
    let mut t = TsTables { year_days: [0; 131], mstart: [[0; 13]; 2], mlen: [[0; 13]; 2], shuf: [[0; 16]; 17] };
    for y in 1970..=2100 {
        t.year_days[y - 1970] = days_from_civil(y as i64, 1, 1) as i32;
    }
    for lp in 0..2 {
        let mut acc = 0;
        for m in 1..=12 {
            t.mstart[lp][m] = acc;
            t.mlen[lp][m] = if m == 2 && lp == 1 { 29 } else { MDAYS[m - 1] as u8 };
            acc += t.mlen[lp][m] as i32;
        }
    }
    for n in 0..=16usize {
        for i in 0..16usize {
            t.shuf[n][i] = if i + n >= 16 { (i + n - 16) as u8 } else { 0x80 };
        }
    }
    t
}

// SWAR: x holds bytes XOR '0' (no borrows between bytes); true if every byte
// selected by m (0xFF bytes) is 0..9. A carry out of a byte only happens when
// that byte is itself not a digit, and the separators are checked first.
#[inline(always)]
fn swar_digits(x: u64, m: u64) -> bool {
    ((x.wrapping_add(0x7676767676767676) | x) & 0x8080808080808080 & m) == 0
}
#[inline(always)]
fn dg(x: u64, i: u32) -> u32 {
    ((x >> (8 * i)) & 0xFF) as u32
}

/// Same answers as parse_ts, for a field inside the read buffer (>= 32
/// readable bytes from p): SIMD for F3, a branch-light fixed-layout parse
/// for F1 / F2.
#[inline(always)]
pub unsafe fn parse_ts_fast(t: &TsTables, p: *const u8, n: u32) -> Option<i64> {
    if (n == 20 || n == 25) && *p.add(4) == b'-' {
        const Z: u64 = 0x3030303030303030;
        let a = ld64(p) ^ Z;
        let b = ld64(p.add(8)) ^ Z;
        let c = ld64(p.add(16)) ^ Z;
        // separators: a[4] '-', a[7] '-', b[2] 'T', b[5] ':', c[0] ':'
        if ((a >> 32) & 0xFF) != (b'-' ^ b'0') as u64
            || ((a >> 56) & 0xFF) != (b'-' ^ b'0') as u64
            || ((b >> 16) & 0xFF) != (b'T' ^ b'0') as u64
            || ((b >> 40) & 0xFF) != (b':' ^ b'0') as u64
            || (c & 0xFF) != (b':' ^ b'0') as u64
        {
            return None;
        }
        if !swar_digits(a, 0x00FFFF00FFFFFFFF) || !swar_digits(b, 0xFFFF00FFFF00FFFF) || !swar_digits(c, 0x0000000000FFFF00) {
            return None;
        }
        let y = dg(a, 0) * 1000 + dg(a, 1) * 100 + dg(a, 2) * 10 + dg(a, 3);
        let mo = dg(a, 5) * 10 + dg(a, 6);
        let d = dg(b, 0) * 10 + dg(b, 1);
        let h = dg(b, 3) * 10 + dg(b, 4);
        let mi = dg(b, 6) * 10 + dg(b, 7);
        let se = dg(c, 1) * 10 + dg(c, 2);
        if mo.wrapping_sub(1) > 11 || h > 23 || mi > 59 || se > 59 || d == 0 {
            return None;
        }
        let lp = leap(y) as usize;
        if d > t.mlen[lp][mo as usize] as u32 {
            return None;
        }
        let days: i64 = if (1970..=2100).contains(&y) {
            t.year_days[(y - 1970) as usize] as i64 + t.mstart[lp][mo as usize] as i64 + d as i64 - 1
        } else {
            days_from_civil(y as i64, mo as i64, d as i64)
        };
        let mut off: i64 = 0;
        if n == 20 {
            if *p.add(19) != b'Z' {
                return None;
            }
        } else {
            let sg = *p.add(19);
            if (sg != b'+' && sg != b'-') || *p.add(22) != b':' {
                return None;
            }
            let o0 = (*p.add(20)).wrapping_sub(b'0') as u32;
            let o1 = (*p.add(21)).wrapping_sub(b'0') as u32;
            let o2 = (*p.add(23)).wrapping_sub(b'0') as u32;
            let o3 = (*p.add(24)).wrapping_sub(b'0') as u32;
            if o0 > 9 || o1 > 9 || o2 > 9 || o3 > 9 {
                return None;
            }
            let (oh, om) = (o0 * 10 + o1, o2 * 10 + o3);
            if oh > 23 || om > 59 {
                return None;
            }
            off = (oh * 60 + om) as i64 * 60000;
            if sg == b'-' {
                off = -off;
            }
        }
        return Some(((days * 24 + h as i64) * 60 + mi as i64) * 60000 + se as i64 * 1000 - off);
    }
    if n == 0 || n > 16 {
        return parse_ts(std::slice::from_raw_parts(p, n as usize)); // empty, long F3, or other lengths
    }
    // F3 with up to 16 digits
    let x = _mm_sub_epi8(_mm_loadu_si128(p as *const __m128i), _mm_set1_epi8(b'0' as i8));
    let live = _mm_cmpgt_epi8(_mm_set1_epi8(n as i8), _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15));
    let isd = _mm_cmpeq_epi8(_mm_min_epu8(x, _mm_set1_epi8(9)), x);
    if _mm_movemask_epi8(_mm_andnot_si128(isd, live)) != 0 {
        return None;
    }
    let r = _mm_shuffle_epi8(x, _mm_loadu_si128(t.shuf[n as usize].as_ptr() as *const __m128i));
    let t1 = _mm_maddubs_epi16(r, _mm_set1_epi16(0x010A)); // 2 digits
    let t2 = _mm_madd_epi16(t1, _mm_set1_epi32(0x0001_0064)); // 4 digits
    let t3 = _mm_packus_epi32(t2, t2); // 4 x u16
    let t4 = _mm_madd_epi16(t3, _mm_set1_epi32(0x0001_2710)); // 8 digits
    let lo = _mm_cvtsi128_si32(t4) as u32 as u64;
    let hi = _mm_extract_epi32::<1>(t4) as u32 as u64;
    Some((lo * 100_000_000 + hi) as i64)
}
