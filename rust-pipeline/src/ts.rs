//! Timestamps: F1 `2026-10-03T23:58:10Z`, F2 `...+HH:MM`, F3 epoch millis.

use crate::hashx::{decimal8, r8};
use std::arch::x86_64::*;

pub const BAD: i64 = i64::MIN;
pub const DAY_MS: i64 = 86_400_000;

static DAYS_IN_MONTH: [u8; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

const SLOTS: usize = 128;

/// Memo of `YYYY-MM-DD` text to the UTC epoch seconds of that midnight. A dataset only
/// holds a few dozen dates, so the calendar arithmetic runs once per date.
pub struct Dates {
    head: [u64; SLOTS],
    tail: [u16; SLOTS],
    seconds: [i64; SLOTS],
}

impl Dates {
    pub fn new() -> Self {
        Dates {
            head: [0; SLOTS],
            tail: [0; SLOTS],
            seconds: [BAD; SLOTS],
        }
    }

    /// The ten bytes at `p` as a date, or `BAD`.
    #[cold]
    #[inline(never)]
    unsafe fn miss(&mut self, p: *const u8, slot: usize) -> i64 {
        let d = |i: usize| (*p.add(i)).wrapping_sub(b'0') as i64;
        for i in [0, 1, 2, 3, 5, 6, 8, 9] {
            if d(i) > 9 || d(i) < 0 {
                return BAD;
            }
        }
        if *p.add(4) != b'-' || *p.add(7) != b'-' {
            return BAD;
        }
        let y = d(0) * 1000 + d(1) * 100 + d(2) * 10 + d(3);
        let m = d(5) * 10 + d(6);
        let day = d(8) * 10 + d(9);
        if !(1..=12).contains(&m) || day == 0 {
            return BAD;
        }
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let len = if m == 2 && leap {
            29
        } else {
            DAYS_IN_MONTH[m as usize] as i64
        };
        if day > len {
            return BAD;
        }
        let seconds = days_from_civil(y, m, day) * (DAY_MS / 1000);
        self.head[slot] = r8(p);
        self.tail[slot] = (p.add(8) as *const u16).read_unaligned();
        self.seconds[slot] = seconds;
        seconds
    }

    /// UTC midnight in epoch seconds, or `BAD`.
    #[inline(always)]
    unsafe fn midnight(&mut self, p: *const u8) -> i64 {
        let head = r8(p);
        let tail = (p.add(8) as *const u16).read_unaligned();
        // Low bits of the day's two digits and of the month's last digit.
        let slot =
            (((tail >> 8) & 0xF) | ((tail & 3) << 4) | (((head >> 48) as u16 & 1) << 6)) as usize;
        if *self.head.get_unchecked(slot) == head && *self.tail.get_unchecked(slot) == tail {
            *self.seconds.get_unchecked(slot)
        } else {
            self.miss(p, slot)
        }
    }

    /// Every ingest timestamp in a well-formed event is a valid F1 per the
    /// input contract. Decode its six clock digits without testing them again.
    #[inline(always)]
    pub unsafe fn ingest_seconds(&mut self, p: *const u8, len: usize) -> i64 {
        if len != 20 {
            return BAD;
        }
        let day = self.midnight(p);
        if day == BAD {
            return BAD;
        }
        let x = r8(p.add(11)) ^ 0x3030_3A30_303A_3030;
        let y = x.wrapping_mul(2561);
        day + (((y >> 8) & 255) * 3600 + ((y >> 32) & 255) * 60 + (y >> 56)) as i64
    }

    #[inline(always)]
    unsafe fn datetime(&mut self, p: *const u8) -> i64 {
        let day = self.midnight(p);
        // `HH:MM:SS` at once: after the xor a digit byte is 0..=9 and a colon
        // byte is 0; adding 0x76 (or 0x7F) sets the top bit of any other.
        let x = r8(p.add(11)) ^ 0x3030_3A30_303A_3030;
        let bad = (x.wrapping_add(0x7676_7F76_767F_7676) | x) & 0x8080_8080_8080_8080;
        // Byte k of the product is x[k] + 10 * x[k - 1].
        let y = x.wrapping_mul(2561);
        let (h, m, s) = ((y >> 8) & 0xFF, (y >> 32) & 0xFF, y >> 56);
        if (bad != 0) | (*p.add(10) != b'T') | (h > 23) | (m > 59) | (s > 59) | (day == BAD) {
            return BAD;
        }
        (day + (h * 3600 + m * 60 + s) as i64) * 1000
    }

    /// Epoch milliseconds, or `BAD`. Needs 25 readable bytes at `p`.
    #[inline(always)]
    pub unsafe fn parse(&mut self, p: *const u8, len: usize) -> i64 {
        if len == 13 {
            return thirteen(p);
        }
        if len == 20 {
            if *p.add(19) == b'Z' {
                return self.datetime(p);
            }
        } else if len == 25 {
            let sign = *p.add(19);
            if sign == b'+' || sign == b'-' {
                return self.offset(p, sign);
            }
        }
        digits(p, len)
    }

    #[inline(never)]
    unsafe fn offset(&mut self, p: *const u8, sign: u8) -> i64 {
        let d = |i: usize| (*p.add(i)).wrapping_sub(b'0') as u32;
        let (a, b, c, e) = (d(20), d(21), d(23), d(24));
        if a > 9 || b > 9 || c > 9 || e > 9 || *p.add(22) != b':' {
            return BAD;
        }
        let (oh, om) = (a * 10 + b, c * 10 + e);
        if oh > 23 || om > 59 {
            return BAD;
        }
        let local = self.datetime(p);
        if local == BAD {
            return BAD;
        }
        let offset = ((oh * 3600 + om * 60) as i64) * 1000;
        if sign == b'+' {
            local - offset
        } else {
            local + offset
        }
    }
}

/// A 13-digit millisecond epoch in two groups of eight digits.
#[inline(always)]
unsafe fn thirteen(p: *const u8) -> i64 {
    let digits = _mm_xor_si128(_mm_loadu_si128(p.cast()), _mm_set1_epi8(b'0' as i8));
    let invalid = _mm_or_si128(digits, _mm_add_epi8(digits, _mm_set1_epi8(0x76)));
    if _mm_movemask_epi8(invalid) & 0x1fff != 0 {
        return BAD;
    }
    // Left-pad to 16 digits. Each multiply-add groups two adjacent decimal
    // values, first digits, then pairs, then four-digit values.
    let padded = _mm_slli_si128::<3>(digits);
    let pairs = _mm_maddubs_epi16(padded, _mm_set1_epi16(0x010a));
    let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_0064));
    let packed = _mm_packs_epi32(quads, quads);
    let octets = _mm_madd_epi16(packed, _mm_set1_epi32(0x0001_2710));
    let leading = _mm_cvtsi128_si32(octets) as i64;
    let trailing = _mm_extract_epi32::<1>(octets) as i64;
    leading * 100_000_000 + trailing
}

#[inline(always)]
unsafe fn digits(p: *const u8, len: usize) -> i64 {
    if len == 0 {
        return BAD;
    }
    let mut i = 0;
    while i < len && *p.add(i) == b'0' {
        i += 1;
    }
    // 16 or more significant digits is past year 300000: outside any window.
    if len - i > 15 {
        return BAD;
    }
    let mut v: u64 = 0;
    let mut end = len;
    let mut scale = 1;
    if len - i >= 8 {
        end = len - 8;
        scale = 100_000_000;
        v = decimal8(r8(p.add(end)) ^ 0x3030_3030_3030_3030);
        if v == u64::MAX {
            return BAD;
        }
    }
    let mut hi: u64 = 0;
    while i < end {
        let x = (*p.add(i)).wrapping_sub(b'0');
        if x > 9 {
            return BAD;
        }
        hi = hi * 10 + x as u64;
        i += 1;
    }
    (hi * scale + v) as i64
}

pub fn parse_slice(s: &[u8]) -> i64 {
    // Copy so the fixed-width forms never read past a short slice.
    let mut tmp = [0u8; 64];
    if s.len() > 40 {
        return BAD;
    }
    tmp[..s.len()].copy_from_slice(s);
    unsafe { Dates::new().parse(tmp.as_ptr(), s.len()) }
}
