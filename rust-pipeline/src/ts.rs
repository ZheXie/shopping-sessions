//! Timestamps: F1 `2026-10-03T23:58:10Z`, F2 `...+HH:MM`, F3 epoch millis.

pub const BAD: i64 = i64::MIN;
pub const DAY_MS: i64 = 86_400_000;

static DAYS_IN_MONTH: [u8; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// `YYYY-MM-DDTHH:MM:SS` at `p` (19 readable bytes) as UTC milliseconds.
#[inline(always)]
unsafe fn civil19(p: *const u8) -> i64 {
    let d = |i: usize| (*p.add(i)).wrapping_sub(b'0') as u32;
    let (y0, y1, y2, y3) = (d(0), d(1), d(2), d(3));
    let (m0, m1, d0, d1) = (d(5), d(6), d(8), d(9));
    let (h0, h1, i0, i1, s0, s1) = (d(11), d(12), d(14), d(15), d(17), d(18));
    // x <= 9 exactly when neither x nor x + 6 has a bit above the low nibble.
    let digits = y0
        | y1
        | y2
        | y3
        | m0
        | m1
        | d0
        | d1
        | h0
        | h1
        | i0
        | i1
        | s0
        | s1
        | (y0 + 6)
        | (y1 + 6)
        | (y2 + 6)
        | (y3 + 6)
        | (m0 + 6)
        | (m1 + 6)
        | (d0 + 6)
        | (d1 + 6)
        | (h0 + 6)
        | (h1 + 6)
        | (i0 + 6)
        | (i1 + 6)
        | (s0 + 6)
        | (s1 + 6);
    let seps = (*p.add(4) ^ b'-')
        | (*p.add(7) ^ b'-')
        | (*p.add(10) ^ b'T')
        | (*p.add(13) ^ b':')
        | (*p.add(16) ^ b':');
    if (digits & !0xF) != 0 || seps != 0 {
        return BAD;
    }
    let y = (y0 * 1000 + y1 * 100 + y2 * 10 + y3) as i64;
    let mo = m0 * 10 + m1;
    let dd = d0 * 10 + d1;
    let h = h0 * 10 + h1;
    let mi = i0 * 10 + i1;
    let s = s0 * 10 + s1;
    if mo.wrapping_sub(1) > 11 || h > 23 || mi > 59 || s > 59 || dd == 0 {
        return BAD;
    }
    let mut dim = *DAYS_IN_MONTH.get_unchecked(mo as usize) as u32;
    if mo == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
        dim = 29;
    }
    if dd > dim {
        return BAD;
    }
    days_from_civil(y, mo as i64, dd as i64) * DAY_MS + ((h * 3600 + mi * 60 + s) as i64) * 1000
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
    while i < len {
        let x = (*p.add(i)).wrapping_sub(b'0');
        if x > 9 {
            return BAD;
        }
        v = v * 10 + x as u64;
        i += 1;
    }
    v as i64
}

/// Epoch milliseconds, or `BAD`.
#[inline(always)]
pub unsafe fn parse(p: *const u8, len: usize) -> i64 {
    if len == 20 {
        if *p.add(19) == b'Z' {
            return civil19(p);
        }
    } else if len == 25 {
        let sign = *p.add(19);
        if sign == b'+' || sign == b'-' {
            let d = |i: usize| (*p.add(i)).wrapping_sub(b'0') as u32;
            let (a, b, c, e) = (d(20), d(21), d(23), d(24));
            if a > 9 || b > 9 || c > 9 || e > 9 || *p.add(22) != b':' {
                return BAD;
            }
            let oh = a * 10 + b;
            let om = c * 10 + e;
            if oh > 23 || om > 59 {
                return BAD;
            }
            let local = civil19(p);
            if local == BAD {
                return BAD;
            }
            let offset = ((oh * 3600 + om * 60) as i64) * 1000;
            return if sign == b'+' {
                local - offset
            } else {
                local + offset
            };
        }
    }
    digits(p, len)
}

pub fn parse_slice(s: &[u8]) -> i64 {
    // Copy so the 25-byte forms never read past a short slice.
    let mut tmp = [0u8; 64];
    if s.len() > 40 {
        return BAD;
    }
    tmp[..s.len()].copy_from_slice(s);
    unsafe { parse(tmp.as_ptr(), s.len()) }
}
