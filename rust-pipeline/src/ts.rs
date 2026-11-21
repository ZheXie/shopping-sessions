//! F1 / F2 / F3 timestamp parsing → epoch milliseconds UTC.

#[inline]
fn is_ascii_digits(s: &[u8]) -> bool {
    !s.is_empty() && s.iter().all(|c| c.is_ascii_digit())
}

#[inline]
fn parse_u32(s: &[u8]) -> Option<u32> {
    if !is_ascii_digits(s) {
        return None;
    }
    let mut v: u32 = 0;
    for &c in s {
        v = v.checked_mul(10)?.checked_add((c - b'0') as u32)?;
    }
    Some(v)
}

fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Days from civil date to Unix epoch (1970-01-01), Howard Hinnant algorithm.
fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let mut y = y;
    let m = m as i32;
    let d = d as i32;
    y -= (m <= 2) as i32;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as u32;
    (era as i64) * 146097 + doe as i64 - 719468
}

fn ymd_hms_to_ms(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> Option<i64> {
    if mo < 1 || mo > 12 || d < 1 || d > days_in_month(y, mo) {
        return None;
    }
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    Some(days * 86_400_000 + ((h as i64) * 3600 + (mi as i64) * 60 + s as i64) * 1000)
}

/// Parse F1/F2/F3. None = bad form (or F3 overflowing i64).
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    if is_ascii_digits(s) {
        // F3
        let mut v: i128 = 0;
        for &c in s {
            v = v * 10 + (c - b'0') as i128;
        }
        if v > i64::MAX as i128 {
            return None; // outside any window
        }
        return Some(v as i64);
    }

    if s.len() < 20
        || s[4] != b'-'
        || s[7] != b'-'
        || s[10] != b'T'
        || s[13] != b':'
        || s[16] != b':'
    {
        return None;
    }
    let y = parse_u32(&s[0..4])? as i32;
    let mo = parse_u32(&s[5..7])?;
    let d = parse_u32(&s[8..10])?;
    let h = parse_u32(&s[11..13])?;
    let mi = parse_u32(&s[14..16])?;
    let sec = parse_u32(&s[17..19])?;

    let rest = &s[19..];
    if rest == b"Z" {
        return ymd_hms_to_ms(y, mo, d, h, mi, sec);
    }
    if rest.len() != 6 || (rest[0] != b'+' && rest[0] != b'-') || rest[3] != b':' {
        return None;
    }
    let oh = parse_u32(&rest[1..3])?;
    let om = parse_u32(&rest[4..6])?;
    if oh > 23 || om > 59 {
        return None;
    }
    let local = ymd_hms_to_ms(y, mo, d, h, mi, sec)?;
    let off_ms = ((oh as i64) * 3600 + (om as i64) * 60) * 1000;
    let utc = if rest[0] == b'+' {
        local - off_ms
    } else {
        local + off_ms
    };
    Some(utc)
}

#[inline]
pub fn utc_day(ms: i64) -> i32 {
    // Date32 days since Unix epoch. Floor toward -inf for negative.
    if ms >= 0 {
        (ms / 86_400_000) as i32
    } else {
        let q = ms / 86_400_000;
        let r = ms % 86_400_000;
        if r == 0 {
            q as i32
        } else {
            (q - 1) as i32
        }
    }
}
