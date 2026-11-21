//! CSV dialect of rules.md, vectorised.
//!
//! Only `\n` ends a line and every comma separates fields, so a block of text
//! is indexed in one pass: the positions of all commas and newlines, plus a
//! running count of quotes. A field is quoted when it is at least two bytes
//! and starts and ends with `"`; a line is well formed when it has the right
//! number of fields and every quote on it is one of those wrapping quotes.

use std::arch::x86_64::*;

pub struct Scan {
    /// Offsets (from the scan start) of every comma and newline, in order.
    pub seps: Vec<u32>,
    /// For each newline, its index in `seps`.
    pub nl: Vec<u32>,
    /// For each newline, the number of quotes seen before it.
    pub nlq: Vec<u32>,
}

impl Scan {
    pub fn new() -> Self {
        Scan {
            seps: Vec::with_capacity(1 << 16),
            nl: Vec::with_capacity(1 << 12),
            nlq: Vec::with_capacity(1 << 12),
        }
    }

    /// Indexes whole lines from `start` (a line start) until at least
    /// `soft_end` is passed and one line is complete. Returns the offset of
    /// the first line not indexed. Needs `SLACK` readable bytes after `len`.
    #[inline(never)]
    pub unsafe fn scan(&mut self, buf: *const u8, len: usize, start: usize, soft_end: usize) -> usize {
        self.nl.clear();
        self.nlq.clear();
        let vc = _mm256_set1_epi8(b',' as i8);
        let vn = _mm256_set1_epi8(b'\n' as i8);
        let vq = _mm256_set1_epi8(b'"' as i8);
        let mut pos = start;
        let mut nsep = 0usize;
        let mut quotes = 0u32;
        let mut cap = self.seps.capacity();
        let mut out = self.seps.as_mut_ptr();
        while pos < len {
            if nsep + 72 > cap {
                self.seps.set_len(nsep);
                self.seps.reserve(cap.max(4096));
                cap = self.seps.capacity();
                out = self.seps.as_mut_ptr();
            }
            let a = _mm256_loadu_si256(buf.add(pos) as *const __m256i);
            let b = _mm256_loadu_si256(buf.add(pos + 32) as *const __m256i);
            let mask = |v: __m256i| -> u64 {
                (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64)
                    | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64) << 32)
            };
            let mut cm = mask(vc);
            let mut nm = mask(vn);
            let mut qm = mask(vq);
            let rem = len - pos;
            if rem < 64 {
                let keep = (1u64 << rem) - 1;
                cm &= keep;
                nm &= keep;
                qm &= keep;
            }
            let sm = cm | nm;
            let rel = (pos - start) as u32;
            let mut bits = sm;
            let mut i = nsep;
            while bits != 0 {
                for _ in 0..8 {
                    *out.add(i) = rel + bits.trailing_zeros();
                    bits &= bits.wrapping_sub(1);
                    i += 1;
                }
            }
            let mut m = nm;
            while m != 0 {
                let below = (1u64 << m.trailing_zeros()) - 1;
                self.nl.push((nsep + (sm & below).count_ones() as usize) as u32);
                self.nlq.push(quotes + (qm & below).count_ones());
                m &= m - 1;
            }
            nsep += sm.count_ones() as usize;
            quotes += qm.count_ones();
            pos += 64;
            if pos >= soft_end && !self.nl.is_empty() {
                break;
            }
        }
        self.seps.set_len(nsep);
        match self.nl.last() {
            Some(&last) => start + *out.add(last as usize) as usize + 1,
            None => len,
        }
    }
}

/// Calls `f` with the unquoted `(begin, end)` of each field of every
/// well-formed `N`-field line that starts before `limit`. Offsets are from
/// `base`, the address the scan started at.
#[inline(always)]
pub unsafe fn rows<const N: usize, F: FnMut(&[(u32, u32); N])>(
    sc: &Scan,
    base: *const u8,
    limit: usize,
    mut f: F,
) {
    let seps = sc.seps.as_ptr();
    let nl = sc.nl.as_ptr();
    let nlq = sc.nlq.as_ptr();
    let mut first = 0usize;
    let mut row_start = 0u32;
    let mut quotes_before = 0u32;
    for r in 0..sc.nl.len() {
        let end = *nl.add(r) as usize;
        let q = *nlq.add(r);
        let row = row_start;
        let s = seps.add(first);
        let fields = end + 1 - first;
        let quotes = q - quotes_before;
        first = end + 1;
        row_start = *seps.add(end) + 1;
        quotes_before = q;
        if row as usize >= limit {
            break;
        }
        if fields != N {
            continue;
        }
        let mut out = [(0u32, 0u32); N];
        let mut b = row;
        let mut quoted = 0u32;
        for j in 0..N {
            let e = *s.add(j);
            let wrapped = ((e - b >= 2)
                & (*base.add(b as usize) == b'"')
                & (*base.offset(e as isize - 1) == b'"')) as u32;
            *out.get_unchecked_mut(j) = (b + wrapped, e - wrapped);
            quoted += wrapped;
            b = e + 1;
        }
        if quotes != 2 * quoted {
            continue;
        }
        f(&out);
    }
}

/// Offset of the first line that starts at or after `from`, never the header.
pub unsafe fn line_start(buf: *const u8, len: usize, from: usize) -> usize {
    let mut i = from.saturating_sub(1);
    while i < len && *buf.add(i) != b'\n' {
        i += 1;
    }
    (i + 1).min(len)
}
