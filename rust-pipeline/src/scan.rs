//! CSV dialect of rules.md, vectorised.
//!
//! Only `\n` ends a line and every comma separates fields, so a block of text
//! is indexed in one pass: the positions of all commas and newlines. A field
//! is quoted when it is at least two bytes and starts and ends with `"`; a
//! line is well formed when it has the right number of fields and every quote
//! on it is one of those wrapping quotes.

use std::arch::x86_64::*;

pub struct Scan {
    /// Offsets (from the scan start) of every comma and newline, in order.
    pub seps: Vec<u32>,
    /// Offsets of quotes with no separator on either side, which make their
    /// line malformed. Ends with `u32::MAX`.
    pub inner: Vec<u32>,
    /// Number of entries of `seps` up to and including the last newline.
    pub total: usize,
}

/// How far ahead of the scan to prefetch. Hardware prefetchers stop at page
/// boundaries; this also walks the page tables early.
const AHEAD: usize = 1024;

impl Scan {
    pub fn new() -> Self {
        Scan {
            seps: Vec::with_capacity(1 << 16),
            inner: Vec::with_capacity(64),
            total: 0,
        }
    }

    /// Indexes whole lines from `start` (a line start) until at least
    /// `soft_end` is passed and one line is complete. Returns the offset of
    /// the first line not indexed. Needs `SLACK` readable bytes after `len`.
    #[inline(never)]
    pub unsafe fn scan(
        &mut self,
        buf: *const u8,
        len: usize,
        start: usize,
        soft_end: usize,
    ) -> usize {
        self.inner.clear();
        let vc = _mm256_set1_epi8(b',' as i8);
        let vn = _mm256_set1_epi8(b'\n' as i8);
        let vq = _mm256_set1_epi8(b'"' as i8);
        let mut pos = start;
        let mut nsep = 0usize;
        let mut total = 0usize;
        // Whether the byte before this block is a separator.
        let mut carry = 1u64;
        let mut cap = self.seps.capacity();
        let mut out = self.seps.as_mut_ptr();
        while pos < len {
            if nsep + 72 > cap {
                self.seps.set_len(nsep);
                self.seps.reserve(cap.max(4096));
                cap = self.seps.capacity();
                out = self.seps.as_mut_ptr();
            }
            _mm_prefetch(buf.add((pos + AHEAD).min(len)) as *const i8, _MM_HINT_T0);
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
            let after = *buf.add(pos + 64);
            let sep_after = ((after == b',') | (after == b'\n')) as u64;
            let inner = qm & !((sm << 1) | carry) & !((sm >> 1) | (sep_after << 63));
            carry = sm >> 63;
            let rel = (pos - start) as u32;
            if inner != 0 {
                let mut m = inner;
                while m != 0 {
                    self.inner.push(rel + m.trailing_zeros());
                    m &= m - 1;
                }
            }
            let mut bits = sm;
            let mut o = out.add(nsep);
            loop {
                for k in 0..8 {
                    *o.add(k) = rel + bits.trailing_zeros();
                    bits &= bits.wrapping_sub(1);
                }
                if bits == 0 {
                    break;
                }
                o = o.add(8);
            }
            if nm != 0 {
                total = nsep + (sm & (u64::MAX >> nm.leading_zeros())).count_ones() as usize;
            }
            nsep += sm.count_ones() as usize;
            pos += 64;
            if pos >= soft_end && total != 0 {
                break;
            }
        }
        self.seps.set_len(nsep);
        self.inner.push(u32::MAX);
        self.total = total;
        if total == 0 {
            len
        } else {
            start + *out.add(total - 1) as usize + 1
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
    let total = sc.total;
    let mut inner = sc.inner.as_ptr();
    let mut first = 0usize;
    let mut row = 0u32;
    while first < total {
        if row as usize >= limit {
            break;
        }
        if first + N <= total {
            let s = seps.add(first);
            let mut out = [(0u32, 0u32); N];
            let mut b = row;
            // Bit 0: a quote that does not wrap its field. Bit 1: a separator
            // of the wrong kind, so not exactly N fields.
            let mut flaw = 0u32;
            for j in 0..N {
                let e = *s.add(j);
                let open = (*base.add(b as usize) == b'"') as u32;
                let close = (*base.add(e as usize).sub(1) == b'"') as u32;
                flaw |= (open ^ close) | (open & (e - b == 1) as u32);
                let sep = if j == N - 1 { b'\n' } else { b',' };
                flaw |= ((*base.add(e as usize) != sep) as u32) << 1;
                *out.get_unchecked_mut(j) = (b + open, e - close);
                b = e + 1;
            }
            if flaw < 2 {
                let end = *s.add(N - 1);
                first += N;
                row = end + 1;
                while *inner < end {
                    flaw = 1;
                    inner = inner.add(1);
                }
                if flaw == 0 {
                    f(&out);
                }
                continue;
            }
        }
        // Not N fields: drop the line.
        let mut i = first;
        while *base.add(*seps.add(i) as usize) != b'\n' {
            i += 1;
        }
        let end = *seps.add(i);
        while *inner < end {
            inner = inner.add(1);
        }
        first = i + 1;
        row = end + 1;
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
