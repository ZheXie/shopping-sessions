//! Index the contest CSV dialect. Every comma separates fields, including
//! inside malformed lines. Quotes must wrap a whole field.

use std::arch::x86_64::*;

const LOOKAHEAD: usize = 1024;
const BYTE_LSB: u64 = 0x0101_0101_0101_0101;

pub struct Scan {
    /// Byte offset and two tag bits per separator. Bit 0 means the field is
    /// quoted, bit 1 that this separator is a newline.
    seps: Vec<u32>,
    tags: Vec<u8>,
    /// Byte positions of malformed quotes or field ends. Sentinel terminated.
    errors: Vec<u32>,
    /// Includes only complete lines in the separator index.
    total: usize,
    wide: bool,
}

impl Scan {
    pub fn new() -> Self {
        Scan {
            seps: Vec::with_capacity(1 << 14),
            tags: Vec::with_capacity(1 << 14),
            errors: Vec::with_capacity(16),
            total: 0,
            wide: std::env::var_os("SS_AVX2").as_deref() != Some(std::ffi::OsStr::new("1"))
                && is_x86_feature_detected!("avx512vbmi2")
                && is_x86_feature_detected!("avx512bw"),
        }
    }

    /// Stop once the scan reaches `soft_end` and includes a complete line.
    /// The mapping guarantees 64 readable bytes of slack after the input.
    pub unsafe fn scan(
        &mut self,
        buf: *const u8,
        len: usize,
        start: usize,
        soft_end: usize,
    ) -> usize {
        if self.wide {
            self.wide(buf, len, start, soft_end)
        } else {
            self.index::<false>(buf, len, start, soft_end)
        }
    }

    #[target_feature(enable = "avx512vbmi2,avx512bw")]
    unsafe fn wide(&mut self, buf: *const u8, len: usize, start: usize, soft_end: usize) -> usize {
        self.index::<true>(buf, len, start, soft_end)
    }

    #[inline(always)]
    unsafe fn index<const WIDE: bool>(
        &mut self,
        buf: *const u8,
        len: usize,
        start: usize,
        soft_end: usize,
    ) -> usize {
        self.seps.clear();
        self.tags.clear();
        self.errors.clear();
        let mut nsep = 0usize;
        let mut total = 0usize;
        let mut prev_sep = 1u64;
        let mut prev_quote = 0u64;
        let mut next_field_quoted = (*buf.add(start) == b'"') as u64;
        let mut pos = start;
        while pos < len {
            // A block contributes at most 64 separators. SIMD stores can use
            // unused lanes beyond the last separator.
            if nsep + 72 > self.seps.capacity().min(self.tags.capacity()) {
                self.seps.set_len(nsep);
                self.tags.set_len(nsep);
                self.seps.reserve(nsep.max(4096));
                self.tags.reserve(nsep.max(4096));
            }
            _mm_prefetch(buf.add((pos + LOOKAHEAD).min(len)).cast(), _MM_HINT_T0);
            let (mut commas, mut newlines, mut quotes) = classify::<WIDE>(buf.add(pos));
            let remaining = len - pos;
            if remaining < 64 {
                let keep = (1u64 << remaining) - 1;
                commas &= keep;
                newlines &= keep;
                quotes &= keep;
            }
            let separators = commas | newlines;
            let count = separators.count_ones() as usize;
            let offset = (pos - start) as u32;
            let after = *buf.add(pos + 64);
            let after_sep = matches!(after, b',' | b'\n') as u64;
            let left = (separators << 1) | prev_sep;
            let right = (separators >> 1) | (after_sep << 63);
            // An interior quote has neither neighbour as a separator. A quote
            // with both neighbours as separators is an invalid one-byte field.
            let mut bad = quotes & !(left ^ right);
            prev_sep = separators >> 63;

            // In separator order, each closing flag must equal the previous
            // separator's opening flag, including across a block boundary.
            let closes = _pext_u64((quotes << 1) | prev_quote, separators);
            let opens = _pext_u64((quotes >> 1) | (((after == b'"') as u64) << 63), separators);
            let mismatch = closes ^ _bzhi_u64((opens << 1) | next_field_quoted, count as u32);
            if count != 0 {
                next_field_quoted = opens >> (count - 1);
            }
            prev_quote = quotes >> 63;
            if mismatch != 0 {
                bad |= _pdep_u64(mismatch, separators);
            }
            while bad != 0 {
                self.errors.push(offset + bad.trailing_zeros());
                bad &= bad - 1;
            }
            let ends = _pext_u64(newlines, separators);
            if ends != 0 {
                total = nsep + (64 - ends.leading_zeros()) as usize;
            }
            let dst = self.tags.as_mut_ptr().add(nsep);
            let mut i = 0;
            loop {
                let tags = _pdep_u64(closes >> i, BYTE_LSB) | _pdep_u64(ends >> i, BYTE_LSB << 1);
                dst.add(i).cast::<u64>().write_unaligned(tags);
                i += 8;
                if i >= count {
                    break;
                }
            }
            write_positions::<WIDE>(self.seps.as_mut_ptr().add(nsep), offset, separators);
            nsep += count;
            pos += 64;
            if pos >= soft_end && total != 0 {
                break;
            }
        }
        self.total = total;
        self.seps.set_len(nsep);
        self.tags.set_len(nsep);
        // The row loop reads eight tags, including for an incomplete tail.
        self.tags.extend_from_slice(&[0; 8]);
        self.errors.push(u32::MAX);
        if total == 0 {
            len
        } else {
            start + *self.seps.get_unchecked(total - 1) as usize + 1
        }
    }
}

#[inline(always)]
unsafe fn classify<const WIDE: bool>(p: *const u8) -> (u64, u64, u64) {
    if WIDE {
        let v = _mm512_loadu_si512(p.cast());
        let mask = |c| _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(c));
        (mask(b',' as i8), mask(b'\n' as i8), mask(b'"' as i8))
    } else {
        let a = _mm256_loadu_si256(p.cast());
        let b = _mm256_loadu_si256(p.add(32).cast());
        let mask = |c| {
            let v = _mm256_set1_epi8(c);
            u64::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32)
                | (u64::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32) << 32)
        };
        (mask(b',' as i8), mask(b'\n' as i8), mask(b'"' as i8))
    }
}

const POSITIONS: [u8; 64] = {
    let mut a = [0u8; 64];
    let mut i = 0;
    while i < 64 {
        a[i] = i as u8;
        i += 1;
    }
    a
};

#[inline(always)]
unsafe fn write_positions<const WIDE: bool>(dst: *mut u32, offset: u32, mut bits: u64) {
    if WIDE {
        let count = bits.count_ones();
        let positions =
            _mm512_maskz_compress_epi8(bits, _mm512_loadu_si512(POSITIONS.as_ptr().cast()));
        let base = _mm512_set1_epi32(offset as i32);
        let store = |chunk, i| {
            _mm512_storeu_si512(
                dst.add(i).cast(),
                _mm512_add_epi32(base, _mm512_cvtepu8_epi32(chunk)),
            );
        };
        store(_mm512_castsi512_si128(positions), 0);
        if count > 16 {
            store(_mm512_extracti32x4_epi32::<1>(positions), 16);
            if count > 32 {
                store(_mm512_extracti32x4_epi32::<2>(positions), 32);
                if count > 48 {
                    store(_mm512_extracti32x4_epi32::<3>(positions), 48);
                }
            }
        }
    } else {
        let mut i = 0;
        loop {
            for j in 0..8 {
                *dst.add(i + j) = offset + bits.trailing_zeros();
                bits &= bits.wrapping_sub(1);
            }
            if bits == 0 {
                break;
            }
            i += 8;
        }
    }
}

/// Visit each well-formed row with the unquoted byte range of each field.
#[inline(always)]
pub unsafe fn rows<const N: usize, F: FnMut(&[(u32, u32); N])>(sc: &Scan, limit: usize, mut f: F) {
    const {
        assert!(N > 0 && N <= 8);
    }
    let newline_mask = (BYTE_LSB << 1) >> ((8 - N) * 8);
    let mut first = 0usize;
    let mut row = 0u32;
    let mut error = sc.errors.as_ptr();
    while first < sc.total && (row as usize) < limit {
        let tags = sc.tags.as_ptr().add(first).cast::<u64>().read_unaligned();
        if tags & newline_mask == 2 << (8 * (N - 1)) {
            let mut fields = [(0u32, 0u32); N];
            let mut begin = row;
            for (i, field) in fields.iter_mut().enumerate() {
                let end = *sc.seps.get_unchecked(first + i);
                let trim = ((tags >> (8 * i)) & 1) as u32;
                *field = (begin + trim, end - trim);
                begin = end + 1;
            }
            row = begin;
            first += N;
            if *error >= row {
                f(&fields);
            }
        } else {
            while *sc.tags.get_unchecked(first) & 2 == 0 {
                first += 1;
            }
            row = *sc.seps.get_unchecked(first) + 1;
            first += 1;
        }
        while *error < row {
            error = error.add(1);
        }
    }
}

/// Offset of the first line starting at or after `from`, excluding the header.
pub unsafe fn line_start(buf: *const u8, len: usize, from: usize) -> usize {
    let mut i = from.saturating_sub(1);
    while i < len && *buf.add(i) != b'\n' {
        i += 1;
    }
    (i + 1).min(len)
}
