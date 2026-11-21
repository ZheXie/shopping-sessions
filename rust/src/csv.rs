//! The CSV dialect: line splitting (scalar and AVX2) and the chunk reader.

use std::arch::x86_64::*;
use std::fs::File;
use std::os::unix::fs::FileExt;

use crate::sys::die;

#[derive(Clone, Copy)]
pub struct Fld {
    pub p: *const u8,
    pub n: u32,
}
impl Default for Fld {
    fn default() -> Self {
        Fld { p: std::ptr::null(), n: 0 }
    }
}
impl Fld {
    #[inline(always)]
    pub fn bytes<'a>(&self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.p, self.n as usize) }
    }
}

/// Split one line [s, e) into exactly nf fields. Quotes are syntax only when
/// they wrap a whole field; any other quote, a wrong count, or an empty line
/// is malformed.
pub unsafe fn split(s: *const u8, e: *const u8, f: &mut [Fld], nf: usize) -> bool {
    if s == e {
        return false;
    }
    let mut k = 0usize;
    let mut fs = s;
    let mut q = 0;
    let mut c = s;
    loop {
        if c == e || *c == b',' {
            if k >= nf {
                return false;
            }
            let len = c.offset_from(fs) as u32;
            if q != 0 {
                if q != 2 || len < 2 || *fs != b'"' || *c.sub(1) != b'"' {
                    return false;
                }
                f[k] = Fld { p: fs.add(1), n: len - 2 };
            } else {
                f[k] = Fld { p: fs, n: len };
            }
            k += 1;
            if c == e {
                break;
            }
            fs = c.add(1);
            q = 0;
        } else if *c == b'"' {
            q += 1;
        }
        c = c.add(1);
    }
    k == nf
}

#[inline(always)]
unsafe fn mask64(p: *const u8, v: __m256i) -> u64 {
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64)
        | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64) << 32)
}
#[inline(always)]
fn ctz128(x: u128) -> u32 {
    let lo = x as u64;
    if lo != 0 {
        lo.trailing_zeros()
    } else {
        64 + ((x >> 64) as u64).trailing_zeros()
    }
}

/// The line at p and 128 bytes from p are readable. 1 ok, 0 malformed,
/// -1 no '\n' in those 128 bytes. *le = the line's '\n' unless -1.
#[inline(always)]
unsafe fn split_simd(p: *const u8, f: &mut [Fld], nf: usize, le: &mut *const u8) -> i32 {
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let cm = _mm256_set1_epi8(b',' as i8);
    let qt = _mm256_set1_epi8(b'"' as i8);
    let mut n = mask64(p, nl) as u128;
    let mut c = mask64(p, cm) as u128;
    let mut q = mask64(p, qt) as u128;
    if n == 0 {
        n = (mask64(p.add(64), nl) as u128) << 64;
        if n == 0 {
            return -1;
        }
        c |= (mask64(p.add(64), cm) as u128) << 64;
        q |= (mask64(p.add(64), qt) as u128) << 64;
    }
    let l = ctz128(n) as usize;
    *le = p.add(l);
    if l == 0 {
        return 0;
    }
    let lm = (1u128 << l) - 1;
    c &= lm;
    q &= lm;
    if c.count_ones() as usize != nf - 1 {
        return 0;
    }
    // A field that starts with '"' must end with '"' and be >= 2 bytes; then
    // the line is well quoted iff its quote count is exactly 2 per such field
    // (any other quote, in any field, makes the count too high).
    let mut c0 = c as u64;
    let mut c1 = (c >> 64) as u64;
    let mut a = 0usize;
    let mut nq = 0u32;
    let mut bad = 0u32;
    for k in 0..nf {
        let b: usize;
        if k < nf - 1 {
            if c0 != 0 {
                b = c0.trailing_zeros() as usize;
                c0 &= c0 - 1;
            } else {
                b = 64 + c1.trailing_zeros() as usize;
                c1 &= c1 - 1;
            }
        } else {
            b = l;
        }
        let qa = (*p.add(a) == b'"') as u32; // p[a] is the delimiter when the field is empty
        let qe = ((b >= a + 2) & (*p.add(if b > a { b - 1 } else { a }) == b'"')) as u32;
        bad |= qa & (qe ^ 1);
        *f.get_unchecked_mut(k) = Fld { p: p.add(a + qa as usize), n: (b - a) as u32 - 2 * qa };
        nq += qa;
        a = b + 1;
    }
    (bad == 0 && q.count_ones() == 2 * nq) as i32
}

/// Split the line at p (the buffer ends at fe). 1 ok, 0 malformed (skip it),
/// -2 the line's end is not buffered yet and this is not the end of the file.
#[inline(always)]
pub unsafe fn split_line(p: *const u8, fe: *const u8, eof: bool, f: &mut [Fld], nf: usize, next: &mut *const u8) -> i32 {
    if fe.offset_from(p) >= 128 {
        let mut le = p;
        let r = split_simd(p, f, nf, &mut le);
        if r >= 0 {
            *next = le.add(1);
            return r;
        }
    }
    let rest = std::slice::from_raw_parts(p, fe.offset_from(p) as usize);
    let le = match rest.iter().position(|&b| b == b'\n') {
        Some(i) => p.add(i),
        None => {
            if !eof {
                return -2;
            }
            fe
        }
    };
    *next = le.add(1);
    split(p, le, f, nf) as i32
}

// ------------------------------------------------------------------ reader
/// A byte range of one input file; the lines that START inside [b, e) belong
/// to it. kind: 0 events, 1 clicks, 2 links.
#[derive(Clone, Copy)]
pub struct Chunk {
    pub kind: u8,
    pub file: usize,
    pub size: usize,
    pub b: usize,
    pub e: usize,
}
pub const PAD: usize = 256;
/// Streams a chunk's lines through a reusable buffer (pread). The line at
/// file offset 0 is the header and is skipped. At least PAD readable bytes
/// follow the buffered data, so SIMD loads past a field stay in bounds.
pub struct Reader {
    buf: Vec<u8>,
    rbuf: usize,
    minleft: usize,
}
impl Reader {
    pub fn new(rbuf: usize) -> Self {
        Reader { buf: vec![0u8; rbuf + PAD], rbuf, minleft: (rbuf / 4).min(1 << 16) }
    }
    /// fn(line_start, buffer_end, at_eof) -> next line start, or null when the
    /// line runs past the buffered data.
    pub fn run<F: FnMut(*const u8, *const u8, bool) -> *const u8>(&mut self, c: &Chunk, file: &File, mut f: F) {
        let _ = self.rbuf;
        let mut off = if c.b > 0 { c.b - 1 } else { 0 }; // file offset of buf[0]
        let mut len = 0usize; // valid bytes in buf
        let minleft = self.minleft;
        let buf = &mut self.buf;
        let fill = |buf: &mut Vec<u8>, off: &mut usize, len: &mut usize, keep_from: usize| {
            let keep = *len - keep_from;
            if keep_from > 0 {
                buf.copy_within(keep_from..*len, 0);
            }
            *off += keep_from;
            *len = keep;
            if buf.len() < *len + minleft + PAD {
                let nl = buf.len() * 2;
                buf.resize(nl, 0);
            }
            let mut room = buf.len() - PAD - *len;
            while room > 0 && *off + *len < c.size {
                let want = room.min(c.size - *off - *len);
                match file.read_at(&mut buf[*len..*len + want], (*off + *len) as u64) {
                    Ok(0) => break,
                    Ok(r) => {
                        *len += r;
                        room -= r;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => die("pread"),
                }
            }
            for x in &mut buf[*len..*len + PAD] {
                *x = 0;
            }
        };
        fill(buf, &mut off, &mut len, 0);
        let mut p: usize; // index of the current line start
        if c.b == 0 || buf[0] != b'\n' {
            // skip the header (b == 0) or the tail of a line that started before b
            let mut i = if c.b > 0 { 1 } else { 0 };
            loop {
                if let Some(j) = buf[i..len].iter().position(|&x| x == b'\n') {
                    p = i + j + 1;
                    break;
                }
                if off + len >= c.size {
                    return;
                }
                i = len;
                fill(buf, &mut off, &mut len, 0);
            }
        } else {
            p = 1;
        }
        loop {
            if off + p >= c.e || off + p >= c.size {
                return;
            }
            let mut eof = off + len >= c.size;
            if !eof && len - p < minleft {
                fill(buf, &mut off, &mut len, p);
                p = 0;
                eof = off + len >= c.size;
            }
            let base = buf.as_ptr();
            let np = unsafe { f(base.add(p), base.add(len), eof) };
            if np.is_null() {
                // the line runs past the buffer: grow and read more, then retry it
                let old = len - p;
                fill(buf, &mut off, &mut len, p);
                p = 0;
                if len == old && off + len < c.size {
                    die("reader stuck");
                }
                continue;
            }
            p = unsafe { np.offset_from(buf.as_ptr()) } as usize;
        }
    }
}

/// Data lines of a whole small file (header skipped), without the reader.
pub fn each_line<F: FnMut(&[u8])>(d: &[u8], mut f: F) {
    let Some(first) = d.iter().position(|&b| b == b'\n') else { return };
    let mut p = first + 1;
    while p < d.len() {
        let e = d[p..].iter().position(|&b| b == b'\n').map(|i| p + i).unwrap_or(d.len());
        f(&d[p..e]);
        p = e + 1;
    }
}
