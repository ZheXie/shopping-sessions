//! Products and campaigns: exact string -> index lookups.

use std::collections::HashMap;

/// Exact string -> int.
pub struct SMap(HashMap<Vec<u8>, i32>);
impl SMap {
    pub fn new() -> Self {
        SMap(HashMap::new())
    }
    /// false if already present
    pub fn insert(&mut self, k: &[u8], v: i32) -> bool {
        if self.0.contains_key(k) {
            return false;
        }
        self.0.insert(k.to_vec(), v);
        true
    }
    pub fn find(&self, k: &[u8]) -> i32 {
        *self.0.get(k).unwrap_or(&-1)
    }
}

/// Exact fast path for IDs shaped <prefix><1-9><0-9>* (p123, c_45): a dense
/// array built from every file entry of that shape. Any other string, and
/// any shaped string the file lacks, is still decided exactly.
pub struct NumMap {
    pre: Vec<u8>,
    arr: Vec<i32>,
    on: bool,
}
impl NumMap {
    pub fn new() -> Self {
        NumMap { pre: Vec::new(), arr: Vec::new(), on: false }
    }
    #[inline(always)]
    fn shape(p: &[u8], pre: &[u8]) -> Option<u64> {
        let (n, pl) = (p.len(), pre.len());
        if n <= pl || n - pl > 9 || &p[..pl] != pre {
            return None;
        }
        if !(b'1'..=b'9').contains(&p[pl]) {
            return None;
        }
        let mut x = 0u64;
        for &c in &p[pl..] {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            x = x * 10 + d as u64;
        }
        Some(x)
    }
    pub fn build(&mut self, ids: &[(Vec<u8>, i32)]) {
        let Some(first) = ids.first() else { return };
        let i = first.0.iter().position(|c| c.is_ascii_digit()).unwrap_or(first.0.len());
        if i > 8 {
            return;
        }
        self.pre = first.0[..i].to_vec();
        let mut mx = 0u64;
        for (s, _) in ids {
            if let Some(v) = Self::shape(s, &self.pre) {
                mx = mx.max(v);
            }
        }
        if mx >= 1 << 24 {
            return;
        }
        self.arr = vec![-1; mx as usize + 1];
        for (s, v) in ids {
            if let Some(k) = Self::shape(s, &self.pre) {
                self.arr[k as usize] = *v;
            }
        }
        self.on = true;
    }
    /// The number of a shaped ID, None if not shaped (or the fast path is off).
    /// Same answer as shape(), for a field inside the read buffer (>= 16
    /// readable bytes from p): up to 8 digits parsed at once (SWAR).
    #[inline(always)]
    pub unsafe fn num_buf(&self, p: *const u8, n: u32) -> Option<u64> {
        if !self.on {
            return None;
        }
        let pl = self.pre.len();
        let n = n as usize;
        if n <= pl || std::slice::from_raw_parts(p, pl) != self.pre.as_slice() {
            return None;
        }
        let d = n - pl;
        if d > 8 {
            return Self::shape(std::slice::from_raw_parts(p, n), &self.pre);
        }
        let mut x = crate::ids::ld64(p.add(pl)) ^ 0x3030303030303030;
        let m = if d == 8 { !0u64 } else { (1u64 << (8 * d)) - 1 };
        if (x.wrapping_add(0x7676767676767676) | x) & 0x8080808080808080 & m != 0 || x & 0xFF == 0 {
            return None; // a non-digit, or a leading zero
        }
        x = (x & m) << (8 * (8 - d)); // right-align: leading zero bytes
        x = (x.wrapping_mul(10) + (x >> 8)) & 0x00FF00FF00FF00FF; // 2 digits per 16 bits
        x = (x.wrapping_mul(100) + (x >> 16)) & 0x0000FFFF0000FFFF; // 4 digits per 32 bits
        x = (x.wrapping_mul(10000) + (x >> 32)) & 0xFFFFFFFF; // 8 digits
        Some(x)
    }
    /// Dense table number -> value for the shaped IDs (absent: dflt).
    pub fn table<T: Copy>(&self, dflt: T, val: impl Fn(i32) -> T) -> Vec<T> {
        self.arr.iter().map(|&i| if i < 0 { dflt } else { val(i) }).collect()
    }
    /// -2: not shaped (ask the hash map); -1: absent; else the value.
    /// (The pipeline uses num_buf + a dense table; kept as the plain form.)
    #[allow(dead_code)]
    pub fn find(&self, p: &[u8]) -> i32 {
        if !self.on {
            return -2;
        }
        match Self::shape(p, &self.pre) {
            None => -2,
            Some(v) => *self.arr.get(v as usize).unwrap_or(&-1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// num_buf must agree with shape() on every string
    #[test]
    fn num_buf_matches_shape() {
        let mut ids: Vec<(Vec<u8>, i32)> = (1..=50000).map(|i| (format!("p{i}").into_bytes(), i)).collect();
        ids.push((b"p12345678".to_vec(), 7));
        let mut m = NumMap::new();
        m.build(&ids);
        let al = b"0123456789pP_ x";
        let mut seed = 12345u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..3_000_000 {
            let len = (rnd() % 12) as usize;
            let mut buf = [0u8; 40];
            for b in buf.iter_mut() {
                *b = al[(rnd() % al.len() as u64) as usize];
            }
            if rnd() % 2 == 0 {
                buf[0] = b'p';
                for b in buf.iter_mut().take(len).skip(1) {
                    *b = b'0' + (rnd() % 10) as u8;
                }
            }
            let a = NumMap::shape(&buf[..len], &m.pre);
            let b = unsafe { m.num_buf(buf.as_ptr(), len as u32) };
            assert_eq!(a, b, "{:?}", std::str::from_utf8(&buf[..len]));
        }
    }
}
