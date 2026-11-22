//! Product and campaign dictionaries. Short UTF-8 byte strings are their own
//! exact keys; longer ones are compared byte-for-byte in the overflow map.
use crate::hashx::{decimal8, r8};
use std::collections::HashMap;

const MASK: [u64; 8] = [
    0,
    0xff,
    0xffff,
    0xff_ffff,
    0xffff_ffff,
    0xff_ffff_ffff,
    0xffff_ffff_ffff,
    0xff_ffff_ffff_ffff,
];

pub struct SmallTab {
    direct: Vec<u16>,
    prefix: u16,
    prefix_len: usize,
    keys: Vec<u64>,
    values: Vec<u32>,
    long: HashMap<Vec<u8>, u32>,
    shift: u32,
}

impl SmallTab {
    pub fn new(rows: usize, prefix: &[u8]) -> Self {
        let prefix_word = match prefix {
            [a] => u16::from(*a),
            [a, b] => u16::from_le_bytes([*a, *b]),
            _ => panic!("dense dimension prefix must be one or two bytes"),
        };
        let cap = (rows * 2 + 1).next_power_of_two().max(16);
        Self {
            direct: vec![0; (rows + 1).min(100_000)],
            prefix: prefix_word,
            prefix_len: prefix.len(),
            keys: vec![0; cap],
            values: vec![0; cap],
            long: HashMap::new(),
            shift: 64 - cap.trailing_zeros(),
        }
    }

    #[inline(always)]
    fn locate(&self, key: u64) -> usize {
        let h = (key ^ (key >> 23)).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let mut i = (h >> self.shift) as usize;
        loop {
            let seen = unsafe { *self.keys.get_unchecked(i) };
            if seen == key || seen == 0 {
                return i;
            }
            i = (i + 1) & (self.keys.len() - 1);
        }
    }

    pub fn insert(&mut self, id: &[u8], value: u32) {
        if id.len() > 7 {
            self.long.insert(id.to_vec(), value);
            return;
        }
        let mut input = [0; 16];
        input[..id.len()].copy_from_slice(id);
        if let Some(i) = unsafe { self.direct_index(input.as_ptr(), id.len()) } {
            self.direct[i] = u16::try_from(value + 1).expect("dimension index exceeds 16 bits");
        } else {
            input[7] = id.len() as u8;
            let key = unsafe { r8(input.as_ptr()) };
            let i = self.locate(key);
            self.keys[i] = key;
            self.values[i] = value + 1;
        }
    }

    /// Canonically spelled decimal identifiers use a dense index. A different
    /// prefix, extra leading zero or arbitrary byte string retains its exact key.
    #[inline(always)]
    unsafe fn direct_index(&self, p: *const u8, len: usize) -> Option<usize> {
        let n = len.wrapping_sub(self.prefix_len);
        if n.wrapping_sub(1) >= 5 {
            return None;
        }
        let prefix = p.cast::<u16>().read_unaligned();
        let prefix_mask = if self.prefix_len == 1 { 0xff } else { 0xffff };
        if prefix & prefix_mask != self.prefix || (n > 1 && *p.add(self.prefix_len) == b'0') {
            return None;
        }
        let digits = (r8(p.add(self.prefix_len)) ^ 0x3030_3030_3030_3030) << (64 - n * 8);
        let i = decimal8(digits) as usize;
        (i < self.direct.len()).then_some(i)
    }

    /// `p` points into a mapped input with readable slack.
    #[inline(always)]
    pub unsafe fn get(&self, p: *const u8, len: usize) -> u32 {
        if let Some(i) = self.direct_index(p, len) {
            return (self.direct[i] as u32).wrapping_sub(1);
        }
        if len <= 7 {
            let key = (r8(p) & *MASK.get_unchecked(len)) | ((len as u64) << 56);
            return self.values.get_unchecked(self.locate(key)).wrapping_sub(1);
        }
        self.long
            .get(std::slice::from_raw_parts(p, len))
            .copied()
            .unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_numbers_and_other_spellings_have_distinct_keys() {
        let mut table = SmallTab::new(500, b"p");
        let ids: &[&[u8]] = &[b"p1", b"p01", b"p0", b"p500", b"p99999", b"P1",
                             b"p1\0", b"p/1", b"p", b"p1234567", "p２".as_bytes()];
        for (i, id) in ids.iter().enumerate() {
            table.insert(id, i as u32);
        }
        for (i, id) in ids.iter().enumerate() {
            let mut padded = [0; 32];
            padded[..id.len()].copy_from_slice(id);
            assert_eq!(unsafe { table.get(padded.as_ptr(), id.len()) }, i as u32);
        }
        assert_eq!(unsafe { table.get(b"p10.............".as_ptr(), 3) }, u32::MAX);
    }
}
