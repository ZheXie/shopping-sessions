//! Product and campaign dictionaries. Short UTF-8 byte strings are their own
//! exact keys; longer ones are compared byte-for-byte in the overflow map.
use crate::hashx::r8;
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
    keys: Vec<u64>,
    values: Vec<u32>,
    long: HashMap<Vec<u8>, u32>,
    shift: u32,
}

impl SmallTab {
    pub fn new(rows: usize) -> Self {
        let cap = (rows * 2 + 1).next_power_of_two().max(16);
        Self {
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
        } else {
            let mut bytes = [0; 8];
            bytes[..id.len()].copy_from_slice(id);
            bytes[7] = id.len() as u8;
            let key = u64::from_le_bytes(bytes);
            let i = self.locate(key);
            self.keys[i] = key;
            self.values[i] = value + 1;
        }
    }

    /// `p` points into a mapped input with readable slack.
    #[inline(always)]
    pub unsafe fn get(&self, p: *const u8, len: usize) -> u32 {
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
