//! A click's timestamp, campaign and tie-break ID fit in two words.
use crate::hashx::r8;
use crate::links::id_bytes;
use std::arch::x86_64::{_pdep_u64, _pext_u64};

const INDIRECT: u64 = 1 << 63;
const ASCII: u64 = 0x7f7f_7f7f_7f7f_7f7f;

#[derive(Clone, Copy, Default)]
pub struct Click {
    time_campaign: u64,
    id: u64,
}

impl Click {
    /// The packed event window leaves at least 24 spare timestamp bits, and
    /// the dimension loader checks that campaign indices fit in 16 bits.
    /// `p` has SIMD slack; `stable` gives a copy of the ID that lives until
    /// exit and is only called for IDs that do not pack.
    #[inline(always)]
    pub unsafe fn new(
        time: u64,
        campaign: u32,
        p: *const u8,
        len: usize,
        stable: impl FnOnce() -> *const u8,
    ) -> Self {
        debug_assert!(len != 0);
        let mut packed = None;
        if len <= 9 && *p.add(len - 1) != 0 {
            let bytes = r8(p);
            let head = if len < 8 {
                bytes & (u64::MAX >> (64 - len * 8))
            } else {
                bytes
            };
            let tail = if len == 9 { *p.add(8) } else { 0 };
            if (head & !ASCII) == 0 && tail < 128 {
                packed = Some((_pext_u64(head.swap_bytes(), ASCII) << 7) | tail as u64);
            }
        }
        let id = match packed {
            Some(id) => id,
            None => INDIRECT | stable() as u64,
        };
        Self {
            time_campaign: (time << 16) | u64::from(campaign),
            id,
        }
    }

    #[inline(always)]
    pub fn time(self) -> u64 {
        self.time_campaign >> 16
    }

    #[inline(always)]
    pub fn campaign(self) -> usize {
        self.time_campaign as u16 as usize
    }

    /// Big-endian 7-bit symbols preserve byte order. Padding zeros sort
    /// before any extension with a nonzero final byte. IDs ending in NUL,
    /// long IDs, and UTF-8 use full byte comparison.
    #[inline(always)]
    pub unsafe fn precedes(self, other: Self) -> bool {
        if (self.id | other.id) & INDIRECT == 0 {
            self.id < other.id
        } else {
            self.bytes(&mut [0; 9]) < other.bytes(&mut [0; 9])
        }
    }

    #[cold]
    unsafe fn bytes<'a>(self, scratch: &'a mut [u8; 9]) -> &'a [u8] {
        if self.id & INDIRECT != 0 {
            return id_bytes(self.id & !INDIRECT);
        }
        scratch[..8].copy_from_slice(&_pdep_u64(self.id >> 7, ASCII).to_be_bytes());
        scratch[8] = self.id as u8 & 0x7f;
        let len = scratch.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
        &scratch[..len]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_and_indirect_tie_breaks_follow_byte_order() {
        // Include interior and trailing zeroes, plus both sides of the nine-byte limit.
        let ids: &[&[u8]] = &[
            b"\0", b"\0x", b"a", b"a\0", b"a\0b", b"a ", b"A", b"k7654321",
            b"k7654321\0", b"k76543210", b"k765432100", b"k76543211", "同".as_bytes(),
        ];
        let csv: Vec<Vec<u8>> = ids.iter().map(|id| {
            let mut buf = id.to_vec();
            buf.push(b',');
            buf.resize(buf.len() + 64, 0);
            buf
        }).collect();
        for (i, id) in ids.iter().enumerate() {
            let t = (1 << 36) + i as u64;
            let a = unsafe { Click::new(t, 59_999, csv[i].as_ptr(), id.len(), || csv[i].as_ptr()) };
            assert_eq!((a.time(), a.campaign()), (t, 59_999));
            for (j, other) in ids.iter().enumerate() {
                let b = unsafe { Click::new(t, 0, csv[j].as_ptr(), other.len(), || csv[j].as_ptr()) };
                assert_eq!(unsafe { a.precedes(b) }, id < other, "{id:?} vs {other:?}");
            }
        }
    }
}
