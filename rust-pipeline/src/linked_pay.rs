//! Linked people have a small, exact slot within their partition. Pack that
//! slot and the event word into eight bytes while keeping zero for dedup.
use crate::sys::{Arena, Slab};
use crate::PRICE_BITS;
use std::arch::x86_64::{_pdep_u64, _pext_u64};

#[derive(Clone, Copy)]
pub struct LinkedPay {
    slot_bits: u32,
    slot_mask: u64,
    word_mask: u64,
}

impl LinkedPay {
    pub fn new(slot_bits: u32, window: u64, category_bits: u32) -> Self {
        // Preserve the timestamp, category, and as many low price bits as
        // fit. The top bit tags overflow and adding one reserves the tombstone.
        let available = 62 - slot_bits;
        let time_bits = (u64::BITS - window.saturating_sub(1).leading_zeros()).min(available);
        let low = (available - time_bits).min(PRICE_BITS + category_bits);
        let word_mask = ((1u64 << low) - 1)
            | (((1u64 << time_bits) - 1) << (PRICE_BITS + category_bits));
        Self { slot_bits, slot_mask: (1 << slot_bits) - 1, word_mask }
    }

    pub fn span(self) -> usize {
        self.slot_mask as usize + 1
    }

    #[inline(always)]
    pub fn index(self, encoded: u64) -> usize {
        ((encoded - 1) & self.slot_mask) as usize
    }

    #[inline(always)]
    pub unsafe fn encode(self, slot: u64, w: u64, slab: &mut Slab, arena: &Arena) -> u64 {
        if w & !self.word_mask == 0 {
            let bits = _pext_u64(w, self.word_mask);
            ((bits << self.slot_bits) | slot) + 1
        } else {
            let p = slab.alloc(8);
            (p as *mut u64).write(w);
            let offset = ((p - arena.base) / Arena::UNIT) as u64;
            (1 << 63) | (((offset << self.slot_bits) | slot) + 1)
        }
    }

    #[inline(always)]
    pub unsafe fn word(self, encoded: u64, arena: &Arena) -> u64 {
        let value = ((encoded & i64::MAX as u64) - 1) >> self.slot_bits;
        if encoded >> 63 == 0 {
            _pdep_u64(value, self.word_mask)
        } else {
            *((arena.base + value as usize * Arena::UNIT) as *const u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_first_last_slots_and_oversized_words() {
        let arena = Arena::new();
        let mut slab = Slab::new(&arena);
        let layout = LinkedPay::new(13, 14 * 86_400_000, 4);
        for slot in [0, 8191] {
            for word in [0, 1, layout.word_mask, 10_000_000 << 4, u64::MAX] {
                let encoded = unsafe { layout.encode(slot, word, &mut slab, &arena) };
                assert_ne!(encoded, 0);
                assert_eq!(layout.index(encoded), slot as usize);
                assert_eq!(unsafe { layout.word(encoded, &arena) }, word);
                assert_eq!(encoded >> 63 != 0, word & !layout.word_mask != 0);
            }
        }
    }
}
