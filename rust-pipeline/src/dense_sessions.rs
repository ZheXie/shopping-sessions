//! Exact session reduction for people with many events in a bounded window.
use crate::{GAP, PRICE_BITS, PRICE_MASK, WEEK};
use crate::click::Click;

#[derive(Clone, Copy, Default)]
pub struct SessionSummary {
    pub start: u64,
    pub events: u32,
    pub purchase_end: u32,
}

#[derive(Clone, Copy, Default)]
struct TimeBin {
    first: u64,
    last: u64,
    events: u32,
    purchases: u32,
    next_session: u32,
}

/// A bin spans fewer than 30 minutes, so it cannot contain an internal
/// session break. Its first and last timestamps fully determine whether it
/// joins its neighbors, irrespective of input order.
#[derive(Default)]
pub struct DenseSessions {
    bins: Vec<TimeBin>,
    occupied: Vec<u64>,
    pub summaries: Vec<SessionSummary>,
    purchases: Vec<u64>,
    click_slots: Vec<u32>,
    touched_slots: Vec<usize>,
}

impl DenseSessions {
    const BIN_SHIFT: u32 = 20;

    pub fn group(&mut self, events: &mut [u64], window: u64, cat_bits: u32) {
        let shift = PRICE_BITS + cat_bits;
        let bins = (((window - 1) >> Self::BIN_SHIFT) + 1) as usize;
        if self.bins.len() < bins {
            self.bins.resize(bins, TimeBin::default());
            self.occupied.resize(bins.div_ceil(64), 0);
        }
        self.summaries.clear();
        self.purchases.clear();
        for &w in events.iter() {
            let time = w >> shift;
            let index = (time >> Self::BIN_SHIFT) as usize;
            let bin = &mut self.bins[index];
            if bin.events == 0 {
                self.occupied[index >> 6] |= 1 << (index & 63);
                bin.first = time;
                bin.last = time;
                bin.purchases = 0;
            } else {
                bin.first = bin.first.min(time);
                bin.last = bin.last.max(time);
            }
            bin.events += 1;
            if (w >> cat_bits) & PRICE_MASK != 0 {
                bin.purchases += 1;
                self.purchases.push(w);
            }
        }
        let mut end = 0;
        let mut last = None;
        for (word_index, occupied) in self.occupied.iter_mut().enumerate() {
            let mut bits = std::mem::take(occupied);
            while bits != 0 {
                let index = word_index * 64 + bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let bin = &mut self.bins[index];
                if last.is_none_or(|t| bin.first > t + GAP) {
                    self.summaries.push(SessionSummary {
                        start: bin.first,
                        ..Default::default()
                    });
                }
                let at = end;
                end += bin.purchases;
                bin.purchases = at;
                let summary = self.summaries.last_mut().unwrap();
                summary.events += bin.events;
                summary.purchase_end = end;
                bin.events = 0;
                last = Some(bin.last);
            }
        }
        // Only purchased events need individual processing from here on.
        for &w in &self.purchases {
            let time = w >> shift;
            let at = &mut self.bins[(time >> Self::BIN_SHIFT) as usize].purchases;
            events[*at as usize] = w;
            *at += 1;
        }
    }

    /// Between consecutive session starts, only the latest click for a given
    /// campaign can matter: they first become eligible together, and this one
    /// also expires last. Return those representatives in arbitrary order.
    pub unsafe fn prune_clicks(
        &mut self,
        clicks: &mut [Click],
        campaigns: usize,
        scratch: &mut Vec<Click>,
    ) -> usize {
        let needed = self.summaries.len().saturating_mul(campaigns);
        // Bound this optional dense index to 4 MiB per worker.
        if clicks.len() < 4096 || needed > (1 << 20) || needed > clicks.len().saturating_mul(32) {
            return clicks.len();
        }
        let mut next_session = 0;
        for (bin_index, bin) in self.bins.iter_mut().enumerate() {
            let lower = (bin_index as u64) << Self::BIN_SHIFT;
            while next_session < self.summaries.len()
                && self.summaries[next_session].start < lower
            {
                next_session += 1;
            }
            bin.next_session = next_session as u32;
        }
        if self.click_slots.len() < needed {
            self.click_slots.resize(needed, 0);
        }
        self.touched_slots.clear();
        for (i, &c) in clicks.iter().enumerate() {
            let t = c.time();
            let rel = t.saturating_sub(WEEK);
            let mut session = self.bins[(rel >> Self::BIN_SHIFT) as usize].next_session as usize;
            if session == self.summaries.len() {
                continue;
            }
            if self.summaries[session].start < rel {
                session += 1;
                if session == self.summaries.len() {
                    continue;
                }
            }
            if self.summaries[session].start > t {
                continue;
            }
            let at = session * campaigns + c.campaign();
            let slot = &mut self.click_slots[at];
            if *slot == 0 {
                self.touched_slots.push(at);
            } else {
                let old = clicks[(*slot - 1) as usize];
                let old_t = old.time();
                if t < old_t || (t == old_t && !c.precedes(old)) {
                    continue;
                }
            }
            *slot = i as u32 + 1;
        }
        scratch.clear();
        for &at in &self.touched_slots {
            let index = std::mem::take(&mut self.click_slots[at]) as usize - 1;
            scratch.push(clicks[index]);
        }
        let kept = scratch.len();
        clicks[..kept].copy_from_slice(scratch);
        kept
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bins_match_sorted_sessions_and_keep_each_purchase() {
        let mut events = Vec::new();
        let cat_bits = 4;
        let shift = PRICE_BITS + cat_bits;
        // Reverse-ordered duplicates, adjacent bins on a boundary, exactly
        // one session gap, and then a gap one millisecond longer.
        let times = [
            0, (1 << 20) - 1, 1 << 20, (1 << 20) + GAP,
            (1 << 20) + 2 * GAP + 1, WEEK, WEEK + GAP,
        ];
        for i in (0..8192).rev() {
            let t = times[(i * 17) % times.len()];
            let purchase = if i % 5 == 0 { (i as u64 + 1) << cat_bits | 7 } else { 0 };
            events.push(t << shift | purchase);
        }
        let mut sorted = events.clone();
        sorted.sort_unstable();
        let mut expected = Vec::<(u64, u32, Vec<u64>)>::new();
        let mut last = None;
        for word in sorted {
            let t = word >> shift;
            if last.is_none_or(|v| t > v + GAP) {
                expected.push((t, 0, Vec::new()));
            }
            let group = expected.last_mut().unwrap();
            group.1 += 1;
            if (word >> cat_bits) & PRICE_MASK != 0 {
                group.2.push(word);
            }
            last = Some(t);
        }
        let mut dense = DenseSessions::default();
        dense.group(&mut events, WEEK + GAP + 1, cat_bits);
        assert_eq!(dense.summaries.len(), expected.len());
        let mut from = 0;
        for (summary, (start, count, purchases)) in dense.summaries.iter().zip(expected) {
            assert_eq!((summary.start, summary.events), (start, count));
            let to = summary.purchase_end as usize;
            events[from..to].sort_unstable();
            assert_eq!(&events[from..to], purchases.as_slice());
            from = to;
        }
    }

    #[test]
    fn pruned_clicks_preserve_credit_and_every_assist() {
        let mut dense = DenseSessions::default();
        let starts = [0, GAP + 1, WEEK, WEEK + GAP + 1, 2 * WEEK];
        let mut events: Vec<_> = starts.iter().map(|t| t << (PRICE_BITS + 1)).collect();
        dense.group(&mut events, 2 * WEEK + 1, 1);
        let times = [0, WEEK - 1, WEEK, WEEK + 1, WEEK + GAP, 2 * WEEK, 3 * WEEK];
        let ids = [
            b"a0,.....", b"a1,.....", b"a2,.....",
            b"z0,.....", b"z1,.....", b"z2,.....",
        ];
        let mut clicks = Vec::new();
        for i in (0..4200).rev() {
            let time = times[(i / 7) % times.len()];
            let campaign = i % 3;
            let id = ids[campaign + (i % 2) * 3].as_ptr();
            unsafe {
                clicks.push(Click::new(time, campaign as u32, id, 2, || id));
            }
        }
        unsafe fn eligible(clicks: &[Click], start: u64) -> (Option<(u64, usize)>, u32) {
            let mut best: Option<Click> = None;
            let mut campaigns = 0;
            for &click in clicks {
                if click.time() >= start && click.time() <= start + WEEK {
                    campaigns |= 1 << click.campaign();
                    if best.is_none_or(|old| {
                        click.time() > old.time()
                            || (click.time() == old.time() && click.precedes(old))
                    }) {
                        best = Some(click);
                    }
                }
            }
            (best.map(|c| (c.time(), c.campaign())), campaigns)
        }
        let expected: Vec<_> = starts.iter().map(|&s| unsafe { eligible(&clicks, s) }).collect();
        let kept = unsafe { dense.prune_clicks(&mut clicks, 3, &mut Vec::new()) };
        assert!(kept < 32);
        let actual: Vec<_> = starts.iter().map(|&s| unsafe { eligible(&clicks[..kept], s) }).collect();
        assert_eq!(actual, expected);
    }
}
