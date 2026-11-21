//! Union-Find with component min-id (byte-wise).

use rustc_hash::FxHashMap;

#[derive(Default)]
pub struct UnionFind {
    parent: FxHashMap<u32, u32>,
    rank: FxHashMap<u32, u8>,
    min_id: FxHashMap<u32, u32>,
}

impl UnionFind {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, x: u32) {
        self.parent.entry(x).or_insert(x);
        self.rank.entry(x).or_insert(0);
        self.min_id.entry(x).or_insert(x);
    }

    pub fn find(&mut self, x: u32) -> u32 {
        let p = *self.parent.get(&x).unwrap_or(&x);
        if p != x {
            let r = self.find(p);
            self.parent.insert(x, r);
            return r;
        }
        x
    }

    pub fn union_with(&mut self, a: u32, b: u32, bytes: &[Box<[u8]>]) {
        self.add(a);
        self.add(b);
        let mut ra = self.find(a);
        let mut rb = self.find(b);
        if ra == rb {
            return;
        }
        let ma = self.min_id[&ra];
        let mb = self.min_id[&rb];
        let m = if bytes[ma as usize].as_ref() <= bytes[mb as usize].as_ref() {
            ma
        } else {
            mb
        };
        let ra_rank = self.rank[&ra];
        let rb_rank = self.rank[&rb];
        if ra_rank < rb_rank {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent.insert(rb, ra);
        self.min_id.insert(ra, m);
        if ra_rank == rb_rank {
            *self.rank.get_mut(&ra).unwrap() += 1;
        }
    }

    pub fn person(&mut self, x: u32) -> u32 {
        self.add(x);
        let r = self.find(x);
        self.min_id[&r]
    }
}
