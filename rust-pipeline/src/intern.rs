//! Byte-string interner → dense u32 ids.

use rustc_hash::FxHashMap;

#[derive(Default)]
pub struct Interner {
    map: FxHashMap<Box<[u8]>, u32>,
    rev: Vec<Box<[u8]>>,
}

impl Interner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: FxHashMap::with_capacity_and_hasher(cap, Default::default()),
            rev: Vec::with_capacity(cap),
        }
    }

    #[inline]
    pub fn intern(&mut self, s: &[u8]) -> u32 {
        if let Some(&id) = self.map.get(s) {
            return id;
        }
        let id = self.rev.len() as u32;
        let owned: Box<[u8]> = s.to_vec().into_boxed_slice();
        self.map.insert(owned.clone(), id);
        self.rev.push(owned);
        id
    }

    #[inline]
    pub fn get(&self, id: u32) -> &[u8] {
        &self.rev[id as usize]
    }

    pub fn len(&self) -> usize {
        self.rev.len()
    }
}
