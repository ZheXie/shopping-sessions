//! Shopping Sessions pipeline.
//!
//! 1. Links: lock-free union-find keyed by ID hash; a person is their
//!    byte-wise smallest ID.
//! 2. Parse (all cores, vectorised CSV indexing). Every event row is decided
//!    valid or not on the spot and scattered twice: a dedup key partitioned by
//!    event-id hash, and, when valid, a payload partitioned by person. Clicks
//!    are scattered by person.
//! 3. Dedup, one event-id partition at a time: of the copies of an event only
//!    the one with the smallest `ingest_ts` keeps its payload alive.
//! 4. One person partition at a time: sessionize, attribute, aggregate, and
//!    write Parquet.

mod hashx;
mod links;
mod pq;
mod scan;
mod sys;
mod ts;

use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

use hashx::{hash, hash2, r4, r8};
use links::{id_bytes, name_hash, same_name, Edge, Name, Table, ADDR, LONG};
use pq::{bytes_of, Col, Kind};
use scan::Scan;
use sys::{par_map, Arena, Blk, Buf, PartWriter, Slab};

const DAY: u64 = 86_400_000;
const WEEK: u64 = 7 * DAY;
const GAP: u64 = 1_800_000;
const MAX_PRICE: u64 = 10_000_000;
const PRICE_BITS: u32 = 24;
const PRICE_MASK: u64 = (1 << PRICE_BITS) - 1;
const DEAD: u64 = 1 << 63;
/// Bits of `Key::x` holding `ingest_ts`, in ms from 2^32 ms before the window.
const INGEST_BITS: u32 = 33;
const INGEST_MASK: u64 = (1 << INGEST_BITS) - 1;
const PART_SHIFT: u32 = 44;
const MAX_PARTS: usize = 4096;
const BATCH: usize = 32;
const NONE: u32 = u32::MAX;

/// Dedup record of one event row. `x` is `ingest_ts` under the arena offset
/// of the row's payload (0 when the row is invalid). Rows are partitioned by
/// bits of a second hash of the event id, so two ids are only ever taken for
/// one when they agree on `h` and on those bits.
#[repr(C)]
#[derive(Clone, Copy)]
struct Key {
    h: u64,
    x: u64,
}

/// A valid event row. `n1` gets `DEAD` once dedup finds a copy that outranks
/// this one. `w` packs `(ts - window_start, price_cents, category)` so that
/// sorting by it sorts by time.
#[repr(C)]
#[derive(Clone, Copy)]
struct Pay {
    n0: u64,
    n1: u64,
    w: u64,
}

/// A valid click. `cp` is the address of the click id with the campaign index
/// on top; `t` is `ts - (window_start - 7 days)`.
#[repr(C)]
#[derive(Clone, Copy)]
struct Clk {
    n0: u64,
    n1: u64,
    cp: u64,
    t: u64,
}

/// Hash set of short strings mapping to a 16-bit value. A slot keeps the top
/// 48 hash bits; the index comes from the low bits.
struct SmallTab {
    slots: Vec<u64>,
    mask: usize,
}

impl SmallTab {
    fn new(n: usize) -> Self {
        let cap = (n * 2 + 2).next_power_of_two().max(16);
        SmallTab { slots: vec![0; cap], mask: cap - 1 }
    }

    fn insert(&mut self, h: u64, val: u32) {
        let entry = (h & !0xFFFF) | (val as u64 + 1);
        let mut i = h as usize & self.mask;
        loop {
            let s = self.slots[i];
            if s == 0 {
                self.slots[i] = entry;
                return;
            }
            if (s ^ h) >> 16 == 0 {
                return;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    unsafe fn get(&self, h: u64) -> u32 {
        let mut i = h as usize & self.mask;
        loop {
            let s = *self.slots.get_unchecked(i);
            if s == 0 {
                return NONE;
            }
            if (s ^ h) >> 16 == 0 {
                return (s & 0xFFFF) as u32 - 1;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    unsafe fn prefetch(&self, h: u64) {
        _mm_prefetch(self.slots.as_ptr().add(h as usize & self.mask) as *const i8, _MM_HINT_T0);
    }
}

struct Ctx {
    arena: Arena,
    w0: i64,
    /// Length of the event window in ms.
    wlen: u64,
    links: Table,
    products: SmallTab,
    campaigns: SmallTab,
    unknown: u64,
    /// Bits of `Pay::w` holding the category; the timestamp sits above
    /// `cat_bits + PRICE_BITS`.
    cat_bits: u32,
    ev_mask: usize,
    person_mask: usize,
}

#[derive(Clone, Copy)]
struct EvStage {
    eh: u64,
    part: u64,
    ingest: u64,
    uh: u64,
    up: u64,
    ulen: u64,
    ph: u64,
    tp: u64,
    valid: bool,
}

#[derive(Clone, Copy)]
struct ClStage {
    uh: u64,
    up: u64,
    ulen: u64,
    cp: u64,
    t: u64,
}

struct Work<'a> {
    slab: Slab<'a>,
    keys: PartWriter<Key>,
    pays: PartWriter<Pay>,
    clks: PartWriter<Clk>,
    ev: [EvStage; BATCH],
    nev: usize,
    cl: [ClStage; BATCH],
    ncl: usize,
}

fn env_path(key: &str, default: &str) -> PathBuf {
    PathBuf::from(std::env::var(key).unwrap_or_else(|_| default.to_string()))
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok().and_then(|s| s.parse().ok())
}

fn list_csvs(dir: &Path) -> Vec<Buf> {
    let Ok(rd) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = rd
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false))
        .collect();
    paths.sort();
    paths.iter().map(|p| sys::map_file(p)).filter(|b| b.len > 0).collect()
}

fn load_manifest(data: &Path) -> (i64, i64) {
    let raw = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let field = |name: &str| -> i64 {
        let value = raw
            .split(&format!("\"{name}\""))
            .nth(1)
            .and_then(|s| s.split('"').nth(1))
            .unwrap_or_else(|| panic!("manifest.json has no {name}"));
        let t = ts::parse_slice(value.as_bytes());
        assert!(t != ts::BAD, "manifest.json: bad {name}");
        t
    };
    (field("window_start"), field("window_end"))
}

/// Calls `f(base, fields)` for every well-formed `N`-field data line of `buf`
/// that starts in `[a, b)`. Field offsets are relative to `base`.
#[inline(always)]
unsafe fn for_rows<const N: usize, F: FnMut(*const u8, &[(u32, u32); N])>(
    sc: &mut Scan,
    buf: Buf,
    a: usize,
    b: usize,
    sub: usize,
    mut f: F,
) {
    let mut pos = scan::line_start(buf.ptr, buf.len, a);
    while pos < b {
        let next = sc.scan(buf.ptr, buf.len, pos, (pos + sub).min(b));
        let base = buf.ptr.add(pos);
        scan::rows::<N, _>(sc, base, b - pos, |fields| f(base, fields));
        pos = next;
    }
}

#[inline(always)]
unsafe fn field<'a>(base: *const u8, f: (u32, u32)) -> &'a [u8] {
    std::slice::from_raw_parts(base.add(f.0 as usize), (f.1 - f.0) as usize)
}

/// A purchase price in cents, or 0 when it is not digits from 1 to 10,000,000.
#[inline(always)]
unsafe fn parse_price(p: *const u8, len: usize) -> u64 {
    let mut i = 0;
    while i < len && *p.add(i) == b'0' {
        i += 1;
    }
    if len - i > 8 {
        return 0;
    }
    let mut v = 0u64;
    while i < len {
        let d = (*p.add(i)).wrapping_sub(b'0');
        if d > 9 {
            return 0;
        }
        v = v * 10 + d as u64;
        i += 1;
    }
    if v > MAX_PRICE {
        0
    } else {
        v
    }
}

/// Split point list for `[0, len)` in pieces of about `chunk` bytes.
fn chunks(len: usize, chunk: usize) -> Vec<(usize, usize)> {
    let n = len.div_ceil(chunk).max(1);
    (0..n).map(|i| (len * i / n, len * (i + 1) / n)).collect()
}

fn parts_for(rows: usize, per_part: usize) -> usize {
    (rows / per_part).max(1).next_power_of_two().min(MAX_PARTS)
}

struct Dims {
    /// Category names in byte order, so a smaller index is a smaller name.
    cat_names: Vec<Vec<u8>>,
    camp_names: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
}

/// Products (id -> category index), campaigns (id -> index), the index of
/// the `unknown` category, and the names behind the indices.
fn load_dims(data: &Path) -> (SmallTab, SmallTab, u64, Dims) {
    let mut sc = Scan::new();
    let mut raw_products: Vec<(u64, u32)> = Vec::new();
    let mut cat_ids: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut cat_list: Vec<Vec<u8>> = Vec::new();
    let mut intern = |name: &[u8], ids: &mut HashMap<Vec<u8>, u32>| -> u32 {
        if let Some(&i) = ids.get(name) {
            return i;
        }
        let i = cat_list.len() as u32;
        cat_list.push(name.to_vec());
        ids.insert(name.to_vec(), i);
        i
    };
    let unknown_tmp = intern(b"unknown", &mut cat_ids);
    let buf = sys::map_file(&data.join("products.csv"));
    unsafe {
        for_rows::<2, _>(&mut sc, buf, 0, buf.len, 1 << 16, |base, f| {
            let id = field(base, f[0]);
            if id.is_empty() {
                return;
            }
            let cat = field(base, f[1]);
            let c = if cat.is_empty() { unknown_tmp } else { intern(cat, &mut cat_ids) };
            raw_products.push((hash(id.as_ptr(), id.len()), c));
        });
    }
    assert!(cat_list.len() < 60_000, "too many categories");
    let mut order: Vec<u32> = (0..cat_list.len() as u32).collect();
    order.sort_by(|&a, &b| cat_list[a as usize].cmp(&cat_list[b as usize]));
    let mut rank = vec![0u32; cat_list.len()];
    for (r, &i) in order.iter().enumerate() {
        rank[i as usize] = r as u32;
    }
    let cat_names: Vec<Vec<u8>> = order.iter().map(|&i| cat_list[i as usize].clone()).collect();
    let mut products = SmallTab::new(raw_products.len());
    for &(h, c) in &raw_products {
        products.insert(h, rank[c as usize]);
    }

    let mut camp_names: Vec<Vec<u8>> = Vec::new();
    let mut channels: Vec<Vec<u8>> = Vec::new();
    let mut hashes: Vec<u64> = Vec::new();
    let buf = sys::map_file(&data.join("campaigns.csv"));
    unsafe {
        for_rows::<2, _>(&mut sc, buf, 0, buf.len, 1 << 16, |base, f| {
            let id = field(base, f[0]);
            if id.is_empty() {
                return;
            }
            hashes.push(hash(id.as_ptr(), id.len()));
            camp_names.push(id.to_vec());
            channels.push(field(base, f[1]).to_vec());
        });
    }
    assert!(camp_names.len() < 60_000, "too many campaigns");
    let mut campaigns = SmallTab::new(hashes.len());
    for (i, &h) in hashes.iter().enumerate() {
        campaigns.insert(h, i as u32);
    }
    let unknown = rank[unknown_tmp as usize] as u64;
    (products, campaigns, unknown, Dims { cat_names, camp_names, channels })
}

fn load_links(files: &[Buf], threads: usize) -> Table {
    let mut tasks: Vec<(Buf, usize, usize)> = Vec::new();
    for &buf in files {
        for (a, b) in chunks(buf.len, 1 << 20) {
            tasks.push((buf, a, b));
        }
    }
    let next = AtomicUsize::new(0);
    let lists = par_map(threads.min(tasks.len()).max(1), |_| {
        let mut sc = Scan::new();
        let mut edges: Vec<Edge> = Vec::new();
        loop {
            let Some(&(buf, a, b)) = tasks.get(next.fetch_add(1, Relaxed)) else {
                break;
            };
            unsafe {
                for_rows::<2, _>(&mut sc, buf, a, b, 1 << 16, |base, f| {
                    let (la, lb) = ((f[0].1 - f[0].0) as usize, (f[1].1 - f[1].0) as usize);
                    if la == 0 || lb == 0 {
                        return;
                    }
                    let pa = base.add(f[0].0 as usize);
                    let pb = base.add(f[1].0 as usize);
                    edges.push(Edge {
                        ha: hash(pa, la),
                        pa: pa as u64,
                        xa: links::prefix(pa, la),
                        hb: hash(pb, lb),
                        pb: pb as u64,
                        xb: links::prefix(pb, lb),
                    });
                });
            }
        }
        edges
    });
    Table::build(&lists, threads)
}

const VIEW: u32 = u32::from_le_bytes(*b"view");
const CART: u32 = u32::from_le_bytes(*b"cart");
const PURCHASE: u64 = u64::from_le_bytes(*b"purchase");

#[inline(always)]
unsafe fn event_row(ctx: &Ctx, w: &mut Work, base: *const u8, f: &[(u32, u32); 7]) {
    let (b0, e0) = f[0];
    if b0 == e0 {
        return;
    }
    let ingest = ts::parse(base.add(f[3].0 as usize), (f[3].1 - f[3].0) as usize);
    if ingest == ts::BAD {
        return;
    }
    let (eh, eh2) = hash2(base.add(b0 as usize), (e0 - b0) as usize);
    let t = ts::parse(base.add(f[2].0 as usize), (f[2].1 - f[2].0) as usize);
    let rel = t.wrapping_sub(ctx.w0) as u64;
    let user_len = (f[1].1 - f[1].0) as usize;
    let mut valid = (user_len != 0)
        & (f[5].1 != f[5].0)
        & (rel < ctx.wlen)
        & (ingest.wrapping_sub(t) <= DAY as i64);
    let ty = base.add(f[4].0 as usize);
    let ty_len = f[4].1 - f[4].0;
    let mut price = 0u64;
    if ty_len == 4 {
        let v = r4(ty);
        valid &= (v == VIEW) | (v == CART);
    } else if ty_len == 8 && r8(ty) == PURCHASE {
        price = parse_price(base.add(f[6].0 as usize), (f[6].1 - f[6].0) as usize);
        valid &= price != 0;
    } else {
        valid = false;
    }
    let s = w.ev.get_unchecked_mut(w.nev);
    s.eh = eh;
    s.part = eh2;
    s.ingest = (ingest - ctx.w0 + (1 << (INGEST_BITS - 1))).clamp(0, INGEST_MASK as i64) as u64;
    s.valid = valid;
    if valid {
        let up = base.add(f[1].0 as usize);
        let pp = base.add(f[5].0 as usize);
        s.uh = hash(up, user_len);
        s.up = up as u64;
        s.ulen = user_len as u64;
        s.ph = hash(pp, (f[5].1 - f[5].0) as usize);
        s.tp = (rel << (PRICE_BITS + ctx.cat_bits)) | (price << ctx.cat_bits);
        ctx.links.prefetch(s.uh);
        ctx.products.prefetch(s.ph);
    }
    w.nev += 1;
    if w.nev == BATCH {
        flush_events(ctx, w);
    }
}

#[inline(never)]
unsafe fn flush_events(ctx: &Ctx, w: &mut Work) {
    for i in 0..w.nev {
        let s = *w.ev.get_unchecked(i);
        let mut pay = 0u64;
        if s.valid {
            let (pk, name) = ctx.links.lookup(s.uh, s.up as *const u8, s.ulen as usize);
            let c = ctx.products.get(s.ph);
            let cat = if c == NONE { ctx.unknown } else { c as u64 };
            let part = (pk >> PART_SHIFT) as usize & ctx.person_mask;
            let at = w.pays.push(part, &mut w.slab, Pay { n0: name.0, n1: name.1, w: s.tp | cat });
            pay = ((at - ctx.arena.base) / Arena::UNIT) as u64;
        }
        let part = (s.part >> PART_SHIFT) as usize & ctx.ev_mask;
        w.keys.push(part, &mut w.slab, Key { h: s.eh, x: (pay << INGEST_BITS) | s.ingest });
    }
    w.nev = 0;
}

#[inline(always)]
unsafe fn click_row(ctx: &Ctx, w: &mut Work, base: *const u8, f: &[(u32, u32); 4]) {
    let user_len = (f[1].1 - f[1].0) as usize;
    if f[0].0 == f[0].1 || user_len == 0 {
        return;
    }
    let t = ts::parse(base.add(f[3].0 as usize), (f[3].1 - f[3].0) as usize);
    let rel = t.wrapping_sub(ctx.w0 - WEEK as i64) as u64;
    if rel >= ctx.wlen + WEEK {
        return;
    }
    let cp = base.add(f[2].0 as usize);
    let camp = ctx.campaigns.get(hash(cp, (f[2].1 - f[2].0) as usize));
    if camp == NONE {
        return;
    }
    let up = base.add(f[1].0 as usize);
    let s = w.cl.get_unchecked_mut(w.ncl);
    s.uh = hash(up, user_len);
    s.up = up as u64;
    s.ulen = user_len as u64;
    s.cp = (base.add(f[0].0 as usize) as u64) | ((camp as u64) << 48);
    s.t = rel;
    ctx.links.prefetch(s.uh);
    w.ncl += 1;
    if w.ncl == BATCH {
        flush_clicks(ctx, w);
    }
}

#[inline(never)]
unsafe fn flush_clicks(ctx: &Ctx, w: &mut Work) {
    for i in 0..w.ncl {
        let s = *w.cl.get_unchecked(i);
        let (pk, name) = ctx.links.lookup(s.uh, s.up as *const u8, s.ulen as usize);
        let part = (pk >> PART_SHIFT) as usize & ctx.person_mask;
        w.clks.push(part, &mut w.slab, Clk { n0: name.0, n1: name.1, cp: s.cp, t: s.t });
    }
    w.ncl = 0;
}

/// Marks every copy of an event but the one with the smallest `ingest_ts`.
unsafe fn dedup_partition(arena: &Arena, blocks: &[Blk], table: &mut Vec<u64>) {
    let n: usize = blocks.iter().map(|b| b.n as usize).sum();
    let cap = (n * 2).next_power_of_two().max(64);
    if table.len() < cap {
        table.resize(cap, 0);
    }
    let tab = &mut table[..cap];
    tab.fill(0);
    let mask = cap - 1;
    let tab = tab.as_mut_ptr();
    let kill = |x: u64| {
        let at = (x >> INGEST_BITS) as usize;
        if at != 0 {
            (*((arena.base + at * Arena::UNIT) as *mut Pay)).n1 |= DEAD;
        }
    };
    for blk in blocks {
        let base = blk.ptr as *const Key;
        for i in 0..blk.n as usize {
            let k = base.add(i);
            let h = (*k).h;
            let entry = ((k as u64) << 16) | (h & 0xFFFF);
            let mut slot = (h >> 16) as usize & mask;
            loop {
                let e = *tab.add(slot);
                if e == 0 {
                    *tab.add(slot) = entry;
                    break;
                }
                if (e ^ h) & 0xFFFF == 0 {
                    let o = (e >> 16) as *const Key;
                    if (*o).h == h {
                        if (*k).x & INGEST_MASK < (*o).x & INGEST_MASK {
                            kill((*o).x);
                            *tab.add(slot) = entry;
                        } else {
                            kill((*k).x);
                        }
                        break;
                    }
                }
                slot = (slot + 1) & mask;
            }
        }
    }
}

/// One thread's share of both output tables.
struct Out {
    names: Vec<u8>,
    day: Vec<i32>,
    sessions: Vec<i32>,
    events: Vec<i32>,
    revenue: Vec<i64>,
    top_def: Vec<u8>,
    top: Vec<u8>,
    rows: usize,
    /// `[sessions, purchasing_sessions, revenue_cents, assisted_sessions]`
    /// for each `(campaign, day)`; the last campaign is `organic`.
    camp: Vec<[i64; 4]>,
}

struct Agg<'a> {
    ctx: &'a Ctx,
    cat_names: &'a [Vec<u8>],
    ndays: usize,
    day0: i32,
    /// `window_start` as ms into its UTC day.
    day_off: u64,
    organic: usize,
    out: Out,
    slots: Vec<u32>,
    names: Vec<Name>,
    ev_end: Vec<u32>,
    ck_end: Vec<u32>,
    ev_idx: Vec<u32>,
    ev_w: Vec<u64>,
    ck_idx: Vec<u32>,
    ck_raw: Vec<(u64, u64)>,
    evs: Vec<u64>,
    cks: Vec<(u64, u64)>,
    cnt: Vec<u32>,
    act: Vec<u32>,
    days: Vec<(u32, u32)>,
    cat_sum: Vec<u64>,
    touched: Vec<u32>,
}

impl<'a> Agg<'a> {
    fn new(ctx: &'a Ctx, dims: &'a Dims, w0: i64, w1: i64) -> Self {
        let day0 = w0.div_euclid(DAY as i64);
        let ndays = ((w1 - 1).div_euclid(DAY as i64) - day0 + 1).max(1) as usize;
        let ncamp = dims.camp_names.len();
        Agg {
            ctx,
            cat_names: &dims.cat_names,
            ndays,
            day0: day0 as i32,
            day_off: (w0 - day0 * DAY as i64) as u64,
            organic: ncamp,
            out: Out {
                names: Vec::new(),
                day: Vec::new(),
                sessions: Vec::new(),
                events: Vec::new(),
                revenue: Vec::new(),
                top_def: Vec::new(),
                top: Vec::new(),
                rows: 0,
                camp: vec![[0; 4]; (ncamp + 1) * ndays],
            },
            slots: Vec::new(),
            names: Vec::new(),
            ev_end: Vec::new(),
            ck_end: Vec::new(),
            ev_idx: Vec::new(),
            ev_w: Vec::new(),
            ck_idx: Vec::new(),
            ck_raw: Vec::new(),
            evs: Vec::new(),
            cks: Vec::new(),
            cnt: vec![0; ncamp + 1],
            act: vec![0; ncamp + 1],
            days: Vec::new(),
            cat_sum: vec![0; dims.cat_names.len()],
            touched: Vec::new(),
        }
    }

    /// Groups the partition's live events and its clicks by person, each
    /// person's rows sorted by time, then aggregates person by person.
    unsafe fn partition(&mut self, pays: &[Blk], clks: &[Blk]) {
        let n: usize = pays.iter().map(|b| b.n as usize).sum();
        if n == 0 {
            return;
        }
        let cap = (n * 2).next_power_of_two().max(64);
        if self.slots.len() < cap {
            self.slots.resize(cap, 0);
        }
        self.slots[..cap].fill(0);
        let mask = cap - 1;
        let slots = self.slots.as_mut_ptr();
        self.names.clear();
        self.ev_end.clear();
        self.ev_idx.clear();
        self.ev_w.clear();
        for blk in pays {
            let base = blk.ptr as *const Pay;
            for i in 0..blk.n as usize {
                let p = *base.add(i);
                if p.n1 & DEAD != 0 {
                    continue;
                }
                let name = (p.n0, p.n1);
                let mut slot = (name_hash(name) >> 32) as usize & mask;
                let idx = loop {
                    let s = *slots.add(slot);
                    if s == 0 {
                        let idx = self.names.len() as u32;
                        *slots.add(slot) = idx + 1;
                        self.names.push(name);
                        self.ev_end.push(0);
                        break idx;
                    }
                    if same_name(*self.names.get_unchecked(s as usize - 1), name) {
                        break s - 1;
                    }
                    slot = (slot + 1) & mask;
                };
                *self.ev_end.get_unchecked_mut(idx as usize) += 1;
                self.ev_idx.push(idx);
                self.ev_w.push(p.w);
            }
        }
        let people = self.names.len();
        if people == 0 {
            return;
        }
        self.ck_end.clear();
        self.ck_end.resize(people, 0);
        self.ck_idx.clear();
        self.ck_raw.clear();
        for blk in clks {
            let base = blk.ptr as *const Clk;
            for i in 0..blk.n as usize {
                let c = *base.add(i);
                let name = (c.n0, c.n1);
                let mut slot = (name_hash(name) >> 32) as usize & mask;
                loop {
                    let s = *slots.add(slot);
                    if s == 0 {
                        break;
                    }
                    if same_name(*self.names.get_unchecked(s as usize - 1), name) {
                        *self.ck_end.get_unchecked_mut(s as usize - 1) += 1;
                        self.ck_idx.push(s - 1);
                        self.ck_raw.push((c.t, c.cp));
                        break;
                    }
                    slot = (slot + 1) & mask;
                }
            }
        }
        // Counts become start offsets, and the scatter turns them into ends.
        let mut sum = 0u32;
        for e in self.ev_end.iter_mut() {
            let c = *e;
            *e = sum;
            sum += c;
        }
        let mut sum = 0u32;
        for e in self.ck_end.iter_mut() {
            let c = *e;
            *e = sum;
            sum += c;
        }
        let nev = self.ev_w.len();
        if self.evs.len() < nev {
            self.evs.resize(nev, 0);
        }
        for i in 0..nev {
            let at = self.ev_end.get_unchecked_mut(*self.ev_idx.get_unchecked(i) as usize);
            *self.evs.get_unchecked_mut(*at as usize) = *self.ev_w.get_unchecked(i);
            *at += 1;
        }
        let nck = self.ck_raw.len();
        if self.cks.len() < nck {
            self.cks.resize(nck, (0, 0));
        }
        for i in 0..nck {
            let at = self.ck_end.get_unchecked_mut(*self.ck_idx.get_unchecked(i) as usize);
            *self.cks.get_unchecked_mut(*at as usize) = *self.ck_raw.get_unchecked(i);
            *at += 1;
        }
        let (mut ev_at, mut ck_at) = (0usize, 0usize);
        for p in 0..people {
            let ev_to = *self.ev_end.get_unchecked(p) as usize;
            let ck_to = *self.ck_end.get_unchecked(p) as usize;
            self.evs[ev_at..ev_to].sort_unstable();
            if ck_to - ck_at > 1 {
                self.cks[ck_at..ck_to].sort_unstable_by_key(|c| c.0);
            }
            self.person(*self.names.get_unchecked(p), ev_at, ev_to, ck_at, ck_to);
            ev_at = ev_to;
            ck_at = ck_to;
        }
    }

    /// Counts sessions `[from, to)` of the current person as assisted by `camp`.
    #[inline(always)]
    unsafe fn assist(&mut self, camp: usize, from: u32, to: u32) {
        let mut end = to;
        for i in (0..self.days.len()).rev() {
            let (day, begin) = *self.days.get_unchecked(i);
            let start = from.max(begin);
            if end > start {
                self.out.camp.get_unchecked_mut(camp * self.ndays + day as usize)[3] += (end - start) as i64;
            }
            if begin <= from {
                break;
            }
            end = begin;
        }
    }

    #[inline(always)]
    unsafe fn user_row(&mut self, name: Name, day: u32, sessions: u32, events: u32, revenue: u64) {
        let o = &mut self.out;
        if name.1 & LONG == 0 {
            // Length, then both words: the bytes past the length are dropped.
            let len = (name.1 >> 56) as usize;
            o.names.reserve(20);
            let at = o.names.as_mut_ptr().add(o.names.len());
            (at as *mut u32).write_unaligned(len as u32);
            (at.add(4) as *mut u64).write_unaligned(name.0);
            (at.add(12) as *mut u64).write_unaligned(name.1);
            o.names.set_len(o.names.len() + 4 + len);
        } else {
            let bytes = id_bytes(name.1 & ADDR);
            o.names.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            o.names.extend_from_slice(bytes);
        }
        o.day.push(self.day0 + day as i32);
        o.sessions.push(sessions as i32);
        o.events.push(events as i32);
        o.revenue.push(revenue as i64);
        if o.rows & 7 == 0 {
            o.top_def.push(0);
        }
        if !self.touched.is_empty() {
            let (mut best, mut best_sum) = (u32::MAX, 0u64);
            for &c in &self.touched {
                let s = std::mem::take(self.cat_sum.get_unchecked_mut(c as usize));
                if s > best_sum || (s == best_sum && c < best) {
                    best = c;
                    best_sum = s;
                }
            }
            self.touched.clear();
            let cat = self.cat_names.get_unchecked(best as usize);
            o.top.extend_from_slice(&(cat.len() as u32).to_le_bytes());
            o.top.extend_from_slice(cat);
            *o.top_def.last_mut().unwrap_unchecked() |= 1 << (o.rows & 7);
        }
        o.rows += 1;
    }

    unsafe fn person(&mut self, name: Name, ev_at: usize, ev_to: usize, ck_at: usize, ck_to: usize) {
        let cat_bits = self.ctx.cat_bits;
        let shift = PRICE_BITS + cat_bits;
        let cat_mask = (1u64 << cat_bits) - 1;
        let ev = self.evs.as_ptr();
        let ck = self.cks.as_ptr();
        let (mut lo, mut hi) = (ck_at, ck_at);
        self.days.clear();
        let mut session = 0u32;
        let mut cur_day = u32::MAX;
        let (mut row_sessions, mut row_events, mut row_revenue) = (0u32, 0u32, 0u64);
        let mut k = ev_at;
        while k < ev_to {
            let mut w = *ev.add(k);
            let start = w >> shift;
            let day = ((start + self.day_off) / DAY) as u32;
            if day != cur_day {
                if cur_day != u32::MAX {
                    self.user_row(name, cur_day, row_sessions, row_events, row_revenue);
                    row_sessions = 0;
                    row_events = 0;
                    row_revenue = 0;
                }
                cur_day = day;
                self.days.push((day, session));
            }
            let mut prev = start;
            let mut events = 0u32;
            let mut revenue = 0u64;
            loop {
                let price = (w >> cat_bits) & PRICE_MASK;
                events += 1;
                if price != 0 {
                    revenue += price;
                    let c = (w & cat_mask) as usize;
                    let sum = self.cat_sum.get_unchecked_mut(c);
                    if *sum == 0 {
                        self.touched.push(c as u32);
                    }
                    *sum += price;
                }
                k += 1;
                if k == ev_to {
                    break;
                }
                w = *ev.add(k);
                let t = w >> shift;
                if t - prev > GAP {
                    break;
                }
                prev = t;
            }

            // Click times are offset by 7 days, so the clicks that count for
            // this session are those with `start <= t <= start + WEEK`.
            let latest = start + WEEK;
            while hi < ck_to && (*ck.add(hi)).0 <= latest {
                let c = ((*ck.add(hi)).1 >> 48) as usize;
                let n = self.cnt.get_unchecked_mut(c);
                if *n == 0 {
                    *self.act.get_unchecked_mut(c) = session;
                }
                *n += 1;
                hi += 1;
            }
            while lo < hi && (*ck.add(lo)).0 < start {
                let c = ((*ck.add(lo)).1 >> 48) as usize;
                let n = self.cnt.get_unchecked_mut(c);
                *n -= 1;
                if *n == 0 {
                    let from = *self.act.get_unchecked(c);
                    self.assist(c, from, session);
                }
                lo += 1;
            }
            let camp = if hi > lo {
                let mut best = hi - 1;
                let t = (*ck.add(best)).0;
                let mut j = best;
                while j > lo && (*ck.add(j - 1)).0 == t {
                    j -= 1;
                    if id_bytes((*ck.add(j)).1 & ADDR) < id_bytes((*ck.add(best)).1 & ADDR) {
                        best = j;
                    }
                }
                ((*ck.add(best)).1 >> 48) as usize
            } else {
                self.organic
            };
            let acc = self.out.camp.get_unchecked_mut(camp * self.ndays + day as usize);
            acc[0] += 1;
            acc[1] += (revenue != 0) as i64;
            acc[2] += revenue as i64;
            row_sessions += 1;
            row_events += events;
            row_revenue += revenue;
            session += 1;
        }
        self.user_row(name, cur_day, row_sessions, row_events, row_revenue);
        for j in lo..hi {
            let c = ((*ck.add(j)).1 >> 48) as usize;
            if *self.cnt.get_unchecked(c) != 0 {
                *self.cnt.get_unchecked_mut(c) = 0;
                let from = *self.act.get_unchecked(c);
                self.assist(c, from, session);
            }
        }
    }
}

fn write_user_file(dir: &Path, index: usize, o: &Out) {
    let cols = [
        Col { name: "user_id", kind: Kind::Str, parts: vec![&o.names] },
        Col { name: "day", kind: Kind::Date, parts: vec![bytes_of(&o.day)] },
        Col { name: "sessions", kind: Kind::I32, parts: vec![bytes_of(&o.sessions)] },
        Col { name: "events", kind: Kind::I32, parts: vec![bytes_of(&o.events)] },
        Col { name: "revenue_cents", kind: Kind::I64, parts: vec![bytes_of(&o.revenue)] },
        Col { name: "top_category", kind: Kind::OptStr, parts: vec![&o.top_def, &o.top] },
    ];
    pq::write_file(&dir.join(format!("part-{index}.parquet")), o.rows, &cols);
}

fn write_campaign_file(dir: &Path, dims: &Dims, ndays: usize, day0: i32, acc: &[[i64; 4]]) -> usize {
    let (mut ids, mut channels) = (Vec::new(), Vec::new());
    let mut day: Vec<i32> = Vec::new();
    let mut cols: [Vec<i64>; 4] = Default::default();
    let ncamp = dims.camp_names.len();
    for c in 0..=ncamp {
        let (id, channel): (&[u8], &[u8]) = if c == ncamp {
            (b"organic", b"organic")
        } else {
            (&dims.camp_names[c], &dims.channels[c])
        };
        for d in 0..ndays {
            let a = acc[c * ndays + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            ids.extend_from_slice(&(id.len() as u32).to_le_bytes());
            ids.extend_from_slice(id);
            channels.extend_from_slice(&(channel.len() as u32).to_le_bytes());
            channels.extend_from_slice(channel);
            day.push(day0 + d as i32);
            for (col, v) in cols.iter_mut().zip(a) {
                col.push(v);
            }
        }
    }
    let rows = day.len();
    let table = [
        Col { name: "campaign_id", kind: Kind::Str, parts: vec![&ids] },
        Col { name: "channel", kind: Kind::Str, parts: vec![&channels] },
        Col { name: "day", kind: Kind::Date, parts: vec![bytes_of(&day)] },
        Col { name: "sessions", kind: Kind::I64, parts: vec![bytes_of(&cols[0])] },
        Col { name: "purchasing_sessions", kind: Kind::I64, parts: vec![bytes_of(&cols[1])] },
        Col { name: "revenue_cents", kind: Kind::I64, parts: vec![bytes_of(&cols[2])] },
        Col { name: "assisted_sessions", kind: Kind::I64, parts: vec![bytes_of(&cols[3])] },
    ];
    pq::write_file(&dir.join("part-0.parquet"), rows, &table);
    rows
}

fn merge_blocks(per_thread: Vec<Vec<Vec<Blk>>>, parts: usize) -> Vec<Vec<Blk>> {
    let mut out: Vec<Vec<Blk>> = (0..parts).map(|_| Vec::new()).collect();
    for lists in per_thread {
        for (part, blocks) in lists.into_iter().enumerate() {
            out[part].extend(blocks);
        }
    }
    out
}

fn main() {
    let started = Instant::now();
    let timing = std::env::var("SS_TIMING").ok().as_deref() == Some("1");
    let mut last = started;
    let mut lap = |what: &str| {
        if timing {
            let now = Instant::now();
            eprintln!("{what:>10} {:>8.1} ms", (now - last).as_secs_f64() * 1e3);
            last = now;
        }
    };

    let threads = env_usize("NCPU")
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
        .min(256);
    let data = env_path("DATA_DIR", "/data");
    let out_dir = env_path("OUTPUT_DIR", "/output");
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&user_dir).expect("create user_daily");
    fs::create_dir_all(&camp_dir).expect("create campaign_daily");

    let (w0, w1) = load_manifest(&data);
    let (products, campaigns, unknown, dims) = load_dims(&data);
    let event_files = list_csvs(&data.join("events"));
    let click_files = list_csvs(&data.join("clicks"));
    let link_files = list_csvs(&data.join("links"));
    lap("setup");

    let links = load_links(&link_files, threads);
    lap("links");

    let cat_bits = (usize::BITS - dims.cat_names.len().leading_zeros()).max(1);
    let wlen = (w1 - w0).max(0) as u64;
    assert!(
        (64 - wlen.leading_zeros()) + PRICE_BITS + cat_bits <= 64,
        "window too long to pack with {} categories",
        dims.cat_names.len()
    );
    let event_bytes: usize = event_files.iter().map(|b| b.len).sum();
    let click_bytes: usize = click_files.iter().map(|b| b.len).sum();
    let per_part = env_usize("SS_PART_ROWS").unwrap_or(24_000).max(1);
    let ev_parts = parts_for(event_bytes / 80, per_part);
    let person_parts = parts_for(event_bytes / 80 + click_bytes / 50, per_part);
    let ctx = Ctx {
        arena: Arena::new(),
        w0,
        wlen,
        links,
        products,
        campaigns,
        unknown,
        cat_bits,
        ev_mask: ev_parts - 1,
        person_mask: person_parts - 1,
    };

    // Parse and scatter.
    let chunk = env_usize("SS_CHUNK").unwrap_or(2 << 20).max(4096);
    let sub = env_usize("SS_SUB").unwrap_or(96 << 10).max(64);
    let mut tasks: Vec<(Buf, usize, usize, bool)> = Vec::new();
    for &buf in &event_files {
        for (a, b) in chunks(buf.len, chunk) {
            tasks.push((buf, a, b, true));
        }
    }
    for &buf in &click_files {
        for (a, b) in chunks(buf.len, chunk) {
            tasks.push((buf, a, b, false));
        }
    }
    let mode = env_usize("SS_MODE").unwrap_or(0);
    let populate = env_usize("SS_POPULATE").unwrap_or(1) != 0;
    let next = AtomicUsize::new(0);
    let scattered = par_map(threads, |_| {
        let mut sc = Scan::new();
        let blank_ev =
            EvStage { eh: 0, part: 0, ingest: 0, uh: 0, up: 0, ulen: 0, ph: 0, tp: 0, valid: false };
        let blank_cl = ClStage { uh: 0, up: 0, ulen: 0, cp: 0, t: 0 };
        let mut w = Work {
            slab: Slab::new(&ctx.arena),
            keys: PartWriter::new(ev_parts),
            pays: PartWriter::new(person_parts),
            clks: PartWriter::new(person_parts),
            ev: [blank_ev; BATCH],
            nev: 0,
            cl: [blank_cl; BATCH],
            ncl: 0,
        };
        loop {
            let Some(&(buf, a, b, is_events)) = tasks.get(next.fetch_add(1, Relaxed)) else {
                break;
            };
            if populate {
                sys::populate(unsafe { buf.ptr.add(a) }, b - a);
            }
            unsafe {
                if mode == 1 {
                    let mut pos = scan::line_start(buf.ptr, buf.len, a);
                    while pos < b { pos = sc.scan(buf.ptr, buf.len, pos, (pos + sub).min(b)); }
                    continue;
                }
                if mode == 2 {
                    let mut x = 0u64;
                    if is_events { for_rows::<7, _>(&mut sc, buf, a, b, sub, |_, f| x += f[0].0 as u64); }
                    else { for_rows::<4, _>(&mut sc, buf, a, b, sub, |_, f| x += f[0].0 as u64); }
                    std::hint::black_box(x);
                    continue;
                }
                if mode == 3 {
                    if is_events { for_rows::<7, _>(&mut sc, buf, a, b, sub, |base, f| { event_row(&ctx, &mut w, base, f); if w.nev == BATCH - 1 { w.nev = 0; } }); }
                    else { for_rows::<4, _>(&mut sc, buf, a, b, sub, |base, f| { click_row(&ctx, &mut w, base, f); if w.ncl == BATCH - 1 { w.ncl = 0; } }); }
                    continue;
                }
                if is_events {
                    for_rows::<7, _>(&mut sc, buf, a, b, sub, |base, f| event_row(&ctx, &mut w, base, f));
                    flush_events(&ctx, &mut w);
                } else {
                    for_rows::<4, _>(&mut sc, buf, a, b, sub, |base, f| click_row(&ctx, &mut w, base, f));
                    flush_clicks(&ctx, &mut w);
                }
            }
        }
        (w.keys.finish(), w.pays.finish(), w.clks.finish())
    });
    let (mut keys, mut pays, mut clks) = (Vec::new(), Vec::new(), Vec::new());
    for (k, p, c) in scattered {
        keys.push(k);
        pays.push(p);
        clks.push(c);
    }
    let keys = merge_blocks(keys, ev_parts);
    let pays = merge_blocks(pays, person_parts);
    let clks = merge_blocks(clks, person_parts);
    lap("parse");

    // Dedup.
    let next = AtomicUsize::new(0);
    par_map(threads, |_| {
        let mut table: Vec<u64> = Vec::new();
        loop {
            let Some(blocks) = keys.get(next.fetch_add(1, Relaxed)) else {
                break;
            };
            unsafe { dedup_partition(&ctx.arena, blocks, &mut table) };
        }
    });
    lap("dedup");

    // Sessions, attribution, user_daily.
    let next = AtomicUsize::new(0);
    let outs = par_map(threads, |t| {
        let mut agg = std::mem::ManuallyDrop::new(Agg::new(&ctx, &dims, w0, w1));
        loop {
            let part = next.fetch_add(1, Relaxed);
            if part >= person_parts {
                break;
            }
            unsafe { agg.partition(&pays[part], &clks[part]) };
        }
        if agg.out.rows > 0 || t == 0 {
            write_user_file(&user_dir, t, &agg.out);
        }
        (agg.out.rows, std::mem::take(&mut agg.out.camp))
    });
    lap("aggregate");

    let day0 = w0.div_euclid(DAY as i64);
    let ndays = ((w1 - 1).div_euclid(DAY as i64) - day0 + 1).max(1) as usize;
    let mut total = vec![[0i64; 4]; (dims.camp_names.len() + 1) * ndays];
    let mut user_rows = 0usize;
    for (rows, camp) in &outs {
        user_rows += rows;
        for (t, c) in total.iter_mut().zip(camp) {
            for i in 0..4 {
                t[i] += c[i];
            }
        }
    }
    let camp_rows = write_campaign_file(&camp_dir, &dims, ndays, day0 as i32, &total);
    lap("campaigns");
    if timing {
        eprintln!("{:>10} {:>8.1} ms", "total", started.elapsed().as_secs_f64() * 1e3);
        if let Ok(roll) = fs::read_to_string("/proc/self/smaps_rollup") {
            for line in roll.lines().filter(|l| l.starts_with("Rss") || l.starts_with("AnonHuge") || l.starts_with("Anonymous")) {
                eprintln!("  {line}");
            }
        }
    }
    eprintln!("wrote user_daily={user_rows} campaign_daily={camp_rows} threads={threads}");
    // Everything still mapped is reclaimed faster by the kernel than by us.
    unsafe { libc::_exit(0) };
}
