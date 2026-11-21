//! Shopping Sessions pipeline.
//!
//! 1. Links: lock-free union-find keyed by exact ID; a person is their
//!    byte-wise smallest ID.
//! 2. Parse (all cores, vectorised CSV indexing). Every event row is decided
//!    valid or not on the spot and scattered twice: a dedup key partitioned by
//!    event-id hash, and, when valid, a payload partitioned by person. Clicks
//!    are scattered by person.
//! 3. Dedup, one event-id partition at a time: of the copies of an event only
//!    the one with the smallest `ingest_ts` keeps its payload alive.
//! 4. One person partition at a time: sessionize, attribute, aggregate, and
//!    write Parquet.

mod click;
mod dense_sessions;
mod dim;
mod event_key;
mod hashx;
mod links;
mod pq;
mod scan;
mod sys;
mod time_sort;
mod ts;

use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

use click::Click;
use dense_sessions::DenseSessions;
use dim::SmallTab;
use hashx::{r4, r8};
use links::{name_hash, same_name, Edge, Name, Table};
use pq::{bytes_of, Col, Kind};
use scan::Scan;
use sys::{par_map, Arena, Blk, Buf, PartWriter, Slab};

#[global_allocator]
static ALLOC: sys::HugeAlloc = sys::HugeAlloc;

const DAY: u64 = 86_400_000;
const WEEK: u64 = 7 * DAY;
const GAP: u64 = 1_800_000;
const MAX_PRICE: u64 = 10_000_000;
const PRICE_BITS: u32 = 24;
const PRICE_MASK: u64 = (1 << PRICE_BITS) - 1;
const PART_SHIFT: u32 = 44;
const MAX_PARTS: usize = 4096;
const BATCH: usize = 32;
const NONE: u32 = u32::MAX;

/// Exact event key and packed (ingest second, payload offset).
#[repr(C)]
#[derive(Clone, Copy)]
struct Key {
    h: u64,
    x: u64,
}

/// Dedup clears `name` when another copy outranks this event. Sorting
/// the packed (relative time, price, category) word orders events by time.
#[repr(C)]
#[derive(Clone, Copy)]
struct Pay {
    name: Name,
    w: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Clk {
    name: Name,
    click: Click,
}

struct Ctx {
    arena: Arena,
    w0: i64,
    ingest_origin: i64,
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
    ingest: i64,
    link_group: usize,
    name: Name,
    tp: u64,
    valid: bool,
}

#[derive(Clone, Copy)]
struct ClStage {
    link_group: usize,
    name: Name,
    click: Click,
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
    dates: ts::Dates,
}

fn env_path(key: &str, default: &str) -> PathBuf {
    PathBuf::from(std::env::var(key).unwrap_or_else(|_| default.to_string()))
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok().and_then(|s| s.parse().ok())
}

fn list_csvs(dir: &Path) -> Vec<(PathBuf, Buf)> {
    let Ok(rd) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = rd
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false))
        .collect();
    paths.sort();
    paths
        .into_iter()
        .map(|p| {
            let buf = sys::map_file(&p);
            (p, buf)
        })
        .filter(|(_, b)| b.len > 0)
        .collect()
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
        scan::rows::<N, _>(sc, b - pos, |fields| f(base, fields));
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
    let mut raw_products: Vec<(&[u8], u32)> = Vec::new();
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
            let c = if cat.is_empty() {
                unknown_tmp
            } else {
                intern(cat, &mut cat_ids)
            };
            raw_products.push((id, c));
        });
    }
    assert!(cat_list.len() < 60_000, "too many categories");
    let mut order: Vec<u32> = (0..cat_list.len() as u32).collect();
    order.sort_by(|&a, &b| cat_list[a as usize].cmp(&cat_list[b as usize]));
    let mut rank = vec![0u32; cat_list.len()];
    for (r, &i) in order.iter().enumerate() {
        rank[i as usize] = r as u32;
    }
    let cat_names: Vec<Vec<u8>> = order
        .iter()
        .map(|&i| cat_list[i as usize].clone())
        .collect();
    let mut products = SmallTab::new(raw_products.len(), b"p");
    for &(id, c) in &raw_products {
        products.insert(id, rank[c as usize]);
    }

    let mut camp_names: Vec<Vec<u8>> = Vec::new();
    let mut channels: Vec<Vec<u8>> = Vec::new();
    let buf = sys::map_file(&data.join("campaigns.csv"));
    unsafe {
        for_rows::<2, _>(&mut sc, buf, 0, buf.len, 1 << 16, |base, f| {
            let id = field(base, f[0]);
            if id.is_empty() {
                return;
            }
            camp_names.push(id.to_vec());
            channels.push(field(base, f[1]).to_vec());
        });
    }
    assert!(camp_names.len() < 60_000, "too many campaigns");
    let mut campaigns = SmallTab::new(camp_names.len(), b"c_");
    for (i, id) in camp_names.iter().enumerate() {
        campaigns.insert(id, i as u32);
    }
    let unknown = rank[unknown_tmp as usize] as u64;
    (
        products,
        campaigns,
        unknown,
        Dims {
            cat_names,
            camp_names,
            channels,
        },
    )
}

fn load_links(files: &[(PathBuf, Buf)], threads: usize) -> Table {
    let mut tasks: Vec<(Buf, usize, usize)> = Vec::new();
    for &(_, buf) in files {
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
                    edges.push((links::name_of(pa, la), links::name_of(pb, lb)));
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
unsafe fn event_row(
    ctx: &Ctx,
    w: &mut Work,
    base: *const u8,
    source: *const u8,
    f: &[(u32, u32); 7],
) {
    let (b0, e0) = f[0];
    if b0 == e0 {
        return;
    }
    let ingest = w
        .dates
        .ingest_seconds(base.add(f[3].0 as usize), (f[3].1 - f[3].0) as usize);
    if ingest == ts::BAD {
        return;
    }
    let (eh, eh2) = event_key::encode(
        base.add(b0 as usize),
        (e0 - b0) as usize,
        source.add(b0 as usize),
    );
    let t = w
        .dates
        .parse(base.add(f[2].0 as usize), (f[2].1 - f[2].0) as usize);
    let rel = t.wrapping_sub(ctx.w0) as u64;
    let user_len = (f[1].1 - f[1].0) as usize;
    let mut valid = (user_len != 0)
        & (f[5].1 != f[5].0)
        & (rel < ctx.wlen)
        & ((ingest * 1000).wrapping_sub(t) <= DAY as i64);
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
    s.ingest = ingest - ctx.ingest_origin;
    s.valid = valid;
    if valid {
        let up = base.add(f[1].0 as usize);
        s.name = links::name_at(up, user_len, source.add(f[1].0 as usize));
        s.link_group = ctx.links.prefetch(name_hash(s.name));
        let mut cat = 0;
        if price != 0 {
            let pp = base.add(f[5].0 as usize);
            let c = ctx.products.get(pp, (f[5].1 - f[5].0) as usize);
            cat = if c == NONE { ctx.unknown } else { c as u64 };
        }
        s.tp = (rel << (PRICE_BITS + ctx.cat_bits)) | (price << ctx.cat_bits) | cat;
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
            let name = ctx.links.lookup(s.link_group, s.name);
            let part = (name_hash(name).rotate_left(24) >> PART_SHIFT) as usize & ctx.person_mask;
            let at = w.pays.push(part, &mut w.slab, Pay { name, w: s.tp });
            pay = ((at - ctx.arena.base) / Arena::UNIT) as u64;
        }
        let part = (s.part >> PART_SHIFT) as usize & ctx.ev_mask;
        w.keys.push(
            part,
            &mut w.slab,
            Key {
                h: s.eh,
                x: event_key::pack(s.ingest, pay),
            },
        );
    }
    w.nev = 0;
}

/// `base` holds the row (possibly a reusable read buffer); `source` is the
/// same row in the file mapping, which lives until exit. Names and click IDs
/// that are not inline keep `source` addresses, so the read buffer can be
/// reused; those mapped pages are only touched for the rare long IDs and ties.
#[inline(always)]
unsafe fn click_row(ctx: &Ctx, w: &mut Work, base: *const u8, source: *const u8, f: &[(u32, u32); 4]) {
    let user_len = (f[1].1 - f[1].0) as usize;
    if f[0].0 == f[0].1 || user_len == 0 {
        return;
    }
    let t = w
        .dates
        .parse(base.add(f[3].0 as usize), (f[3].1 - f[3].0) as usize);
    let rel = t.wrapping_sub(ctx.w0 - WEEK as i64) as u64;
    if rel >= ctx.wlen + WEEK {
        return;
    }
    let cp = base.add(f[2].0 as usize);
    let camp = ctx.campaigns.get(cp, (f[2].1 - f[2].0) as usize);
    if camp == NONE {
        return;
    }
    let up = base.add(f[1].0 as usize);
    let s = w.cl.get_unchecked_mut(w.ncl);
    s.name = links::name_at(up, user_len, source.add(f[1].0 as usize));
    s.link_group = ctx.links.prefetch(name_hash(s.name));
    s.click = Click::new(
        rel,
        camp,
        base.add(f[0].0 as usize),
        (f[0].1 - f[0].0) as usize,
        source.add(f[0].0 as usize),
    );
    w.ncl += 1;
    if w.ncl == BATCH {
        flush_clicks(ctx, w);
    }
}

#[inline(never)]
unsafe fn flush_clicks(ctx: &Ctx, w: &mut Work) {
    for i in 0..w.ncl {
        let s = *w.cl.get_unchecked(i);
        let name = ctx.links.lookup(s.link_group, s.name);
        let part = (name_hash(name).rotate_left(24) >> PART_SHIFT) as usize & ctx.person_mask;
        w.clks.push(
            part,
            &mut w.slab,
            Clk {
                name,
                click: s.click,
            },
        );
    }
    w.ncl = 0;
}

/// Blocks of a partition are scattered, so the hardware prefetcher loses the
/// trail at the end of each one.
#[inline(always)]
unsafe fn prefetch_block(blk: Option<&Blk>) {
    if let Some(blk) = blk {
        for line in 0..4 {
            _mm_prefetch((blk.ptr + line * 64) as *const i8, _MM_HINT_T0);
        }
    }
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
        let at = event_key::payload(x);
        if at != 0 {
            (*((arena.base + at * Arena::UNIT) as *mut Pay)).name = 0;
        }
    };
    for (b, blk) in blocks.iter().enumerate() {
        prefetch_block(blocks.get(b + 1));
        let base = blk.ptr as *const Key;
        for i in 0..blk.n as usize {
            let k = base.add(i);
            let h = event_key::probe((*k).h);
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
                    if event_key::equal((*o).h, (*k).h) {
                        if event_key::earlier((*k).x, (*o).x) {
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

/// Per-worker event counts. Common values use their own dictionary index.
/// A full dictionary can always expand back to PLAIN int32 without losing a row.
struct Counts {
    indices: Vec<u16>,
    values: Vec<i32>,
    large: HashMap<u32, u16>,
    plain: Option<Vec<i32>>,
}

impl Counts {
    fn new(hint: usize) -> Self {
        Self {
            indices: Vec::with_capacity(hint),
            values: (0..256).collect(),
            large: HashMap::new(),
            plain: None,
        }
    }

    #[inline(always)]
    fn push(&mut self, n: u32) {
        if let Some(plain) = &mut self.plain {
            plain.push(n as i32);
        } else if n < 256 {
            self.indices.push(n as u16);
        } else {
            self.large_count(n);
        }
    }

    fn large_count(&mut self, n: u32) {
        if let Some(&index) = self.large.get(&n) {
            self.indices.push(index);
        } else if let Ok(index) = u16::try_from(self.values.len()) {
            self.values.push(n as i32);
            self.large.insert(n, index);
            self.indices.push(index);
        } else {
            let mut plain: Vec<i32> = std::mem::take(&mut self.indices)
                .into_iter()
                .map(|i| self.values[i as usize])
                .collect();
            plain.push(n as i32);
            self.plain = Some(plain);
        }
    }
}

/// One thread's share of both output tables.
struct Out {
    names: Vec<u8>,
    name_runs: Vec<u8>,
    people: usize,
    day: Vec<u8>,
    wide_day: Vec<i32>,
    sessions: Vec<u8>,
    events: Counts,
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
    simd_sort: bool,
    dense: DenseSessions,
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
    ck_raw: Vec<Click>,
    evs: Vec<u64>,
    cks: Vec<Click>,
    cnt: Vec<u32>,
    act: Vec<u32>,
    days: Vec<(u32, u32)>,
    cat_sum: Vec<u64>,
    touched: Vec<u32>,
}

impl<'a> Agg<'a> {
    fn new(ctx: &'a Ctx, dims: &'a Dims, w0: i64, w1: i64, rows_hint: usize) -> Self {
        let day0 = w0.div_euclid(DAY as i64);
        let ndays = ((w1 - 1).div_euclid(DAY as i64) - day0 + 1).max(1) as usize;
        let ncamp = dims.camp_names.len();
        Agg {
            ctx,
            dense: DenseSessions::default(),
            simd_sort: std::env::var_os("SS_AVX2").as_deref() != Some(std::ffi::OsStr::new("1"))
                && is_x86_feature_detected!("avx512f"),
            cat_names: &dims.cat_names,
            ndays,
            day0: day0 as i32,
            day_off: (w0 - day0 * DAY as i64) as u64,
            organic: ncamp,
            out: Out {
                // Sized up front (a generous estimate of this worker's rows):
                // large blocks come from the huge-page allocator, untouched
                // capacity costs nothing, and the columns never get copied.
                names: Vec::with_capacity(rows_hint * 8),
                name_runs: {
                    let mut v = Vec::with_capacity(rows_hint * 4);
                    v.push(32); // dictionary index width in bits
                    v
                },
                people: 0,
                day: Vec::with_capacity(if ndays <= 256 { rows_hint } else { 0 }),
                wide_day: Vec::new(),
                sessions: Vec::with_capacity(rows_hint),
                events: Counts::new(rows_hint),
                revenue: Vec::with_capacity(rows_hint),
                top_def: Vec::with_capacity(rows_hint / 8 + 1),
                top: Vec::with_capacity(rows_hint * 4),
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
        for (b, blk) in pays.iter().enumerate() {
            prefetch_block(pays.get(b + 1));
            let base = blk.ptr as *const Pay;
            for i in 0..blk.n as usize {
                let p = *base.add(i);
                if p.name == 0 {
                    continue;
                }
                let name = p.name;
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
        for (b, blk) in clks.iter().enumerate() {
            prefetch_block(clks.get(b + 1));
            let base = blk.ptr as *const Clk;
            for i in 0..blk.n as usize {
                let c = *base.add(i);
                let name = c.name;
                let mut slot = (name_hash(name) >> 32) as usize & mask;
                loop {
                    let s = *slots.add(slot);
                    if s == 0 {
                        break;
                    }
                    if same_name(*self.names.get_unchecked(s as usize - 1), name) {
                        *self.ck_end.get_unchecked_mut(s as usize - 1) += 1;
                        self.ck_idx.push(s - 1);
                        self.ck_raw.push(c.click);
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
            let at = self
                .ev_end
                .get_unchecked_mut(*self.ev_idx.get_unchecked(i) as usize);
            *self.evs.get_unchecked_mut(*at as usize) = *self.ev_w.get_unchecked(i);
            *at += 1;
        }
        let nck = self.ck_raw.len();
        if self.cks.len() < nck {
            self.cks.resize(nck, Click::default());
        }
        for i in 0..nck {
            let at = self
                .ck_end
                .get_unchecked_mut(*self.ck_idx.get_unchecked(i) as usize);
            *self.cks.get_unchecked_mut(*at as usize) = *self.ck_raw.get_unchecked(i);
            *at += 1;
        }
        let (mut ev_at, mut ck_at) = (0usize, 0usize);
        for p in 0..people {
            let ev_to = *self.ev_end.get_unchecked(p) as usize;
            let ck_end = *self.ck_end.get_unchecked(p) as usize;
            let events = &mut self.evs[ev_at..ev_to];
            let dense = events.len() >= 256 && self.ctx.wlen < 1 << 33;
            if dense {
                self.dense.group(events, self.ctx.wlen, self.ctx.cat_bits);
            } else if events.len() <= 128 && self.simd_sort {
                time_sort::small(events);
            } else {
                events.sort_unstable();
            }
            let ck_to = if dense {
                ck_at + self.dense.prune_clicks(
                    &mut self.cks[ck_at..ck_end],
                    self.organic,
                    &mut self.ck_raw,
                )
            } else {
                ck_end
            };
            let clicks = &mut self.cks[ck_at..ck_to];
            if clicks.len() >= 1024 && self.ctx.wlen + WEEK < 1 << 33 {
                if self.ck_raw.len() < clicks.len() {
                    self.ck_raw.resize(clicks.len(), Click::default());
                }
                time_sort::radix_time(clicks, &mut self.ck_raw, Click::time);
            } else if clicks.len() > 1 {
                clicks.sort_unstable_by_key(|c| c.time());
            }
            let name = *self.names.get_unchecked(p);
            if dense {
                self.person::<true>(name, ev_at, ev_to, ck_at, ck_to);
            } else {
                self.person::<false>(name, ev_at, ev_to, ck_at, ck_to);
            }
            ev_at = ev_to;
            ck_at = ck_end;
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
                self.out
                    .camp
                    .get_unchecked_mut(camp * self.ndays + day as usize)[3] += (end - start) as i64;
            }
            if begin <= from {
                break;
            }
            end = begin;
        }
    }

    #[inline(always)]
    unsafe fn user_row(&mut self, day: u32, sessions: u32, events: u32, revenue: u64) {
        let o = &mut self.out;
        if self.ndays <= 256 {
            o.day.push(day as u8);
        } else {
            o.wide_day.push(self.day0 + day as i32);
        }
        // Session starts are more than 30 minutes apart, even between sessions
        // crossing midnight. No UTC day can hold more than 48 starts per person.
        o.sessions.push(sessions as u8);
        o.events.push(events);
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

    unsafe fn person<const DENSE: bool>(
        &mut self,
        name: Name,
        ev_at: usize,
        ev_to: usize,
        ck_at: usize,
        ck_to: usize,
    ) {
        let first_row = self.out.rows;
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
        loop {
            let start = if DENSE {
                let Some(summary) = self.dense.summaries.get(session as usize) else {
                    break;
                };
                summary.start
            } else {
                if k == ev_to {
                    break;
                }
                *ev.add(k) >> shift
            };
            let day = ((start + self.day_off) / DAY) as u32;
            if day != cur_day {
                if cur_day != u32::MAX {
                    self.user_row(cur_day, row_sessions, row_events, row_revenue);
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
            let end = if DENSE {
                let summary = self.dense.summaries[session as usize];
                events = summary.events;
                ev_at + summary.purchase_end as usize
            } else {
                ev_to
            };
            while k < end {
                let w = *ev.add(k);
                if !DENSE {
                    let t = w >> shift;
                    if t - prev > GAP {
                        break;
                    }
                    prev = t;
                    events += 1;
                }
                let price = (w >> cat_bits) & PRICE_MASK;
                if DENSE || price != 0 {
                    revenue += price;
                    let c = (w & cat_mask) as usize;
                    let sum = self.cat_sum.get_unchecked_mut(c);
                    if *sum == 0 {
                        self.touched.push(c as u32);
                    }
                    *sum += price;
                }
                k += 1;
            }

            // Click times are offset by 7 days, so the clicks that count for
            // this session are those with `start <= t <= start + WEEK`.
            let latest = start + WEEK;
            while hi < ck_to && (*ck.add(hi)).time() <= latest {
                let c = (*ck.add(hi)).campaign();
                let n = self.cnt.get_unchecked_mut(c);
                if *n == 0 {
                    *self.act.get_unchecked_mut(c) = session;
                }
                *n += 1;
                hi += 1;
            }
            while lo < hi && (*ck.add(lo)).time() < start {
                let c = (*ck.add(lo)).campaign();
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
                let t = (*ck.add(best)).time();
                let mut j = best;
                while j > lo && (*ck.add(j - 1)).time() == t {
                    j -= 1;
                    if (*ck.add(j)).precedes(*ck.add(best)) {
                        best = j;
                    }
                }
                (*ck.add(best)).campaign()
            } else {
                self.organic
            };
            let acc = self
                .out
                .camp
                .get_unchecked_mut(camp * self.ndays + day as usize);
            acc[0] += 1;
            acc[1] += (revenue != 0) as i64;
            acc[2] += revenue as i64;
            row_sessions += 1;
            row_events += events;
            row_revenue += revenue;
            session += 1;
        }
        self.user_row(cur_day, row_sessions, row_events, row_revenue);
        // All days for this person are consecutive. One RLE run addresses
        // them all, and the ID bytes appear only once in the dictionary.
        let out = &mut self.out;
        links::write_name(name, &mut out.names);
        let mut run = ((out.rows - first_row) as u64) << 1;
        while run >= 128 {
            out.name_runs.push((run as u8) | 128);
            run >>= 7;
        }
        out.name_runs.push(run as u8);
        out.name_runs
            .extend_from_slice(&(out.people as u32).to_le_bytes());
        out.people += 1;
        for j in lo..hi {
            let c = (*ck.add(j)).campaign();
            if *self.cnt.get_unchecked(c) != 0 {
                *self.cnt.get_unchecked_mut(c) = 0;
                let from = *self.act.get_unchecked(c);
                self.assist(c, from, session);
            }
        }
    }
}

fn write_user_file(dir: &Path, index: usize, o: &Out, day0: i32, ndays: usize) {
    // A byte-aligned dictionary bit-pack run directly consumes the compact
    // in-memory indices. The Parquet hybrid encoding pads the last group to 8.
    let groups = o.rows.div_ceil(8);
    let missing = groups * 8 - o.rows;
    let pad = &[0u8; 7][..missing];
    let mut run = ((groups as u64) << 1) | 1;
    let mut header = vec![8];
    while run >= 128 {
        header.push((run as u8) | 128);
        run >>= 7;
    }
    header.push(run as u8);
    let session_values: [i32; 49] = std::array::from_fn(|n| n as i32);
    let days = if ndays <= 256 {
        (0..ndays).map(|n| day0 + n as i32).collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let day_col = if ndays <= 256 {
        Col {
            name: "day",
            kind: Kind::DictDate(days.len()),
            parts: vec![bytes_of(&days), &header, &o.day, pad],
        }
    } else {
        Col {
            name: "day",
            kind: Kind::Date,
            parts: vec![bytes_of(&o.wide_day)],
        }
    };

    let mut count_header = header.clone();
    count_header[0] = 16;
    let count_pad = &[0u8; 14][..2 * missing];
    let counts = if let Some(plain) = &o.events.plain {
        Col {
            name: "events",
            kind: Kind::I32,
            parts: vec![bytes_of(plain)],
        }
    } else {
        Col {
            name: "events",
            kind: Kind::DictInt(o.events.values.len()),
            parts: vec![
                bytes_of(&o.events.values),
                &count_header,
                bytes_of(&o.events.indices),
                count_pad,
            ],
        }
    };
    let cols = [
        Col {
            name: "user_id",
            kind: Kind::DictStr(o.people),
            parts: vec![&o.names, &o.name_runs],
        },
        day_col,
        Col {
            name: "sessions",
            kind: Kind::DictInt(session_values.len()),
            parts: vec![bytes_of(&session_values), &header, &o.sessions, pad],
        },
        counts,
        Col {
            name: "revenue_cents",
            kind: Kind::I64,
            parts: vec![bytes_of(&o.revenue)],
        },
        Col {
            name: "top_category",
            kind: Kind::OptStr,
            parts: vec![&o.top_def, &o.top],
        },
    ];
    pq::write_file(&dir.join(format!("part-{index}.parquet")), o.rows, &cols);
}

fn write_campaign_file(
    dir: &Path,
    dims: &Dims,
    ndays: usize,
    day0: i32,
    acc: &[[i64; 4]],
) -> usize {
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
        Col {
            name: "campaign_id",
            kind: Kind::Str,
            parts: vec![&ids],
        },
        Col {
            name: "channel",
            kind: Kind::Str,
            parts: vec![&channels],
        },
        Col {
            name: "day",
            kind: Kind::Date,
            parts: vec![bytes_of(&day)],
        },
        Col {
            name: "sessions",
            kind: Kind::I64,
            parts: vec![bytes_of(&cols[0])],
        },
        Col {
            name: "purchasing_sessions",
            kind: Kind::I64,
            parts: vec![bytes_of(&cols[1])],
        },
        Col {
            name: "revenue_cents",
            kind: Kind::I64,
            parts: vec![bytes_of(&cols[2])],
        },
        Col {
            name: "assisted_sessions",
            kind: Kind::I64,
            parts: vec![bytes_of(&cols[3])],
        },
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
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
        .min(256);
    let data = env_path("DATA_DIR", "/data");
    let out_dir = env_path("OUTPUT_DIR", "/output");
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&user_dir).expect("create user_daily");
    fs::create_dir_all(&camp_dir).expect("create campaign_daily");

    let (w0, w1) = load_manifest(&data);
    let prepare_inputs = || {
        let dims = load_dims(&data);
        let events = list_csvs(&data.join("events"));
        let clicks = list_csvs(&data.join("clicks"));
        (dims, events, clicks)
    };
    let build_links = |workers| load_links(&list_csvs(&data.join("links")), workers);
    let (inputs, links) = if threads == 1 {
        (prepare_inputs(), build_links(1))
    } else {
        std::thread::scope(|scope| {
            let inputs = scope.spawn(prepare_inputs);
            let links = build_links(threads - 1);
            (inputs.join().expect("prepare inputs"), links)
        })
    };
    let ((products, campaigns, unknown, dims), event_files, click_files) = inputs;
    lap("setup+links");

    let cat_bits = (usize::BITS - dims.cat_names.len().leading_zeros()).max(1);
    let wlen = (w1 - w0).max(0) as u64;
    assert!(
        (64 - wlen.leading_zeros()) + PRICE_BITS + cat_bits <= 64,
        "window too long to pack with {} categories",
        dims.cat_names.len()
    );
    let event_bytes: usize = event_files.iter().map(|(_, b)| b.len).sum();
    let click_bytes: usize = click_files.iter().map(|(_, b)| b.len).sum();
    let per_part = env_usize("SS_PART_ROWS").unwrap_or(50_000).max(1);
    let ev_parts = parts_for(event_bytes / 80, per_part);
    let person_parts = parts_for(event_bytes / 80 + click_bytes / 50, per_part);
    let ctx = Ctx {
        arena: Arena::new(),
        w0,
        ingest_origin: w0.div_euclid(1000) - (1 << 31),
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
    let buffered = std::env::var("SS_READ").as_deref() != Ok("mmap");
    let mut tasks: Vec<(Buf, usize, usize, &Path)> = Vec::new();
    for (path, buf) in &event_files {
        for (a, b) in chunks(buf.len, chunk) {
            tasks.push((*buf, a, b, path));
        }
    }
    // Clicks come second and are read like
    // events: a pread into a reusable buffer avoids installing, then tearing
    // down, a page-table entry for every page of the click files.
    let n_event_tasks = tasks.len();
    for (path, buf) in &click_files {
        for (a, b) in chunks(buf.len, chunk) {
            tasks.push((*buf, a, b, path));
        }
    }
    let next = AtomicUsize::new(0);
    let scattered = par_map(threads, |_| {
        let mut sc = Scan::new();
        let mut reader = sys::EventReader::default();
        let blank_ev = EvStage {
            eh: 0,
            part: 0,
            ingest: 0,
            link_group: 0,
            name: 0,
            tp: 0,
            valid: false,
        };
        let blank_cl = ClStage {
            link_group: 0,
            name: 0,
            click: Click::default(),
        };
        let mut w = Work {
            slab: Slab::new(&ctx.arena),
            keys: PartWriter::new(ev_parts),
            pays: PartWriter::new(person_parts),
            clks: PartWriter::new(person_parts),
            ev: [blank_ev; BATCH],
            nev: 0,
            cl: [blank_cl; BATCH],
            ncl: 0,
            dates: ts::Dates::new(),
        };
        loop {
            let task = next.fetch_add(1, Relaxed);
            let Some(&(buf, a, b, path)) = tasks.get(task) else {
                break;
            };
            let is_click = task >= n_event_tasks;
            unsafe {
                // Large files: line boundaries are found in the read buffer,
                // so the mapping is not touched (no faults, no page-table
                // entries to tear down at exit).
                let (input, start, len) = if buffered && buf.can_pread && buf.len >= 256 << 10 {
                    reader.read_lines(path, buf, a, b)
                } else {
                    let start = scan::line_start(buf.ptr, buf.len, a);
                    let end = scan::line_start(buf.ptr, buf.len, b);
                    sys::populate(buf.ptr.add(start), end - start);
                    (buf.ptr.add(start), start, end - start)
                };
                if len == 0 {
                    continue;
                }
                let source = buf.ptr.add(start);
                let mut pos = 0;
                while pos < len {
                    let next = sc.scan(input, len, pos, (pos + sub).min(len));
                    let base = input.add(pos);
                    let mapped = source.add(pos);
                    if is_click {
                        scan::rows::<4, _>(&sc, len - pos, |fields| {
                            click_row(&ctx, &mut w, base, mapped, fields)
                        });
                    } else {
                        scan::rows::<7, _>(&sc, len - pos, |fields| {
                            event_row(&ctx, &mut w, base, mapped, fields)
                        });
                    }
                    pos = next;
                }
                if is_click {
                    flush_clicks(&ctx, &mut w);
                } else {
                    flush_events(&ctx, &mut w);
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

    // Start partitions containing large linked people first so their work
    // overlaps the other workers' ordinary partitions.
    let mut order: Vec<usize> = (0..person_parts).collect();
    order.sort_unstable_by_key(|&p| std::cmp::Reverse(pays[p].len() + clks[p].len()));
    // About one user_daily row per 9 live events; twice that, per worker.
    let live_events: usize = pays.iter().flatten().map(|b| b.n as usize).sum();
    let rows_hint = live_events / 4 / threads + 4096;
    let next = AtomicUsize::new(0);
    let outs = par_map(threads, |t| {
        let mut agg = std::mem::ManuallyDrop::new(Agg::new(&ctx, &dims, w0, w1, rows_hint));
        loop {
            let Some(&part) = order.get(next.fetch_add(1, Relaxed)) else {
                break;
            };
            unsafe { agg.partition(&pays[part], &clks[part]) };
        }
        if agg.out.rows > 0 || t == 0 {
            write_user_file(&user_dir, t, &agg.out, agg.day0, agg.ndays);
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
        eprintln!(
            "{:>10} {:>8.1} ms",
            "total",
            started.elapsed().as_secs_f64() * 1e3
        );
        if let Ok(roll) = fs::read_to_string("/proc/self/smaps_rollup") {
            for line in roll.lines().filter(|l| {
                l.starts_with("Rss") || l.starts_with("AnonHuge") || l.starts_with("Anonymous")
            }) {
                eprintln!("  {line}");
            }
        }
    }
    eprintln!("wrote user_daily={user_rows} campaign_daily={camp_rows} threads={threads}");
    // Everything still mapped is reclaimed faster by the kernel than by us.
    std::process::exit(0);
}

#[cfg(test)]
mod output_tests {
    use super::Counts;

    #[test]
    fn full_count_dictionary_expands_existing_rows_and_accepts_more() {
        let mut column = Counts::new(0);
        let high = u32::from(u16::MAX);
        let counts = (0..=high + 1).chain([123_456_789, 255, 999]);
        for value in counts.clone() {
            column.push(value);
        }
        assert!(column.plain.unwrap().into_iter().eq(counts.map(|v| v as i32)));
    }
}
