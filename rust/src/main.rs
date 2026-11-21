//! Shopping Sessions: fast pipeline (Rust port of src/ss.cpp).
//!
//! Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet,
//! exactly as rules.md defines. Logs phase timings to stderr.
//!
//! Phases:
//!   L. links: parse, intern IDs per hash bucket, then union-find on a
//!      background thread (person name = byte-wise smallest ID) while ...
//!   1. events and clicks are parsed in parallel with pread into small
//!      per-thread buffers. Each valid, non-late event copy goes straight to
//!      its user-ID bucket; every well-formed copy also gets a 16-byte dedup
//!      entry in its event-ID bucket. Valid clicks go to their user-ID bucket.
//!   2. dedup per event-ID bucket: the earliest ingest_ts wins; all other
//!      copies are marked in bitmaps.
//!   3. per user-ID bucket: map events and clicks to their person.
//!   4. per person bucket: sessionize, credit, assists, daily aggregates;
//!      user_daily rows are written as each bucket finishes.
//!   5. campaign_daily.

mod csv;
mod ids;
mod maps;
mod parquet;
mod sys;
mod ts;

use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use csv::{each_line, split, split_line, Chunk, Fld, Reader};
use ids::{key_bytes, key_less, khash, make_key, pow2ge};
use maps::{NumMap, SMap};
use parquet::{ColBuf, Kind, PqWriter};
use sys::{die, make_buckets, parallel_for, per_thread, Arena, BList, Pad, Shared};
use ts::{parse_ts, parse_ts_fast, ts_tables, TsTables};

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const NB: usize = 256; // hash buckets
const BSH: u32 = 56; // bucket = hash >> BSH
const LBITS: u32 = 22; // packed link node: bucket << LBITS | local
const NOLOC: u32 = u32::MAX;

// ------------------------------------------------------------------ records
// pc = price << 8 | category. A purchase is an event with price > 0 (a valid
// purchase costs at least 1, at most 10,000,000 < 2^24).
#[derive(Clone, Copy)]
#[repr(C)]
struct DEnt {
    ek: u64,  // event_id
    ing: u32, // ingest_ts, seconds (relative)
    loc: u32, // where the copy's SvEv is (thread, user bucket, index), or NOLOC
}
#[derive(Clone, Copy)]
#[repr(C)]
struct SvEv {
    uk: u64,
    ts: u32,
    pc: u32,
}
#[derive(Clone, Copy)]
#[repr(C)]
struct ClRec {
    uk: u64,
    ck: u64,
    ts: u32,
    camp: u16,
}
#[derive(Clone, Copy)]
#[repr(C)]
struct LSide {
    k: u64,
    slot: u32,
}
#[derive(Clone, Copy)]
#[repr(C)]
struct PEv {
    nk: u64,
    ts: u32,
    pc: u32,
}
#[derive(Clone, Copy)]
#[repr(C)]
struct PCl {
    nk: u64,
    ck: u64,
    ts: u32,
    camp: u16,
}
#[derive(Clone, Copy)]
struct UserRow {
    nk: u64,
    day: i32,
    sessions: u32,
    events: u32,
    rev: i64,
    top: i32,
}
#[derive(Clone, Copy, Default)]
struct CD {
    sessions: i64,
    purch: i64,
    rev: i64,
    assisted: i64,
}

struct Lap {
    t0: Instant,
    last: Instant,
}
impl Lap {
    fn lap(&mut self, what: &str) {
        let now = Instant::now();
        eprintln!("{:<12} {:7.3}s  (total {:.3}s)", what, (now - self.last).as_secs_f64(), (now - self.t0).as_secs_f64());
        self.last = now;
    }
}

fn read_file(path: &str) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|_| die(&format!("read {path}")))
}
fn json_str(j: &str, key: &str) -> String {
    let k = j.find(&format!("\"{key}\"")).unwrap_or_else(|| die("manifest key"));
    let c = k + j[k..].find(':').unwrap();
    let q1 = c + 1 + j[c + 1..].find('"').unwrap();
    let q2 = q1 + 1 + j[q1 + 1..].find('"').unwrap();
    j[q1 + 1..q2].to_string()
}
fn list_csv(dir: &str) -> Vec<String> {
    let mut out: Vec<String> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".csv"))
            .map(|n| format!("{dir}/{n}"))
            .collect(),
        Err(_) => Vec::new(),
    };
    out.sort();
    out
}
fn env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}
fn floordiv(a: i64, b: i64) -> i64 {
    a.div_euclid(b)
}

fn main() {
    let t0 = Instant::now();
    let mut lap = Lap { t0, last: t0 };
    let args: Vec<String> = std::env::args().collect();
    let data = if args.len() >= 3 { args[args.len() - 2].clone() } else { env("DATA_DIR").unwrap_or("/data".into()) };
    let out = if args.len() >= 3 { args[args.len() - 1].clone() } else { env("OUTPUT_DIR").unwrap_or("/output".into()) };
    let nt: usize = env("NCPU")
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
        .clamp(1, 64);
    unsafe {
        sys::NOTHP = env("SS_NOTHP").is_some();
        sys::NOFREE = env("SS_NOFREE").is_some();
    }
    let tst: TsTables = ts_tables();
    let tst = &tst;

    // ---- manifest, products, campaigns
    let (ws, we) = {
        let man = String::from_utf8_lossy(&read_file(&format!("{data}/manifest.json"))).into_owned();
        let a = json_str(&man, "window_start");
        let b = json_str(&man, "window_end");
        match (parse_ts(a.as_bytes()), parse_ts(b.as_bytes())) {
            (Some(x), Some(y)) => (x, y),
            _ => die("manifest ts"),
        }
    };
    let cws = ws - WEEK_MS;
    if we - cws > 4_000_000_000 || we <= ws {
        die("window not supported");
    }
    let day0 = floordiv(ws, DAY_MS) as i32;
    let ndays = (floordiv(we - 1, DAY_MS) - day0 as i64 + 1) as usize;

    let mut products = SMap::new();
    let mut prod_num = NumMap::new();
    let mut prod_cat: Vec<u8> = Vec::new();
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    {
        let text = read_file(&format!("{data}/products.csv"));
        let mut cats = SMap::new();
        let mut prod_ids: Vec<(Vec<u8>, i32)> = Vec::new();
        each_line(&text, |l| {
            let mut f = [Fld::default(); 2];
            if !unsafe { split(l.as_ptr(), l.as_ptr().add(l.len()), &mut f, 2) } {
                return;
            }
            let (pid, cat) = (f[0].bytes(), f[1].bytes());
            let mut c = cats.find(cat);
            if c < 0 {
                if cat_names.len() >= 255 {
                    die("more than 255 categories");
                }
                c = cat_names.len() as i32;
                cats.insert(cat, c);
                cat_names.push(cat.to_vec());
            }
            if products.insert(pid, prod_cat.len() as i32) {
                prod_ids.push((pid.to_vec(), prod_cat.len() as i32));
                prod_cat.push(c as u8);
            }
        });
        prod_num.build(&prod_ids);
    }
    let unknown_cat = cat_names.len() as u32;
    cat_names.push(b"unknown".to_vec());
    let ncat = cat_names.len();
    let mut cat_rank = vec![0i32; ncat];
    {
        let mut ord: Vec<usize> = (0..ncat).collect();
        ord.sort_by(|&a, &b| cat_names[a].cmp(&cat_names[b]));
        for (i, &c) in ord.iter().enumerate() {
            cat_rank[c] = i as i32;
        }
    }
    let mut campaigns = SMap::new();
    let mut camp_num = NumMap::new();
    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_chan: Vec<Vec<u8>> = Vec::new();
    {
        let text = read_file(&format!("{data}/campaigns.csv"));
        let mut pairs: Vec<(Vec<u8>, i32)> = Vec::new();
        each_line(&text, |l| {
            let mut f = [Fld::default(); 2];
            if !unsafe { split(l.as_ptr(), l.as_ptr().add(l.len()), &mut f, 2) } {
                return;
            }
            if campaigns.insert(f[0].bytes(), camp_ids.len() as i32) {
                pairs.push((f[0].bytes().to_vec(), camp_ids.len() as i32));
                camp_ids.push(f[0].bytes().to_vec());
                camp_chan.push(f[1].bytes().to_vec());
            }
        });
        camp_num.build(&pairs);
    }
    let ncamp = camp_ids.len();
    let cat_by_num: Vec<u8> = prod_num.table(unknown_cat as u8, |i| prod_cat[i as usize]);
    let camp_by_num: Vec<i32> = camp_num.table(-1, |i| i);
    if ncamp >= 65535 {
        die("too many campaigns");
    }
    lap.lap("setup");

    // ---- open inputs, make chunks
    let mut files: Vec<File> = Vec::new();
    let mut link_chunks: Vec<Chunk> = Vec::new();
    let mut data_chunks: Vec<Chunk> = Vec::new();
    let ch: usize = env("SS_CHUNK").and_then(|s| s.parse().ok()).unwrap_or(8 << 20).max(1); // test hook
    let rbuf: usize = env("SS_RBUF").and_then(|s| s.parse().ok()).unwrap_or(1 << 20).max(64); // test hook
    for (k, sub) in ["events", "clicks", "links"].iter().enumerate() {
        for path in list_csv(&format!("{data}/{sub}")) {
            let f = File::open(&path).unwrap_or_else(|_| die(&format!("open {path}")));
            let n = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
            let fi = files.len();
            files.push(f);
            let mut b = 0;
            while b < n {
                let c = Chunk { kind: k as u8, file: fi, size: n, b, e: (b + ch).min(n) };
                if k == 2 { link_chunks.push(c) } else { data_chunks.push(c) }
                b += ch;
            }
        }
    }
    let files = &files;
    let ar_e = per_thread(nt, Arena::new);
    let ar_c = per_thread(nt, Arena::new);
    let ar_l = per_thread(nt, Arena::new);
    let ar_s = per_thread(nt, Arena::new);
    let ar_p = per_thread(nt, Arena::new);
    let readers = per_thread(nt, || Reader::new(rbuf));
    lap.lap("open");

    // ---- phase L: links
    let lkb = make_buckets::<LSide>(nt, NB);
    let nedges: Vec<Shared<Pad<u32>>> = per_thread(nt, || Pad(0));
    parallel_for(link_chunks.len(), nt, |ci, tid| {
        let lb = lkb[tid].get();
        let al = ar_l[tid].get();
        let mut ne = nedges[tid].get().0; // local: no false sharing
        let c = &link_chunks[ci];
        readers[tid].get().run(c, &files[c.file], |p, fe, eof| unsafe {
            let mut f = [Fld::default(); 2];
            let mut next = p;
            let r = split_line(p, fe, eof, &mut f, 2, &mut next);
            if r < 0 {
                return std::ptr::null();
            }
            if r == 0 || f[0].n == 0 || f[1].n == 0 {
                return next;
            }
            let e = ne;
            ne += 1;
            for (sd, fl) in f.iter().enumerate() {
                let k = make_key(fl.p, fl.n);
                lb[(khash(k) >> BSH) as usize].push(LSide { k, slot: e * 2 + sd as u32 }, al);
            }
            next
        });
        nedges[tid].get().0 = ne;
    });
    lap.lap("links");

    let lnodes: Vec<Shared<Vec<u64>>> = (0..NB).map(|_| Shared::new(Vec::new())).collect();
    let edge_end: Vec<Shared<Vec<u32>>> = (0..nt).map(|t| Shared::new(vec![0u32; nedges[t].get().0 as usize * 2])).collect();
    parallel_for(NB, nt, |b, _| {
        thread_local! { static TAB: std::cell::RefCell<Vec<u32>> = const { std::cell::RefCell::new(Vec::new()) }; }
        TAB.with(|tab| {
            let mut tab = tab.borrow_mut();
            let tot: usize = (0..nt).map(|t| lkb[t].get()[b].cnt).sum();
            tab.clear();
            tab.resize(pow2ge(tot as u64 * 2 + 1) as usize, 0);
            let mask = tab.len() as u64 - 1;
            let nodes = lnodes[b].get();
            nodes.reserve(tot);
            for t in 0..nt {
                let ee = edge_end[t].get().as_mut_ptr(); // distinct slots per bucket task
                lkb[t].get()[b].each(|s| {
                    let mut i = khash(s.k) & mask;
                    let local;
                    loop {
                        let v = tab[i as usize];
                        if v == 0 {
                            nodes.push(s.k);
                            tab[i as usize] = nodes.len() as u32;
                            local = nodes.len() as u32 - 1;
                            break;
                        }
                        if nodes[v as usize - 1] == s.k {
                            local = v - 1;
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                    if local >= 1 << LBITS {
                        die("link bucket too large");
                    }
                    unsafe { *ee.add(s.slot as usize) = ((b as u32) << LBITS) | local };
                });
            }
        });
    });
    for a in &ar_l {
        a.get().release();
    }
    lap.lap("intern");

    // Phases 1 and 2 run while union-find runs on its own thread.
    let (ltabs, lmasks, evb, svb, clb, dead, loc_ib) = std::thread::scope(|sc| {
        let lnodes = &lnodes;
        let edge_end = edge_end;
        let uf = sc.spawn(move || {
            // Union-find; every linked ID's person name (the root, the
            // byte-wise smallest ID) goes into a small table per bucket.
            let mut base = vec![0u32; NB + 1];
            for b in 0..NB {
                base[b + 1] = base[b] + lnodes[b].get().len() as u32;
            }
            let nn = base[NB] as usize;
            let mut par: Vec<u32> = (0..nn as u32).collect();
            let mut key = vec![0u64; nn];
            for b in 0..NB {
                for (i, &k) in lnodes[b].get().iter().enumerate() {
                    key[base[b] as usize + i] = k;
                }
            }
            let gid = |packed: u32| base[(packed >> LBITS) as usize] + (packed & ((1 << LBITS) - 1));
            fn find(par: &mut [u32], mut x: u32) -> u32 {
                while par[x as usize] != x {
                    par[x as usize] = par[par[x as usize] as usize];
                    x = par[x as usize];
                }
                x
            }
            for t in 0..nt {
                let e = edge_end[t].get();
                for i in (0..e.len()).step_by(2) {
                    let mut a = find(&mut par, gid(e[i]));
                    let mut b = find(&mut par, gid(e[i + 1]));
                    if a == b {
                        continue;
                    }
                    if key_less(key[b as usize], key[a as usize]) {
                        std::mem::swap(&mut a, &mut b);
                    }
                    par[b as usize] = a;
                }
            }
            let mut ltabs: Vec<Vec<(u64, u64)>> = Vec::with_capacity(NB);
            let mut lmasks = vec![0u64; NB];
            for b in 0..NB {
                let nodes = lnodes[b].get();
                let mut tt = vec![(0u64, 0u64); pow2ge(nodes.len() as u64 * 2 + 1) as usize];
                let mask = tt.len() as u64 - 1;
                lmasks[b] = mask;
                for (i, &k) in nodes.iter().enumerate() {
                    let mut s = khash(k) & mask;
                    while tt[s as usize].0 != 0 {
                        s = (s + 1) & mask;
                    }
                    let r = find(&mut par, base[b] + i as u32);
                    tt[s as usize] = (k, key[r as usize]);
                }
                ltabs.push(tt);
            }
            (ltabs, lmasks)
        });

        // ---- phase 1: events and clicks
        let evb = make_buckets::<DEnt>(nt, NB);
        let svb = make_buckets::<SvEv>(nt, NB);
        let clb = make_buckets::<ClRec>(nt, NB);
        let ing0: i64 = ws / 1000 - (1i64 << 31); // ingest seconds origin (u32 range around the window)
        let mut loc_tb = 1u32;
        while (1usize << loc_tb) < nt {
            loc_tb += 1;
        }
        let loc_ib = 32 - 8 - loc_tb;
        let mut dc = data_chunks.clone();
        dc.sort_by(|a, b| (b.e - b.b).cmp(&(a.e - a.b))); // biggest first
        parallel_for(dc.len(), nt, |ci, tid| {
            let c = &dc[ci];
            let file = &files[c.file];
            let rd = readers[tid].get();
            if c.kind == 0 {
                let eb = evb[tid].get();
                let sb = svb[tid].get();
                let ae = ar_e[tid].get();
                let as_ = ar_s[tid].get();
                rd.run(c, file, |p, fe, eof| unsafe {
                    let mut f = [Fld::default(); 7];
                    let mut next = p;
                    let sr = split_line(p, fe, eof, &mut f, 7, &mut next);
                    if sr < 0 {
                        return std::ptr::null();
                    }
                    if sr == 0 || f[0].n == 0 {
                        return next;
                    }
                    let ek = make_key(f[0].p, f[0].n);
                    // ingest_ts is always valid F1 (whole seconds); anything else sorts last
                    let ing = parse_ts_fast(tst, f[3].p, f[3].n).unwrap_or(i64::MAX);
                    let is = if ing == i64::MAX { -1 } else { ing / 1000 - ing0 };
                    let mut de = DEnt { ek, ing: if !(0..=u32::MAX as i64).contains(&is) { u32::MAX } else { is as u32 }, loc: NOLOC };
                    let mut price = 0u32;
                    let mut ts = 0i64;
                    let mut ok = f[1].n > 0 && f[5].n > 0;
                    if ok {
                        match parse_ts_fast(tst, f[2].p, f[2].n) {
                            Some(t) if t >= ws && t < we => ts = t,
                            _ => ok = false,
                        }
                    }
                    if ok {
                        let t = f[4].bytes();
                        if t == b"view" || t == b"cart" {
                        } else if t == b"purchase" {
                            let q = f[6].bytes();
                            if q.is_empty() || !q.iter().all(|c| c.is_ascii_digit()) {
                                ok = false;
                            } else {
                                let mut i = 0;
                                while i < q.len() && q[i] == b'0' {
                                    i += 1;
                                }
                                if q.len() - i > 8 {
                                    ok = false;
                                } else {
                                    let v = q[i..].iter().fold(0u32, |v, &c| v * 10 + (c - b'0') as u32);
                                    if !(1..=10_000_000).contains(&v) {
                                        ok = false;
                                    }
                                    price = v;
                                }
                            }
                        } else {
                            ok = false;
                        }
                    }
                    if ok && ing - ts > DAY_MS {
                        ok = false; // late: strictly over 24 h
                    }
                    if ok {
                        let uk = make_key(f[1].p, f[1].n);
                        // shaped product IDs: one load from a dense number -> category table
                        let cat = match prod_num.num_buf(f[5].p, f[5].n) {
                            Some(v) => *cat_by_num.get(v as usize).unwrap_or(&(unknown_cat as u8)) as u32,
                            None => {
                                let pi = products.find(f[5].bytes());
                                if pi < 0 { unknown_cat } else { prod_cat[pi as usize] as u32 }
                            }
                        };
                        let b = (khash(uk) >> BSH) as usize;
                        let idx = sb[b].cnt;
                        if idx >= 1usize << loc_ib {
                            die("too many events in one user bucket");
                        }
                        de.loc = ((tid as u32) << (8 + loc_ib)) | ((b as u32) << loc_ib) | idx as u32;
                        sb[b].push(SvEv { uk, ts: (ts - ws) as u32, pc: price << 8 | cat }, as_);
                    }
                    eb[(khash(de.ek) >> BSH) as usize].push(de, ae);
                    next
                });
            } else {
                let cb = clb[tid].get();
                let ac = ar_c[tid].get();
                rd.run(c, file, |p, fe, eof| unsafe {
                    let mut f = [Fld::default(); 4];
                    let mut next = p;
                    let sr = split_line(p, fe, eof, &mut f, 4, &mut next);
                    if sr < 0 {
                        return std::ptr::null();
                    }
                    if sr == 0 || f[0].n == 0 || f[1].n == 0 {
                        return next;
                    }
                    let camp = match camp_num.num_buf(f[2].p, f[2].n) {
                        Some(v) => *camp_by_num.get(v as usize).unwrap_or(&-1),
                        None => campaigns.find(f[2].bytes()),
                    };
                    if camp < 0 {
                        return next;
                    }
                    let ts = match parse_ts_fast(tst, f[3].p, f[3].n) {
                        Some(t) if t >= cws && t < we => t,
                        _ => return next,
                    };
                    let uk = make_key(f[1].p, f[1].n);
                    let ck = make_key(f[0].p, f[0].n);
                    cb[(khash(uk) >> BSH) as usize].push(ClRec { uk, ck, ts: (ts - cws) as u32, camp: camp as u16 }, ac);
                    next
                });
            }
        });
        lap.lap("parse");

        // ---- phase 2: dedup. The earliest ingest_ts wins; every other copy is
        // marked dead (dedup comes before validation, so an invalid winner
        // kills valid later copies too). Equal ingest_ts copies are identical.
        let dead: Vec<Vec<Vec<AtomicU64>>> = (0..nt)
            .map(|t| (0..NB).map(|b| (0..svb[t].get()[b].cnt / 64 + 1).map(|_| AtomicU64::new(0)).collect()).collect())
            .collect();
        let kill = |loc: u32| {
            if loc == NOLOC {
                return;
            }
            let t = (loc >> (8 + loc_ib)) as usize;
            let b = ((loc >> loc_ib) & 255) as usize;
            let i = (loc & ((1 << loc_ib) - 1)) as usize;
            dead[t][b][i >> 6].fetch_or(1 << (i & 63), Ordering::Relaxed);
        };
        parallel_for(NB, nt, |b, _| {
            thread_local! { static TAB: std::cell::RefCell<Vec<DEnt>> = const { std::cell::RefCell::new(Vec::new()) }; }
            TAB.with(|tab| {
                let mut tab = tab.borrow_mut();
                let tot: usize = (0..nt).map(|t| evb[t].get()[b].cnt).sum();
                let cap = pow2ge((tot + tot / 2 + 1) as u64) as usize;
                tab.clear();
                tab.resize(cap, DEnt { ek: 0, ing: 0, loc: 0 }); // ek == 0: empty (no key is 0)
                let mask = cap as u64 - 1;
                for t in 0..nt {
                    evb[t].get()[b].each(|r| {
                        let mut i = (khash(r.ek) >> 8) & mask;
                        loop {
                            let x = unsafe { tab.get_unchecked_mut(i as usize) };
                            if x.ek == 0 {
                                *x = *r;
                                return;
                            }
                            if x.ek == r.ek {
                                if r.ing < x.ing {
                                    kill(x.loc);
                                    x.ing = r.ing;
                                    x.loc = r.loc;
                                } else {
                                    kill(r.loc);
                                }
                                return;
                            }
                            i = (i + 1) & mask;
                        }
                    });
                }
            });
        });
        for a in &ar_e {
            a.get().release();
        }
        lap.lap("dedup");
        let (ltabs, lmasks) = uf.join().unwrap();
        lap.lap("unionfind");
        (ltabs, lmasks, evb, svb, clb, dead, loc_ib)
    });
    let _ = (&evb, loc_ib);

    // ---- phase 3: the person of every event and click
    let peb = make_buckets::<PEv>(nt, NB);
    let pcb = make_buckets::<PCl>(nt, NB);
    parallel_for(NB, nt, |b, tid| {
        let tt = &ltabs[b];
        let mask = lmasks[b];
        let person = |k: u64| -> u64 {
            let mut i = khash(k) & mask;
            loop {
                let e = unsafe { tt.get_unchecked(i as usize) };
                if e.0 == k {
                    return e.1;
                }
                if e.0 == 0 {
                    return k;
                }
                i = (i + 1) & mask;
            }
        };
        let pe = peb[tid].get();
        let pc = pcb[tid].get();
        let ap = ar_p[tid].get();
        for t in 0..nt {
            let dbits = &dead[t][b];
            let mut idx = 0usize;
            svb[t].get()[b].consume(|e| {
                let i = idx;
                idx += 1;
                if (dbits[i >> 6].load(Ordering::Relaxed) >> (i & 63)) & 1 != 0 {
                    return;
                }
                let pk = person(e.uk);
                pe[(khash(pk) >> BSH) as usize].push(PEv { nk: pk, ts: e.ts, pc: e.pc }, ap);
            });
            clb[t].get()[b].consume(|c| {
                let pk = person(c.uk);
                pc[(khash(pk) >> BSH) as usize].push(PCl { nk: pk, ck: c.ck, ts: c.ts, camp: c.camp }, ap);
            });
        }
    });
    for a in ar_s.iter().chain(ar_c.iter()) {
        a.get().release();
    }
    lap.lap("person");

    // ---- phase 4: sessions per person bucket; user_daily is written as each
    // bucket finishes (one row group per bucket, small reused buffers)
    let ud = format!("{out}/user_daily");
    let cdd = format!("{out}/campaign_daily");
    let _ = std::fs::create_dir_all(&ud);
    let _ = std::fs::create_dir_all(&cdd);
    let ucols = vec![
        ("user_id", Kind::Str),
        ("day", Kind::Date),
        ("sessions", Kind::I32),
        ("events", Kind::I32),
        ("revenue_cents", Kind::I64),
        ("top_category", Kind::OptStr),
    ];
    let writers: Vec<Shared<Option<PqWriter>>> = per_thread(nt, || None);
    let ncd = (ncamp + 1) * ndays; // campaign NCAMP = organic
    let cdt: Vec<Shared<Vec<CD>>> = per_thread(nt, || vec![CD::default(); ncd]);
    let scr = per_thread(nt, || Scratch::new(ncamp, ncat, ndays));
    parallel_for(NB, nt, |b, tid| {
        let s = scr[tid].get();
        let tot: usize = (0..nt).map(|t| peb[t].get()[b].cnt).sum();
        let totc: usize = (0..nt).map(|t| pcb[t].get()[b].cnt).sum();
        if tot == 0 {
            return;
        }
        s.group(b, nt, tot, totc, &peb, &pcb);
        let cdt = cdt[tid].get();
        s.sessions(ws, ndays, day0, ncamp, &cat_rank, cdt);
        if !s.ur.is_empty() {
            s.encode(&cat_names);
            let w = writers[tid].get();
            if w.is_none() {
                *w = Some(PqWriter::open(&format!("{ud}/part-{tid}.parquet"), ucols.clone()));
            }
            w.as_mut().unwrap().row_group(&s.cb, s.ur.len() as i64);
            s.ur.clear();
        }
    });
    let mut any = false;
    for w in writers.into_iter() {
        if let Some(w) = w.into_inner() {
            w.close();
            any = true;
        }
    }
    if !any {
        PqWriter::open(&format!("{ud}/part-0.parquet"), ucols.clone()).close(); // no rows: one empty file
    }
    lap.lap("sessions");

    // ---- phase 5: campaign_daily
    {
        let mut cb: Vec<ColBuf> = (0..7).map(|_| ColBuf::default()).collect();
        let mut rows = 0i64;
        for c in 0..=ncamp {
            for d in 0..ndays {
                let mut s = CD::default();
                for t in 0..nt {
                    let x = cdt[t].get()[c * ndays + d];
                    s.sessions += x.sessions;
                    s.purch += x.purch;
                    s.rev += x.rev;
                    s.assisted += x.assisted;
                }
                if s.sessions == 0 && s.assisted == 0 {
                    continue;
                }
                let (id, chn): (&[u8], &[u8]) = if c == ncamp { (&b"organic"[..], &b"organic"[..]) } else { (camp_ids[c].as_slice(), camp_chan[c].as_slice()) };
                cb[0].put_str(id);
                cb[1].put_str(chn);
                cb[2].put_i32(day0 + d as i32);
                cb[3].put_i64(s.sessions);
                cb[4].put_i64(s.purch);
                cb[5].put_i64(s.rev);
                cb[6].put_i64(s.assisted);
                rows += 1;
            }
        }
        let mut w = PqWriter::open(
            &format!("{cdd}/part-0.parquet"),
            vec![
                ("campaign_id", Kind::Str),
                ("channel", Kind::Str),
                ("day", Kind::Date),
                ("sessions", Kind::I64),
                ("purchasing_sessions", Kind::I64),
                ("revenue_cents", Kind::I64),
                ("assisted_sessions", Kind::I64),
            ],
        );
        w.row_group(&cb, rows);
        w.close();
    }
    lap.lap("write");
    unsafe { sys::_exit(0) } // skip destructors: the OS reclaims the rest
}

// ------------------------------------------------------------------ sessions
#[derive(Clone, Copy)]
struct E {
    ts: u32,
    pc: u32,
}
#[derive(Clone, Copy)]
struct C {
    ck: u64,
    ts: u32,
    camp: u16,
}
#[derive(Clone, Copy)]
struct S {
    start: u32,
    day: i32,
    rev: i64,
    purch: u8,
}
#[derive(Clone, Copy)]
struct EP {
    ts: u32,
    pc: u32,
    pid: u32,
}
/// Per-thread scratch for the sessions phase, reused across buckets.
struct Scratch {
    tab: Vec<(u64, u32)>,
    pkeys: Vec<u64>,
    ecount: Vec<u32>,
    ccount: Vec<u32>,
    eidx: Vec<u32>,
    cidx: Vec<u32>,
    eoff: Vec<u32>,
    coff: Vec<u32>,
    pos: Vec<u32>,
    ev: Vec<E>,
    cl: Vec<C>,
    ss: Vec<S>,
    cnt: Vec<i32>,
    act: Vec<i32>,
    touched_f: Vec<u8>,
    touched: Vec<u16>,
    cattouch: Vec<u16>,
    catsum: Vec<i64>,
    fd: Vec<u32>,
    ra: Vec<EP>,
    rb: Vec<EP>,
    hist: Vec<u32>,
    ur: Vec<UserRow>,
    cb: Vec<ColBuf>,
}
impl Scratch {
    fn new(ncamp: usize, ncat: usize, ndays: usize) -> Self {
        Scratch {
            tab: Vec::new(),
            pkeys: Vec::new(),
            ecount: Vec::new(),
            ccount: Vec::new(),
            eidx: Vec::new(),
            cidx: Vec::new(),
            eoff: Vec::new(),
            coff: Vec::new(),
            pos: Vec::new(),
            ev: Vec::new(),
            cl: Vec::new(),
            ss: Vec::new(),
            cnt: vec![0; ncamp],
            act: vec![-1; ncamp],
            touched_f: vec![0; ncamp],
            touched: Vec::new(),
            cattouch: Vec::new(),
            catsum: vec![0; ncat],
            fd: vec![0; ndays + 1],
            ra: Vec::new(),
            rb: Vec::new(),
            hist: vec![0; 3 * 2048],
            ur: Vec::new(),
            cb: (0..6).map(|_| ColBuf::default()).collect(),
        }
    }

    #[inline(always)]
    fn slot(k: u64) -> u64 {
        khash(k) >> 8
    }
    fn grow(&mut self) {
        let n = self.tab.len() * 2;
        self.tab.clear();
        self.tab.resize(n, (0, 0));
        let mask = n as u64 - 1;
        for (id, &k) in self.pkeys.iter().enumerate() {
            let mut i = Self::slot(k) & mask;
            while self.tab[i as usize].0 != 0 {
                i = (i + 1) & mask;
            }
            self.tab[i as usize] = (k, id as u32);
        }
    }
    /// index of person k in this bucket; inserted when ins
    #[inline(always)]
    fn lookup(&mut self, k: u64, ins: bool) -> u32 {
        let mut mask = self.tab.len() as u64 - 1;
        let mut i = Self::slot(k) & mask;
        loop {
            let x = unsafe { *self.tab.get_unchecked(i as usize) };
            if x.0 == k {
                return x.1;
            }
            if x.0 == 0 {
                if !ins {
                    return u32::MAX;
                }
                let id = self.ecount.len() as u32;
                self.tab[i as usize] = (k, id);
                self.ecount.push(0);
                self.pkeys.push(k);
                if self.pkeys.len() * 2 > self.tab.len() {
                    self.grow();
                    mask = self.tab.len() as u64 - 1;
                    let _ = mask;
                }
                return id;
            }
            i = (i + 1) & mask;
        }
    }

    /// Group bucket b's events and clicks by person; events in ts order.
    fn group(&mut self, b: usize, nt: usize, tot: usize, totc: usize, peb: &sys::Buckets<PEv>, pcb: &sys::Buckets<PCl>) {
        self.tab.clear();
        self.tab.resize(pow2ge((tot / 16 + 64) as u64) as usize, (0, 0)); // grown at load 1/2
        self.ecount.clear();
        self.pkeys.clear();
        self.eidx.resize(tot, 0);
        self.cidx.resize(totc, 0);
        let mut k = 0usize;
        for t in 0..nt {
            peb[t].get()[b].each(|e| {
                let id = self.lookup(e.nk, true);
                self.ecount[id as usize] += 1;
                self.eidx[k] = id;
                k += 1;
            });
        }
        let np = self.ecount.len();
        self.ccount.clear();
        self.ccount.resize(np, 0);
        k = 0;
        for t in 0..nt {
            pcb[t].get()[b].each(|c| {
                let id = self.lookup(c.nk, false);
                self.cidx[k] = id;
                k += 1;
                if id != u32::MAX {
                    self.ccount[id as usize] += 1;
                }
            });
        }
        self.eoff.clear();
        self.eoff.resize(np + 1, 0);
        self.coff.clear();
        self.coff.resize(np + 1, 0);
        for i in 0..np {
            self.eoff[i + 1] = self.eoff[i] + self.ecount[i];
            self.coff[i + 1] = self.coff[i] + self.ccount[i];
        }
        self.ev.resize(tot, E { ts: 0, pc: 0 });
        self.cl.resize(self.coff[np] as usize, C { ck: 0, ts: 0, camp: 0 });
        // LSD radix sort of the bucket on ts (31 bits, three 11-bit digits),
        // then a stable scatter by person
        self.ra.resize(tot, EP { ts: 0, pc: 0, pid: 0 });
        self.rb.resize(tot, EP { ts: 0, pc: 0, pid: 0 });
        self.hist.iter_mut().for_each(|h| *h = 0);
        k = 0;
        {
            let (ra, hist, eidx) = (&mut self.ra, &mut self.hist, &self.eidx);
            for t in 0..nt {
                peb[t].get_mut_list(b).consume(|e| unsafe {
                    *ra.get_unchecked_mut(k) = EP { ts: e.ts, pc: e.pc, pid: *eidx.get_unchecked(k) };
                    *hist.get_unchecked_mut((e.ts & 2047) as usize) += 1;
                    *hist.get_unchecked_mut(2048 + ((e.ts >> 11) & 2047) as usize) += 1;
                    *hist.get_unchecked_mut(4096 + (e.ts >> 22) as usize) += 1;
                    k += 1;
                });
            }
        }
        let mut src = self.ra.as_mut_ptr();
        let mut dst = self.rb.as_mut_ptr();
        for d in 0..3 {
            let h = &mut self.hist[d * 2048..(d + 1) * 2048];
            let mut sum = 0u32;
            for x in h.iter_mut() {
                let c = *x;
                *x = sum;
                sum += c;
            }
            let sh = d * 11;
            unsafe {
                for i in 0..tot {
                    let x = *src.add(i);
                    let slot = h.get_unchecked_mut(((x.ts >> sh) & 2047) as usize);
                    *dst.add(*slot as usize) = x;
                    *slot += 1;
                }
            }
            std::mem::swap(&mut src, &mut dst);
        }
        self.pos.clear();
        self.pos.extend_from_slice(&self.eoff[..np]);
        unsafe {
            for i in 0..tot {
                let x = *src.add(i);
                let p = self.pos.get_unchecked_mut(x.pid as usize);
                *self.ev.get_unchecked_mut(*p as usize) = E { ts: x.ts, pc: x.pc };
                *p += 1;
            }
        }
        self.pos.clear();
        self.pos.extend_from_slice(&self.coff[..np]);
        k = 0;
        let (cl, pos, cidx) = (&mut self.cl, &mut self.pos, &self.cidx);
        for t in 0..nt {
            pcb[t].get_mut_list(b).consume(|c| {
                let id = cidx[k];
                k += 1;
                if id != u32::MAX {
                    cl[pos[id as usize] as usize] = C { ck: c.ck, ts: c.ts, camp: c.camp };
                    pos[id as usize] += 1;
                }
            });
        }
    }

    /// Sessionize every person of the grouped bucket: user_daily rows into
    /// self.ur, campaign_daily counts into cdt.
    fn sessions(&mut self, ws: i64, ndays: usize, day0: i32, ncamp: usize, cat_rank: &[i32], cdt: &mut [CD]) {
        let np = self.ecount.len();
        for pi in 0..np {
            let (eb, ee) = (self.eoff[pi] as usize, self.eoff[pi + 1] as usize);
            let (cb, ce) = (self.coff[pi] as usize, self.coff[pi + 1] as usize);
            let nc = ce - cb;
            if nc > 1 {
                self.cl[cb..ce].sort_unstable_by(|a, c| {
                    if a.ts != c.ts {
                        a.ts.cmp(&c.ts)
                    } else if key_less(a.ck, c.ck) {
                        std::cmp::Ordering::Less
                    } else if key_less(c.ck, a.ck) {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Equal
                    }
                });
            }
            let pk = self.pkeys[pi];

            // sessionize + user_daily
            self.ss.clear();
            let mut cur_day = i32::MIN;
            let (mut d_sess, mut d_ev, mut d_rev) = (0u32, 0u32, 0i64);
            let mut prev = 0u32;
            for i in eb..ee {
                let x = self.ev[i];
                if i == eb || x.ts - prev > GAP_MS as u32 {
                    // equal ts never splits
                    let day = ((ws + x.ts as i64) / DAY_MS) as i32;
                    self.ss.push(S { start: x.ts, day, rev: 0, purch: 0 });
                    if day != cur_day {
                        self.flush(pk, cur_day, d_sess, d_ev, d_rev, cat_rank);
                        cur_day = day;
                        d_sess = 0;
                        d_ev = 0;
                        d_rev = 0;
                    }
                    d_sess += 1;
                }
                prev = x.ts;
                d_ev += 1;
                let price = x.pc >> 8;
                if price != 0 {
                    let cat = (x.pc & 255) as usize;
                    let s = self.ss.last_mut().unwrap();
                    s.rev += price as i64;
                    s.purch = 1;
                    d_rev += price as i64;
                    if self.catsum[cat] == 0 {
                        self.cattouch.push(cat as u16);
                    }
                    self.catsum[cat] += price as i64;
                }
            }
            self.flush(pk, cur_day, d_sess, d_ev, d_rev, cat_rank);

            // credit and assists
            let m = self.ss.len();
            if nc == 0 {
                for s in &self.ss {
                    let d = &mut cdt[ncamp * ndays + (s.day - day0) as usize];
                    d.sessions += 1;
                    d.purch += s.purch as i64;
                    d.rev += s.rev;
                }
                continue;
            }
            {
                let mut j = 0usize;
                for d in 0..=ndays {
                    while j < m && ((self.ss[j].day - day0) as usize) < d {
                        j += 1;
                    }
                    self.fd[d] = j as u32;
                }
            }
            let (ss, fd) = (&self.ss, &self.fd);
            let add_range = |cdt: &mut [CD], camp: usize, i: u32, j: u32| {
                // campaign assisted sessions [i, j)
                if i >= j {
                    return;
                }
                let da = (ss[i as usize].day - day0) as usize;
                let db = (ss[j as usize - 1].day - day0) as usize;
                for d in da..=db {
                    let lo = i.max(fd[d]);
                    let hi = j.min(fd[d + 1]);
                    if hi > lo {
                        cdt[camp * ndays + d].assisted += (hi - lo) as i64;
                    }
                }
            };
            // sliding window of the clicks with start - 7d <= ts <= start
            let c0 = &self.cl[cb..ce];
            let (mut lo, mut hi) = (0usize, 0usize);
            for si in 0..m {
                let s = ss[si];
                let sc = s.start + WEEK_MS as u32; // session start in the click frame
                while hi < nc && c0[hi].ts <= sc {
                    let cp = c0[hi].camp as usize;
                    self.cnt[cp] += 1;
                    if self.touched_f[cp] == 0 {
                        self.touched_f[cp] = 1;
                        self.touched.push(cp as u16);
                    }
                    hi += 1;
                }
                while lo < hi && c0[lo].ts < s.start {
                    // ts < start - 7d
                    let cp = c0[lo].camp as usize;
                    self.cnt[cp] -= 1;
                    if self.touched_f[cp] == 0 {
                        self.touched_f[cp] = 1;
                        self.touched.push(cp as u16);
                    }
                    lo += 1;
                }
                for &cp in &self.touched {
                    let cp = cp as usize;
                    self.touched_f[cp] = 0;
                    if self.cnt[cp] > 0 && self.act[cp] < 0 {
                        self.act[cp] = si as i32;
                    } else if self.cnt[cp] == 0 && self.act[cp] >= 0 {
                        add_range(cdt, cp, self.act[cp] as u32, si as u32);
                        self.act[cp] = -1;
                    }
                }
                self.touched.clear();
                let mut camp = ncamp;
                if hi > lo {
                    // latest ts; ties: smallest click_id (sorted first)
                    let mut j = hi - 1;
                    let t = c0[j].ts;
                    while j > lo && c0[j - 1].ts == t {
                        j -= 1;
                    }
                    camp = c0[j].camp as usize;
                }
                let d = &mut cdt[camp * ndays + (s.day - day0) as usize];
                d.sessions += 1;
                d.purch += s.purch as i64;
                d.rev += s.rev;
            }
            for c in &c0[lo..hi] {
                let cp = c.camp as usize;
                if self.act[cp] >= 0 {
                    add_range(cdt, cp, self.act[cp] as u32, m as u32);
                    self.act[cp] = -1;
                }
                self.cnt[cp] = 0;
            }
        }
    }

    #[inline(always)]
    fn flush(&mut self, pk: u64, day: i32, sess: u32, ev: u32, rev: i64, cat_rank: &[i32]) {
        if day == i32::MIN {
            return;
        }
        let mut top = -1i32;
        let mut best = 0i64;
        for &c in &self.cattouch {
            let c = c as usize;
            let v = self.catsum[c];
            if top < 0 || v > best || (v == best && cat_rank[c] < cat_rank[top as usize]) {
                top = c as i32;
                best = v;
            }
            self.catsum[c] = 0;
        }
        self.cattouch.clear();
        self.ur.push(UserRow { nk: pk, day, sessions: sess, events: ev, rev, top });
    }

    /// user_daily rows of this bucket into the column buffers
    fn encode(&mut self, cat_names: &[Vec<u8>]) {
        for c in &mut self.cb {
            c.data.clear();
        }
        self.cb[5].defs.clear();
        self.cb[5].defs.resize(self.ur.len(), 0);
        let mut name = Vec::with_capacity(16);
        for (i, r) in self.ur.iter().enumerate() {
            key_bytes(r.nk, &mut name);
            self.cb[0].put_str(&name);
            self.cb[1].put_i32(r.day);
            self.cb[2].put_i32(r.sessions as i32);
            self.cb[3].put_i32(r.events as i32);
            self.cb[4].put_i64(r.rev);
            if r.top >= 0 {
                self.cb[5].defs[i] = 1;
                let n = &cat_names[r.top as usize];
                self.cb[5].put_str(n);
            }
        }
    }
}

trait ListAt<T> {
    fn get_mut_list(&self, b: usize) -> &mut BList<T>;
}
impl<T: Copy> ListAt<T> for Shared<Vec<BList<T>>> {
    #[inline(always)]
    fn get_mut_list(&self, b: usize) -> &mut BList<T> {
        &mut self.get()[b]
    }
}
