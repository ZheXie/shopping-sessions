//! High-performance Shopping Sessions pipeline.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod csvlex;
mod intern;
mod ts;
mod uf;

use std::fs::{self, File};
use std::time::Instant;
use std::path::{Path, PathBuf};

use arrow::array::{Date32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

use crate::csvlex::{data_lines, parse_line_n};
use crate::intern::Interner;
use crate::ts::{parse_ts, utc_day};
use crate::uf::UnionFind;

const DAY_MS: i64 = 86_400_000;
const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;
const GAP_SPLIT_MS: i64 = 1_800_000;

fn env_path(key: &str, default: &str) -> PathBuf {
    PathBuf::from(std::env::var(key).unwrap_or_else(|_| default.to_string()))
}

fn leak_mmap(path: &Path) -> &'static [u8] {
    let f = File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let mmap = unsafe { Mmap::map(&f).unwrap_or_else(|e| panic!("mmap {}: {e}", path.display())) };
    Box::leak(Box::new(mmap))
}

fn list_csvs(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|_| panic!("read_dir {}", dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false))
        .collect();
    v.sort();
    v
}

#[inline]
fn is_ascii_digits(s: &[u8]) -> bool {
    !s.is_empty() && s.iter().all(|c| c.is_ascii_digit())
}

fn load_manifest(data: &Path) -> (i64, i64) {
    let raw = fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let start = raw
        .split("\"window_start\"")
        .nth(1)
        .and_then(|s| s.split('"').nth(1))
        .expect("window_start");
    let end = raw
        .split("\"window_end\"")
        .nth(1)
        .and_then(|s| s.split('"').nth(1))
        .expect("window_end");
    (
        parse_ts(start.as_bytes()).expect("bad window_start"),
        parse_ts(end.as_bytes()).expect("bad window_end"),
    )
}

fn load_products(data: &Path, inter: &mut Interner) -> FxHashMap<u32, u32> {
    let bytes = leak_mmap(&data.join("products.csv"));
    let mut map = FxHashMap::default();
    for line in data_lines(bytes) {
        let Some(fields) = parse_line_n::<2>(line) else {
            continue;
        };
        if fields[0].is_empty() {
            continue;
        }
        let pid = inter.intern(fields[0]);
        let cat = if fields[1].is_empty() {
            inter.intern(b"unknown")
        } else {
            inter.intern(fields[1])
        };
        map.insert(pid, cat);
    }
    map
}

fn load_campaigns(data: &Path, inter: &mut Interner) -> FxHashMap<u32, u32> {
    let bytes = leak_mmap(&data.join("campaigns.csv"));
    let mut map = FxHashMap::default();
    for line in data_lines(bytes) {
        let Some(fields) = parse_line_n::<2>(line) else {
            continue;
        };
        if fields[0].is_empty() {
            continue;
        }
        map.insert(inter.intern(fields[0]), inter.intern(fields[1]));
    }
    map
}

/// Deduped event candidate (fields borrowed from leaked mmap).
struct EventCand {
    ingest: i64,
    /// Parsed ts if valid form, else None — form-checked at parse; window later.
    ts: Option<i64>,
    user_id: &'static [u8],
    etype: &'static [u8],
    product_id: &'static [u8],
    price: &'static [u8],
}

fn parse_event_file(path: &Path) -> FxHashMap<&'static [u8], EventCand> {
    let bytes = leak_mmap(path);
    let mut best: FxHashMap<&'static [u8], EventCand> =
        FxHashMap::with_capacity_and_hasher(4096, Default::default());
    for line in data_lines(bytes) {
        let Some(f) = parse_line_n::<7>(line) else {
            continue;
        };
        if f[0].is_empty() {
            continue;
        }
        let Some(ingest) = parse_ts(f[3]) else {
            continue;
        };
        let eid = f[0];
        let replace = match best.get(eid) {
            None => true,
            Some(prev) => ingest < prev.ingest,
        };
        if replace {
            best.insert(
                eid,
                EventCand {
                    ingest,
                    ts: parse_ts(f[2]),
                    user_id: f[1],
                    etype: f[4],
                    product_id: f[5],
                    price: f[6],
                },
            );
        }
    }
    best
}


#[derive(Clone, Copy)]
struct SurvEvent {
    person: u32,
    ts: i64,
    etype: u8, // 0 view 1 cart 2 purchase
    price: i32,
    category: u32,
}

fn validate_events(
    cands: FxHashMap<&'static [u8], EventCand>,
    w0: i64,
    w1: i64,
    products: &FxHashMap<u32, u32>,
    unknown_cat: u32,
    inter: &mut Interner,
    uf: &mut UnionFind,
) -> Vec<SurvEvent> {
    // Parallel structural validation (no interning).
    let vals: Vec<EventCand> = cands.into_values().collect();
    let pre: Vec<( &'static [u8], i64, u8, i32, &'static [u8])> = vals
        .into_par_iter()
        .filter_map(|e| {
            if e.user_id.is_empty() {
                return None;
            }
            let ts = e.ts?;
            if ts < w0 || ts >= w1 {
                return None;
            }
            let etype = match e.etype {
                b"view" => 0u8,
                b"cart" => 1,
                b"purchase" => 2,
                _ => return None,
            };
            if e.product_id.is_empty() {
                return None;
            }
            let mut price = 0i32;
            if etype == 2 {
                if !is_ascii_digits(e.price) {
                    return None;
                }
                let mut v: i64 = 0;
                for &c in e.price {
                    v = v * 10 + (c - b'0') as i64;
                }
                if v < 1 || v > 10_000_000 {
                    return None;
                }
                price = v as i32;
            }
            if e.ingest - ts > DAY_MS {
                return None;
            }
            Some((e.user_id, ts, etype, price, e.product_id))
        })
        .collect();

    let mut out = Vec::with_capacity(pre.len());
    for (user, ts, etype, price, product_id) in pre {
        let uid = inter.intern(user);
        let person = uf.person(uid);
        let pid = inter.intern(product_id);
        let cat = products.get(&pid).copied().unwrap_or(unknown_cat);
        out.push(SurvEvent {
            person,
            ts,
            etype,
            price,
            category: cat,
        });
    }
    out
}


struct Session {
    person: u32,
    start: i64,
    day: i32,
    events: u32,
    revenue: i64,
    has_purchase: bool,
    cat_rev: FxHashMap<u32, i64>,
    campaign: u32,
    channel: u32,
    assists: Vec<u32>,
}

fn sessionize(mut events: Vec<SurvEvent>) -> Vec<Session> {
    events.par_sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
    let mut sessions = Vec::with_capacity(events.len() / 3 + 1);
    let mut i = 0;
    while i < events.len() {
        let person = events[i].person;
        let mut j = i + 1;
        while j < events.len() && events[j].person == person {
            j += 1;
        }
        // Single pass: gap measured between distinct timestamps.
        let slice = &events[i..j];
        let mut idx = 0usize;
        while idx < slice.len() {
            let start = slice[idx].ts;
            let mut prev_ts = start;
            let mut nev = 0u32;
            let mut revenue = 0i64;
            let mut has_purchase = false;
            let mut cat_rev: FxHashMap<u32, i64> = FxHashMap::default();
            while idx < slice.len() {
                let e = &slice[idx];
                if e.ts != prev_ts {
                    if e.ts - prev_ts > GAP_SPLIT_MS {
                        break;
                    }
                    prev_ts = e.ts;
                }
                nev += 1;
                if e.etype == 2 {
                    has_purchase = true;
                    revenue += e.price as i64;
                    *cat_rev.entry(e.category).or_insert(0) += e.price as i64;
                }
                idx += 1;
            }
            sessions.push(Session {
                person,
                start,
                day: utc_day(start),
                events: nev,
                revenue,
                has_purchase,
                cat_rev,
                campaign: 0,
                channel: 0,
                assists: Vec::new(),
            });
        }
        i = j;
    }
    sessions
}


struct Click {
    click_id: &'static [u8],
    person: u32,
    campaign: u32,
    channel: u32,
    ts: i64,
}

fn parse_click_file(path: &Path) -> FxHashMap<&'static [u8], [&'static [u8]; 3]> {
    let bytes = leak_mmap(path);
    let mut best: FxHashMap<&'static [u8], [&'static [u8]; 3]> = FxHashMap::default();
    for line in data_lines(bytes) {
        let Some(f) = parse_line_n::<4>(line) else {
            continue;
        };
        if f[0].is_empty() {
            continue;
        }
        best.entry(f[0]).or_insert([f[1], f[2], f[3]]);
    }
    best
}

fn build_clicks(
    maps: Vec<FxHashMap<&'static [u8], [&'static [u8]; 3]>>,
    w0: i64,
    w1: i64,
    campaigns: &FxHashMap<u32, u32>,
    inter: &mut Interner,
    uf: &mut UnionFind,
) -> Vec<Click> {
    let mut seen: FxHashMap<&'static [u8], [&'static [u8]; 3]> = FxHashMap::default();
    for m in maps {
        for (k, v) in m {
            seen.entry(k).or_insert(v);
        }
    }
    let cstart = w0 - SEVEN_DAYS_MS;
    let mut out = Vec::with_capacity(seen.len());
    for (click_id, [user, camp, ts_raw]) in seen {
        if user.is_empty() {
            continue;
        }
        let Some(ts) = parse_ts(ts_raw) else {
            continue;
        };
        if ts < cstart || ts >= w1 {
            continue;
        }
        let camp_id = inter.intern(camp);
        let Some(&channel) = campaigns.get(&camp_id) else {
            continue;
        };
        let uid = inter.intern(user);
        let person = uf.person(uid);
        out.push(Click {
            click_id,
            person,
            campaign: camp_id,
            channel,
            ts,
        });
    }
    out
}

fn attribute(sessions: &mut [Session], clicks: &mut [Click], organic: u32, organic_ch: u32) {
    clicks.par_sort_unstable_by(|a, b| {
        a.person
            .cmp(&b.person)
            .then(a.ts.cmp(&b.ts))
            .then(a.click_id.cmp(b.click_id))
    });
    // Sessions are grouped by person with starts ascending within person.
    let mut ci = 0usize;
    let mut i = 0usize;
    while i < sessions.len() {
        let person = sessions[i].person;
        let mut j = i + 1;
        while j < sessions.len() && sessions[j].person == person {
            j += 1;
        }
        // Advance clicks to this person
        while ci < clicks.len() && clicks[ci].person < person {
            ci += 1;
        }
        let c_start = ci;
        while ci < clicks.len() && clicks[ci].person == person {
            ci += 1;
        }
        let person_clicks = &clicks[c_start..ci];

        // Two-pointer over sessions (sorted by start) and clicks (sorted by ts)
        let mut lo = 0usize; // first click with ts >= start-7d
        for s in sessions[i..j].iter_mut() {
            let start = s.start;
            let min_ts = start - SEVEN_DAYS_MS;
            while lo < person_clicks.len() && person_clicks[lo].ts < min_ts {
                lo += 1;
            }
            let mut best: Option<usize> = None;
            let mut assists: Vec<u32> = Vec::new();
            let mut k = lo;
            while k < person_clicks.len() && person_clicks[k].ts <= start {
                let c = &person_clicks[k];
                assists.push(c.campaign);
                match best {
                    None => best = Some(k),
                    Some(bi) => {
                        let b = &person_clicks[bi];
                        if c.ts > b.ts || (c.ts == b.ts && c.click_id < b.click_id) {
                            best = Some(k);
                        }
                    }
                }
                k += 1;
            }
            match best {
                None => {
                    s.campaign = organic;
                    s.channel = organic_ch;
                    s.assists.clear();
                }
                Some(bi) => {
                    let c = &person_clicks[bi];
                    s.campaign = c.campaign;
                    s.channel = c.channel;
                    assists.sort_unstable();
                    assists.dedup();
                    s.assists = assists;
                }
            }
        }
        i = j;
    }
}

fn load_links(data: &Path, inter: &mut Interner, uf: &mut UnionFind) {
    let files = list_csvs(&data.join("links"));
    let pair_bytes: Vec<(&'static [u8], &'static [u8])> = files
        .par_iter()
        .map(|path| {
            let bytes = leak_mmap(path);
            let mut local = Vec::new();
            for line in data_lines(bytes) {
                let Some(f) = parse_line_n::<2>(line) else {
                    continue;
                };
                if f[0].is_empty() || f[1].is_empty() {
                    continue;
                }
                local.push((f[0], f[1]));
            }
            local
        })
        .flatten()
        .collect();
    let ids: Vec<(u32, u32)> = pair_bytes
        .iter()
        .map(|(a, b)| (inter.intern(a), inter.intern(b)))
        .collect();
    let bytes_table: Vec<Box<[u8]>> = (0..inter.len())
        .map(|i| inter.get(i as u32).to_vec().into_boxed_slice())
        .collect();
    for &(ia, ib) in &ids {
        uf.union_with(ia, ib, &bytes_table);
    }
}

fn write_parquet_user(path: &Path, rows: &[(u32, i32, i64, i64, i64, Option<u32>)], inter: &Interner) {
    let schema = ArrowSchema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]);
    let user_id = StringArray::from_iter_values(rows.iter().map(|r| std::str::from_utf8(inter.get(r.0)).unwrap()));
    let day = Date32Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>());
    let sessions = Int64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>());
    let events = Int64Array::from(rows.iter().map(|r| r.3).collect::<Vec<_>>());
    let revenue = Int64Array::from(rows.iter().map(|r| r.4).collect::<Vec<_>>());
    let top = StringArray::from(
        rows.iter()
            .map(|r| r.5.map(|id| std::str::from_utf8(inter.get(id)).unwrap()))
            .collect::<Vec<Option<&str>>>(),
    );
    let batch = RecordBatch::try_new(
        std::sync::Arc::new(schema.clone()),
        vec![
            std::sync::Arc::new(user_id),
            std::sync::Arc::new(day),
            std::sync::Arc::new(sessions),
            std::sync::Arc::new(events),
            std::sync::Arc::new(revenue),
            std::sync::Arc::new(top),
        ],
    )
    .unwrap();
    fs::create_dir_all(path).unwrap();
    let file = File::create(path.join("part-0.parquet")).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, std::sync::Arc::new(schema), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn write_parquet_campaign(path: &Path, rows: &[(u32, u32, i32, i64, i64, i64, i64)], inter: &Interner) {
    let schema = ArrowSchema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]);
    let c0 = StringArray::from_iter_values(rows.iter().map(|r| std::str::from_utf8(inter.get(r.0)).unwrap()));
    let c1 = StringArray::from_iter_values(rows.iter().map(|r| std::str::from_utf8(inter.get(r.1)).unwrap()));
    let day = Date32Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>());
    let s = Int64Array::from(rows.iter().map(|r| r.3).collect::<Vec<_>>());
    let ps = Int64Array::from(rows.iter().map(|r| r.4).collect::<Vec<_>>());
    let rev = Int64Array::from(rows.iter().map(|r| r.5).collect::<Vec<_>>());
    let a = Int64Array::from(rows.iter().map(|r| r.6).collect::<Vec<_>>());
    let batch = RecordBatch::try_new(
        std::sync::Arc::new(schema.clone()),
        vec![
            std::sync::Arc::new(c0),
            std::sync::Arc::new(c1),
            std::sync::Arc::new(day),
            std::sync::Arc::new(s),
            std::sync::Arc::new(ps),
            std::sync::Arc::new(rev),
            std::sync::Arc::new(a),
        ],
    )
    .unwrap();
    fs::create_dir_all(path).unwrap();
    let file = File::create(path.join("part-0.parquet")).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, std::sync::Arc::new(schema), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}




fn merge_event_maps_seq(
    mut maps: Vec<FxHashMap<&'static [u8], EventCand>>,
) -> FxHashMap<&'static [u8], EventCand> {
    if maps.is_empty() {
        return FxHashMap::default();
    }
    // Start from the largest shard to reduce rehashing.
    let mut best_i = 0usize;
    for (i, m) in maps.iter().enumerate() {
        if m.len() > maps[best_i].len() {
            best_i = i;
        }
    }
    let mut out = maps.swap_remove(best_i);
    for m in maps {
        for (k, v) in m {
            match out.get(&k) {
                Some(prev) if prev.ingest <= v.ingest => {}
                _ => {
                    out.insert(k, v);
                }
            }
        }
    }
    out
}


fn main() {
    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        });
    rayon::ThreadPoolBuilder::new()
        .num_threads(ncpu)
        .build_global()
        .ok();

    let data = env_path("DATA_DIR", "/data");
    let out = env_path("OUTPUT_DIR", "/output");

    let (w0, w1) = load_manifest(&data);
    eprintln!("window: {w0} .. {w1}; threads={ncpu}");

    let mut inter = Interner::with_capacity(1 << 20);
    let unknown_cat = inter.intern(b"unknown");
    let organic = inter.intern(b"organic");
    let organic_ch = organic;
    let products = load_products(&data, &mut inter);
    let campaigns = load_campaigns(&data, &mut inter);

    let timing = std::env::var("SS_TIMING").ok().as_deref() == Some("1");
    let mut uf = UnionFind::new();
    let t0 = Instant::now();
    load_links(&data, &mut inter, &mut uf);
    if timing { eprintln!("links {:?}", t0.elapsed()); }

    let t0 = Instant::now();
    let event_files = list_csvs(&data.join("events"));
    let event_maps: Vec<_> = event_files.par_iter().map(|p| parse_event_file(p)).collect();
    if timing { eprintln!("event_parse {:?}", t0.elapsed()); }
    let t0 = Instant::now();
    let merged = merge_event_maps_seq(event_maps);
    if timing { eprintln!("event_merge {:?} n={}", t0.elapsed(), merged.len()); }
    let t0 = Instant::now();
    let events = validate_events(merged, w0, w1, &products, unknown_cat, &mut inter, &mut uf);
    if timing { eprintln!("validate {:?} n={}", t0.elapsed(), events.len()); }
    let t0 = Instant::now();
    let mut sessions = sessionize(events);
    if timing { eprintln!("sessionize {:?} n={}", t0.elapsed(), sessions.len()); }

    let t0 = Instant::now();
    let click_files = list_csvs(&data.join("clicks"));
    let click_maps: Vec<_> = click_files.par_iter().map(|p| parse_click_file(p)).collect();
    let mut clicks = build_clicks(click_maps, w0, w1, &campaigns, &mut inter, &mut uf);
    if timing { eprintln!("clicks {:?}", t0.elapsed()); }
    let t0 = Instant::now();
    attribute(&mut sessions, &mut clicks, organic, organic_ch);
    if timing { eprintln!("attribute {:?}", t0.elapsed()); }

    #[derive(Default)]
    struct UAcc {
        sessions: i64,
        events: i64,
        revenue: i64,
        cat_rev: FxHashMap<u32, i64>,
    }
    let mut user_map: FxHashMap<(u32, i32), UAcc> = FxHashMap::default();
    for s in &sessions {
        let acc = user_map.entry((s.person, s.day)).or_default();
        acc.sessions += 1;
        acc.events += s.events as i64;
        acc.revenue += s.revenue;
        for (&cat, &rev) in &s.cat_rev {
            *acc.cat_rev.entry(cat).or_insert(0) += rev;
        }
    }
    let mut user_rows = Vec::with_capacity(user_map.len());
    for ((person, day), acc) in user_map {
        let top = if acc.cat_rev.is_empty() {
            None
        } else {
            let best = *acc.cat_rev.values().max().unwrap();
            let mut cats: Vec<u32> = acc
                .cat_rev
                .iter()
                .filter(|(_, &r)| r == best)
                .map(|(&c, _)| c)
                .collect();
            cats.sort_by(|&a, &b| inter.get(a).cmp(inter.get(b)));
            Some(cats[0])
        };
        user_rows.push((person, day, acc.sessions, acc.events, acc.revenue, top));
    }

    #[derive(Default)]
    struct CAcc {
        channel: u32,
        sessions: i64,
        purchasing: i64,
        revenue: i64,
        assisted: i64,
    }
    let mut camp_map: FxHashMap<(u32, i32), CAcc> = FxHashMap::default();
    for s in &sessions {
        let acc = camp_map.entry((s.campaign, s.day)).or_default();
        acc.channel = s.channel;
        acc.sessions += 1;
        if s.has_purchase {
            acc.purchasing += 1;
        }
        acc.revenue += s.revenue;
        for &aid in &s.assists {
            let a = camp_map.entry((aid, s.day)).or_default();
            if a.channel == 0 {
                if let Some(&ch) = campaigns.get(&aid) {
                    a.channel = ch;
                }
            }
            a.assisted += 1;
        }
    }
    for (&(cid, _), acc) in camp_map.iter_mut() {
        if cid == organic {
            acc.assisted = 0;
        }
        if acc.channel == 0 {
            if cid == organic {
                acc.channel = organic_ch;
            } else if let Some(&ch) = campaigns.get(&cid) {
                acc.channel = ch;
            }
        }
    }

    let mut camp_rows = Vec::with_capacity(camp_map.len());
    for ((cid, day), acc) in camp_map {
        if acc.sessions == 0 && acc.assisted == 0 {
            continue;
        }
        camp_rows.push((
            cid,
            acc.channel,
            day,
            acc.sessions,
            acc.purchasing,
            acc.revenue,
            if cid == organic { 0 } else { acc.assisted },
        ));
    }

    write_parquet_user(&out.join("user_daily"), &user_rows, &inter);
    write_parquet_campaign(&out.join("campaign_daily"), &camp_rows, &inter);
    eprintln!(
        "wrote user_daily={} campaign_daily={}",
        user_rows.len(),
        camp_rows.len()
    );
}
