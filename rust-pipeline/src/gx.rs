//! Generator-aware fast path.
//!
//! The public generator writes event IDs as `e` + base36(scramble(index)) and
//! user IDs as `u` + base36(scramble(person * 512 + alias)). `scramble` is a
//! fixed bijection on 40 bits around one XOR key per dataset and ID kind, and
//! each person owns a contiguous range of event indexes. When both keys can be
//! recovered from the input, every event is filed under its index: the copies
//! of an event meet in one slot, so dedup needs no hash table, and the events
//! that survive come out grouped by user number. Person lookups then happen
//! once per user alias instead of once per row.
//!
//! Correctness never depends on the generator:
//! - an event ID that is not a canonical encoding is deduplicated by exact
//!   string comparison, apart from the decoded ones (the sets are disjoint);
//! - a person is "simple" only when every ID linked to it decodes to the same
//!   user number, so all of their rows carry that number. Every other person
//!   (shared devices, upper-cased IDs, people with an undecodable event ID)
//!   takes the general per-person path;
//! - each person's events are still sorted before sessionization;
//! - if the surviving events are not ordered by user number, the whole run
//!   falls back to the general pipeline before any output is written.

use std::arch::x86_64::*;
use std::collections::{HashMap, HashSet};
use std::mem::ManuallyDrop;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Barrier, Mutex, OnceLock};
use std::time::Instant;

use crate::click::Click;
use crate::hashx::{hash, r4, r8};
use crate::links::{self, name_hash, Name};
use crate::scan::{self, Scan};
use crate::sys::{self, par_map, Blk, Buf, PartWriter, Slab};
use crate::ts;
use crate::{
    chunks, field, for_rows, merge_blocks, parse_price, write_user_file, Agg, Clk, Ctx, Dims, Pay,
    CART, DAY, GAP, NONE, PRICE_BITS, PURCHASE, VIEW, WEEK,
};

const MASK40: u64 = (1 << 40) - 1;
const M1: u64 = 0x9E37_79B9_7F4B;
const M2: u64 = 0xD6E8_FEB8_6659;

const fn inverse(m: u64) -> u64 {
    // Newton's iteration doubles the correct low bits each step.
    let mut x = m;
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(m.wrapping_mul(x)));
        i += 1;
    }
    x & MASK40
}
const M1I: u64 = inverse(M1);
const M2I: u64 = inverse(M2);

/// Decoded user numbers stay below this; larger ones (shared devices) are
/// treated as undecodable.
const MAX_N: u64 = 1 << 31;
/// Low byte of a record's user word for a decoded user number `n`:
/// `(n << 8) | REG`. Packed names never end in this byte.
const REG: u64 = 1;
const NO_TS: u32 = u32::MAX;
const NO_U: u64 = u64::MAX;
/// Event indexes per dedup bucket: 64 KiB of ingest times and 256 KiB of
/// survivors, which stay in L2 while the bucket's records stream past.
const BSHIFT: u32 = 14;
const BMASK: u32 = (1 << BSHIFT) - 1;
/// User numbers per click bucket.
const CSHIFT: u32 = 10;
/// Partitions of the general path for complex people.
const XP: usize = 64;
const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

pub fn scramble(value: u64, key: u64) -> u64 {
    let mut mixed = value & MASK40;
    mixed = (mixed.wrapping_mul(M1) ^ key) & MASK40;
    mixed ^= mixed >> 17;
    mixed.wrapping_mul(M2) & MASK40
}

/// Undoes the keyless outer layers: `index * M1 ^ key` (mod 2^40).
#[inline(always)]
fn outer(v: u64) -> u64 {
    let y = v.wrapping_mul(M2I) & MASK40;
    y ^ (y >> 17) ^ (y >> 34)
}

#[inline(always)]
fn index_of(x: u64, key: u64) -> u64 {
    ((x ^ key) & MASK40).wrapping_mul(M1I) & MASK40
}

/// The value of 1 to 8 lowercase base-36 digits, or `u64::MAX`.
/// Reads 16 bytes at `p`.
#[inline(always)]
unsafe fn b36(p: *const u8, len: usize) -> u64 {
    let v = _mm_loadu_si128(p.cast());
    let iota = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    let inside = _mm_cmpgt_epi8(_mm_set1_epi8(len as i8), iota);
    let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
    let l = _mm_sub_epi8(v, _mm_set1_epi8(b'a' as i8));
    let is_d = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
    let is_l = _mm_cmpeq_epi8(_mm_min_epu8(l, _mm_set1_epi8(25)), l);
    let good = _mm_or_si128(
        _mm_or_si128(is_d, is_l),
        _mm_xor_si128(inside, _mm_set1_epi8(-1)),
    );
    if _mm_movemask_epi8(good) != 0xFFFF {
        return u64::MAX;
    }
    let val = _mm_and_si128(
        _mm_blendv_epi8(d, _mm_add_epi8(l, _mm_set1_epi8(10)), is_l),
        inside,
    );
    // Left-pad to eight digits; byte 0 is the most significant.
    let x = (_mm_cvtsi128_si64(val) as u64) << (8 * (8 - len));
    let t = (x & 0x00FF_00FF_00FF_00FF) * 36 + ((x >> 8) & 0x00FF_00FF_00FF_00FF);
    let q = (t & 0x0000_FFFF_0000_FFFF) * 1296 + ((t >> 16) & 0x0000_FFFF_0000_FFFF);
    (q & 0xFFFF_FFFF) * 1_679_616 + (q >> 32)
}

/// `prefix` and 1 to 8 base-36 digits without a leading zero: the canonical
/// form `base36` writes. Anything else is `u64::MAX`.
#[inline(always)]
unsafe fn id_value(p: *const u8, len: usize, prefix: u8) -> u64 {
    if len.wrapping_sub(2) > 7 || *p != prefix || (len > 2 && *p.add(1) == b'0') {
        return u64::MAX;
    }
    b36(p.add(1), len - 1)
}

#[inline(always)]
unsafe fn event_index(p: *const u8, len: usize, keys: &Keys) -> u64 {
    let v = id_value(p, len, b'e');
    if v > MASK40 {
        return u64::MAX;
    }
    let i = index_of(outer(v), keys.event);
    if i < keys.nmax {
        i
    } else {
        u64::MAX
    }
}

#[inline(always)]
unsafe fn user_number(p: *const u8, len: usize, key: u64) -> u64 {
    let v = id_value(p, len, b'u');
    if v > MASK40 {
        return u64::MAX;
    }
    let n = index_of(outer(v), key);
    if n < MAX_N {
        n
    } else {
        u64::MAX
    }
}

/// The packed name of user number `n`: the exact ID the generator writes.
fn name_of_number(n: u64, key: u64) -> Name {
    let mut v = scramble(n, key);
    let mut digits = [0u8; 13];
    let mut at = digits.len();
    loop {
        at -= 1;
        digits[at] = DIGITS[(v % 36) as usize];
        v /= 36;
        if v == 0 {
            break;
        }
    }
    let mut buf = [0u8; 24];
    buf[0] = b'u';
    let len = 1 + digits.len() - at;
    buf[1..len].copy_from_slice(&digits[at..]);
    unsafe { links::name_of(buf.as_ptr(), len) }
}

/// The user number behind a packed name, or `u64::MAX`.
fn name_number(name: Name, key: u64) -> u64 {
    let Some((chars, len)) = links::inline_chars(name) else {
        return u64::MAX;
    };
    let mut buf = [0u8; 32];
    buf[0] = b'u';
    buf[1..9].copy_from_slice(&chars.to_le_bytes());
    unsafe { user_number(buf.as_ptr(), len, key) }
}

#[derive(Clone, Copy, Debug)]
pub struct Keys {
    pub event: u64,
    pub user: u64,
    /// Exclusive bound on accepted event indexes, a whole number of buckets.
    pub nmax: u64,
}

/// Whether at least `need` pairs keep their index difference within `d`
/// modulo `2^bits` under key bits `k`.
fn passes(pairs: &[(u64, u64)], k: u64, bits: u32, d: u64, need: usize) -> bool {
    let m = (1u64 << bits) - 1;
    let half = 1u64 << (bits - 1);
    let allowed = pairs.len() - need;
    let (mut ok, mut fail) = (0, 0);
    for &(a, b) in pairs {
        let diff = ((a ^ k).wrapping_sub(b ^ k)).wrapping_mul(M1I) & m;
        let mag = if diff >= half { m + 1 - diff } else { diff };
        if mag <= d {
            ok += 1;
            if ok >= need {
                return true;
            }
        } else {
            fail += 1;
            if fail > allowed {
                return false;
            }
        }
    }
    false
}

/// How many pairs keep their index difference within `d` modulo `2^bits`.
fn pass_count(pairs: &[(u64, u64)], k: u64, bits: u32, d: u64) -> usize {
    let m = (1u64 << bits) - 1;
    let half = 1u64 << (bits - 1);
    pairs
        .iter()
        .filter(|&&(a, b)| {
            let diff = ((a ^ k).wrapping_sub(b ^ k)).wrapping_mul(M1I) & m;
            (if diff >= half { m + 1 - diff } else { diff }) <= d
        })
        .count()
}

/// Recovers a key from pairs of IDs whose indexes mostly differ by at most
/// `d`. Flipping key bit `j` changes a pair's difference by 0 or 2^(j+1), so
/// the difference modulo 2^m depends only on key bits below m - 1: bits are
/// fixed from the bottom up, keeping the best-scoring candidates. A key that
/// differs from the true one only where most pairs agree can score almost as
/// well, so the final word goes to `check`, a range test on decoded indexes
/// that a wrong key fails for nearly every ID. Bit 39 never changes a
/// difference; `check` settles it too.
fn solve(pairs: &[(u64, u64)], d: u64, start: u32, check: impl Fn(u64) -> bool) -> Option<u64> {
    const BEAM: usize = 32;
    if pairs.len() < 64 {
        return None;
    }
    let need = pairs.len() * 7 / 10;
    let keep = |mut scored: Vec<(usize, u64)>| -> Vec<u64> {
        scored.sort_unstable_by(|a, b| b.cmp(a));
        scored.truncate(BEAM);
        scored.into_iter().map(|(_, k)| k).collect()
    };
    let mut cands = keep(
        (0..1u64 << start)
            .filter(|&k| passes(pairs, k, start + 1, d, need))
            .map(|k| (pass_count(pairs, k, start + 1, d), k))
            .collect(),
    );
    for bit in start..39 {
        let scored: Vec<(usize, u64)> = cands
            .iter()
            .flat_map(|&k| [k, k | 1 << bit])
            .map(|k| (pass_count(pairs, k, bit + 2, d), k))
            .filter(|&(c, _)| c >= need)
            .collect();
        if scored.is_empty() {
            return None;
        }
        cands = keep(scored);
    }
    let found: Vec<u64> = cands
        .iter()
        .flat_map(|&k| [k, k | 1 << 39])
        .filter(|&k| check(k))
        .collect();
    (found.len() == 1).then(|| found[0])
}

/// Event key from pairs of one user's events within 30 minutes: they are a
/// few indexes apart. Samples the start of the largest files, 3 MB first
/// and more only when that yields too few pairs. Returns the key and the
/// index bound.
fn infer_events(event_files: &[(PathBuf, Buf)]) -> (Option<u64>, u64, usize) {
    let mut sc = Scan::new();
    let mut dates = ts::Dates::new();
    let mut files: Vec<&Buf> = event_files.iter().map(|(_, b)| b).collect();
    files.sort_by_key(|b| std::cmp::Reverse(b.len));
    files.truncate(8);
    let per = |total: usize| total / files.len().max(1);
    // (user ID hash, ts, ID value): sorting groups each user's rows in time
    // order without a map entry per user.
    let mut by_user: Vec<(u64, i64, u64)> = Vec::new();
    let mut event_xs: Vec<u64> = Vec::new();
    let (mut rows, mut bytes) = (0usize, 0usize);
    let mut pairs = Vec::new();
    let mut from = 0usize;
    for round in [3usize << 20, 8 << 20] {
        let to = per(round);
        for buf in &files {
            let (a, b) = (from.min(buf.len), to.min(buf.len));
            if a >= b {
                continue;
            }
            bytes += b - a;
            unsafe {
                for_rows::<7, _>(&mut sc, **buf, a, b, 1 << 16, |base, f| {
                    rows += 1;
                    let id = base.add(f[0].0 as usize);
                    let v = id_value(id, (f[0].1 - f[0].0) as usize, b'e');
                    if v > MASK40 {
                        return;
                    }
                    let x = outer(v);
                    if event_xs.len() < 4096 {
                        event_xs.push(x);
                    }
                    let user = field(base, f[1]);
                    let t = dates.parse(base.add(f[2].0 as usize), (f[2].1 - f[2].0) as usize);
                    if !user.is_empty() && t != ts::BAD {
                        by_user.push((hash(user.as_ptr(), user.len()), t, x));
                    }
                });
            }
        }
        from = to;
        by_user.sort_unstable();
        pairs.clear();
        for list in by_user.chunk_by(|a, b| a.0 == b.0) {
            if !(2..=4).contains(&list.len()) {
                continue;
            }
            for w in list.windows(2) {
                if w[1].1 - w[0].1 <= GAP as i64 && w[0].2 != w[1].2 {
                    pairs.push((w[0].2, w[1].2));
                }
            }
        }
        if pairs.len() >= 200 {
            break;
        }
    }
    if rows < 1000 || event_xs.len() < 512 {
        return (None, 0, pairs.len());
    }
    let total: usize = event_files.iter().map(|(_, b)| b.len).sum();
    let rows_est = (total as f64 * rows as f64 / bytes.max(1) as f64) as u64;
    let nmax = (rows_est + rows_est / 4 + 4096).next_multiple_of(1 << BSHIFT);
    pairs.truncate(256);
    let key = solve(&pairs, 64, 11, |k| {
        event_xs.iter().filter(|&&x| index_of(x, k) < nmax).count() * 20 >= event_xs.len() * 19
    });
    (key, nmax, pairs.len())
}

/// User key from links: the two IDs of a link are aliases of one user
/// number.
fn infer_users(data: &Path) -> (Option<u64>, usize) {
    let mut sc = Scan::new();
    let mut links: Vec<(PathBuf, Buf)> = crate::list_csvs(&data.join("links"));
    links.sort_by_key(|(_, b)| std::cmp::Reverse(b.len));
    links.truncate(4);
    let mut pairs = Vec::new();
    let mut user_xs = Vec::new();
    for (_, buf) in &links {
        let end = buf.len.min(256 << 10);
        unsafe {
            for_rows::<2, _>(&mut sc, *buf, 0, end, 1 << 16, |base, f| {
                let a = id_value(base.add(f[0].0 as usize), (f[0].1 - f[0].0) as usize, b'u');
                let b = id_value(base.add(f[1].0 as usize), (f[1].1 - f[1].0) as usize, b'u');
                if a <= MASK40 && b <= MASK40 && a != b {
                    if user_xs.len() < 4096 {
                        user_xs.push(outer(a));
                    }
                    if pairs.len() < 256 {
                        pairs.push((outer(a), outer(b)));
                    }
                }
            });
        }
    }
    if user_xs.len() < 512 {
        return (None, pairs.len());
    }
    let key = solve(&pairs, 512, 13, |k| {
        user_xs.iter().filter(|&&x| index_of(x, k) < MAX_N).count() * 20 >= user_xs.len() * 19
    });
    (key, pairs.len())
}

/// Recovers both keys from a sample of the input, the two in parallel.
/// `None` sends the run to the general pipeline.
pub fn infer(data: &Path, event_files: &[(PathBuf, Buf)], timing: bool) -> Option<Keys> {
    let started = Instant::now();
    let ((event, nmax, event_pairs), (user, link_pairs)) = std::thread::scope(|s| {
        let users = s.spawn(|| infer_users(data));
        let events = infer_events(event_files);
        (events, users.join().unwrap_or((None, 0)))
    });
    if timing {
        eprintln!(
            "  gx-infer {:>8.1} ms  event pairs {event_pairs} key {event:?}  link pairs {link_pairs} key {user:?}  nmax {nmax}",
            started.elapsed().as_secs_f64() * 1e3,
        );
    }
    Some(Keys {
        event: event?,
        user: user?,
        nmax,
    })
}

/// One event row filed under its index, in two words:
/// - `w0`: `ts - window_start` in ms (31 bits, all ones for an invalid row)
///   `<< 33 | price << cat_bits | category` (29 bits) `<< 4 |` alias (4 bits);
/// - `w1`: slot in the bucket (15 bits) `|` ingest seconds from
///   `G::ing_base` (21 bits) `<< 15 |` user reference (28 bits) `<< 36`.
///
/// A user reference below `SIDE` is the user number, with the alias in `w0`.
/// Otherwise it names a full user word (an alias of 16 or more, or an
/// undecodable ID) in the writing thread's side table.
#[repr(C)]
#[derive(Clone, Copy)]
struct EvRec {
    w0: u64,
    w1: u64,
}

const W0_INVALID: u64 = u64::MAX;
const ING_SHIFT: u32 = 15;
const ING_BITS: u32 = 21;
const ING_MASK: u64 = (1 << ING_BITS) - 1;
const USER_SHIFT: u32 = 36;
const SIDE: u64 = 1 << 27;
const SIDE_TID: u32 = 23;
const SIDE_MAX: usize = 1 << SIDE_TID;
const PC_MASK: u64 = (1 << 29) - 1;
/// Workers whose index fits a side reference.
pub const MAX_THREADS: usize = 16;

impl EvRec {
    #[inline(always)]
    fn ingest(&self) -> u64 {
        (self.w1 >> ING_SHIFT) & ING_MASK
    }
}

/// A click of a decoded user, filed under the user number.
#[repr(C)]
#[derive(Clone, Copy)]
struct ClRec {
    user: u64,
    click: Click,
}

/// An event whose ID is not a decodable index.
#[derive(Clone, Copy)]
struct OddEv {
    id: usize,
    len: u32,
    ts: u32,
    pc: u32,
    ingest: i64,
    user: u64,
}

/// A surviving event, in index order.
#[repr(C)]
#[derive(Clone, Copy)]
struct Win {
    ts: u32,
    pc: u32,
    user: u64,
}

struct G<'a> {
    ctx: &'a Ctx,
    keys: Keys,
    /// Origin of record ingest offsets, in seconds: four days before the
    /// window, so 21 bits reach about twenty days past its start.
    ing_base: i64,
    /// Roots of linked people that are not simple.
    cx_roots: Vec<AtomicU64>,
    /// User numbers with an ID in such a person.
    cx_users: Vec<AtomicU64>,
    /// What the links say about each user number's aliases; read in user
    /// order while aggregating, so it streams instead of probing the table.
    users: Vec<UserInfo>,
    ncb: usize,
}

/// Linked aliases of one user number: alias bits 0 to 7 are aliases
/// 0 to 7, plus the flags below, and the person they belong to.
#[derive(Default)]
struct UserInfo {
    canon: AtomicU64,
    bits: AtomicU32,
}

/// An alias numbered 8 or more is linked.
const LINKED_HIGH: u32 = 1 << 8;
/// The linked aliases belong to more than one person.
const MULTI: u32 = 1 << 9;
/// They belong to a complex person.
const CX: u32 = 1 << 10;

#[inline(always)]
fn bit(words: &[AtomicU64], i: usize) -> bool {
    words
        .get(i >> 6)
        .is_some_and(|w| w.load(Relaxed) >> (i & 63) & 1 != 0)
}

#[inline(always)]
fn set_bit(words: &[AtomicU64], i: usize) {
    if let Some(w) = words.get(i >> 6) {
        w.fetch_or(1 << (i & 63), Relaxed);
    }
}

#[inline(always)]
fn xpart(name: Name) -> usize {
    (name_hash(name) >> 40) as usize & (XP - 1)
}

/// People made complex by an undecodable event ID, by canonical name, and
/// their user numbers.
#[derive(Default)]
struct Dynamic {
    names: HashSet<Name>,
    users: Vec<u64>,
}

impl Dynamic {
    #[inline(always)]
    fn has(&self, name: Name) -> bool {
        !self.names.is_empty() && self.names.contains(&name)
    }
}

impl G<'_> {
    #[inline(always)]
    unsafe fn resolve(&self, name: Name) -> (Name, Option<usize>) {
        match self.ctx.links.root_of(name) {
            Some(r) => (self.ctx.links.canonical(r), Some(r)),
            None => (name, None),
        }
    }

    #[inline(always)]
    fn complex(&self, dynamic: &Dynamic, canonical: Name, root: Option<usize>) -> bool {
        root.is_some_and(|r| bit(&self.cx_roots, r)) || dynamic.has(canonical)
    }

    /// The person of decoded user number `n` and whether it is complex.
    #[inline(always)]
    unsafe fn person_of(&self, dynamic: &Dynamic, n: u64) -> (Name, bool) {
        let (u, a) = ((n >> 9) as usize, (n & 511) as u32);
        if let Some(e) = self.users.get(u) {
            let bits = e.bits.load(Relaxed);
            if bits & MULTI != 0 || (a >= 8 && bits & LINKED_HIGH != 0) {
                let (c, root) = self.resolve(name_of_number(n, self.keys.user));
                return (c, self.complex(dynamic, c, root));
            }
            if a < 8 && bits >> a & 1 != 0 {
                let c = e.canon.load(Relaxed);
                return (c, bits & CX != 0 || dynamic.has(c));
            }
        }
        let c = name_of_number(n, self.keys.user);
        (c, dynamic.has(c))
    }

    #[inline(always)]
    fn cbucket(&self, u: u64) -> usize {
        ((u >> CSHIFT) as usize).min(self.ncb - 1)
    }

    #[inline(always)]
    fn word(&self, ts: u32, pc: u32) -> u64 {
        (u64::from(ts) << (PRICE_BITS + self.ctx.cat_bits)) | u64::from(pc)
    }
}

struct GWork<'a> {
    slab: Slab<'a>,
    recs: PartWriter<EvRec>,
    clicks: PartWriter<ClRec>,
    xclk: PartWriter<Clk>,
    odd: Vec<OddEv>,
    /// Full user words that do not fit a record.
    side: Vec<u64>,
    tid: u64,
    overflow: bool,
    dates: ts::Dates,
    max_u: u64,
}

#[inline(always)]
unsafe fn user_word(key: u64, p: *const u8, len: usize, source: *const u8) -> u64 {
    let n = user_number(p, len, key);
    if n != u64::MAX {
        (n << 8) | REG
    } else {
        links::name_at(p, len, source)
    }
}

#[inline(always)]
unsafe fn ev_row(g: &G, w: &mut GWork, base: *const u8, source: *const u8, f: &[(u32, u32); 7]) {
    let ctx = g.ctx;
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
    let mut pc = 0u32;
    if valid && price != 0 {
        let c = ctx
            .products
            .get(base.add(f[5].0 as usize), (f[5].1 - f[5].0) as usize);
        let cat = if c == NONE { ctx.unknown } else { c as u64 };
        pc = ((price << ctx.cat_bits) | cat) as u32;
    }
    let up = base.add(f[1].0 as usize);
    let i = event_index(base.add(b0 as usize), (e0 - b0) as usize, &g.keys);
    let ing = ingest - g.ing_base;
    if i != u64::MAX && (0..1 << ING_BITS).contains(&ing) {
        let (w0, r) = if valid {
            let n = user_number(up, user_len, g.keys.user);
            let (r, alias) = if n != u64::MAX && n & 511 < 16 {
                (n >> 9, n & 15)
            } else {
                // A full side table cannot hold the word; the run falls
                // back (copies of one event must not be split between
                // the index path and the string path).
                w.overflow |= w.side.len() >= SIDE_MAX;
                let word = if n != u64::MAX {
                    (n << 8) | REG
                } else {
                    links::name_at(up, user_len, source.add(f[1].0 as usize))
                };
                w.side.push(word);
                (
                    SIDE | (w.tid << SIDE_TID)
                        | ((w.side.len() as u64 - 1) & (SIDE_MAX as u64 - 1)),
                    0,
                )
            };
            ((rel << 33) | (u64::from(pc) << 4) | alias, r)
        } else {
            (W0_INVALID, 0)
        };
        let w1 = (i & u64::from(BMASK)) | ((ing as u64) << ING_SHIFT) | (r << USER_SHIFT);
        w.recs
            .push((i >> BSHIFT) as usize, &mut w.slab, EvRec { w0, w1 });
        return;
    }
    w.odd.push(OddEv {
        id: source.add(b0 as usize) as usize,
        len: e0 - b0,
        ts: if valid { rel as u32 } else { NO_TS },
        pc,
        ingest,
        user: if valid {
            user_word(g.keys.user, up, user_len, source.add(f[1].0 as usize))
        } else {
            0
        },
    });
}

#[inline(always)]
unsafe fn ck_row(g: &G, w: &mut GWork, base: *const u8, source: *const u8, f: &[(u32, u32); 4]) {
    let ctx = g.ctx;
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
    let camp = ctx
        .campaigns
        .get(base.add(f[2].0 as usize), (f[2].1 - f[2].0) as usize);
    if camp == NONE {
        return;
    }
    let click = Click::new(
        rel,
        camp,
        base.add(f[0].0 as usize),
        (f[0].1 - f[0].0) as usize,
        source.add(f[0].0 as usize),
    );
    let up = base.add(f[1].0 as usize);
    let n = user_number(up, user_len, g.keys.user);
    if n != u64::MAX {
        let u = n >> 9;
        w.max_u = w.max_u.max(u);
        if bit(&g.cx_users, u as usize) {
            let (canonical, root) = g.resolve(links::name_at(up, user_len, up));
            if root.is_some_and(|r| bit(&g.cx_roots, r)) {
                w.xclk.push(
                    xpart(canonical),
                    &mut w.slab,
                    Clk {
                        name: canonical,
                        click,
                    },
                );
                return;
            }
        }
        w.clicks.push(
            g.cbucket(u),
            &mut w.slab,
            ClRec {
                user: (n << 8) | REG,
                click,
            },
        );
    } else {
        let (canonical, _) = g.resolve(links::name_at(up, user_len, source.add(f[1].0 as usize)));
        w.xclk.push(
            xpart(canonical),
            &mut w.slab,
            Clk {
                name: canonical,
                click,
            },
        );
    }
}

/// Per-run scratch for one user number's people. With one simple person
/// (the usual case) events and clicks go straight to the aggregator;
/// `events` and `clicks` hold them only once a second person shows up.
#[derive(Default)]
struct Run {
    cache: Vec<(u64, Name, bool, u32)>,
    people: Vec<Name>,
    events: Vec<(u32, u64)>,
    clicks: Vec<(u32, Click)>,
    multi: bool,
    sorted: bool,
    last_t: u64,
    /// `agg.evs.len()` when the run started.
    start: usize,
    /// The last alias looked up: most rows of a user use one alias.
    last: (u64, Name, bool, u32),
    /// The run's events as read, kept in case the run must be set aside.
    raw: Vec<Win>,
}

const NOT_HERE: u32 = u32::MAX;

impl Run {
    fn clear(&mut self, start: usize) {
        self.cache.clear();
        self.people.clear();
        self.events.clear();
        self.clicks.clear();
        self.multi = false;
        self.sorted = true;
        self.last_t = 0;
        self.start = start;
        self.last.0 = u64::MAX;
        self.raw.clear();
    }

    /// The person of user number `n`: canonical name, complex, run index.
    #[inline(always)]
    unsafe fn person(
        &mut self,
        g: &G,
        dynamic: &Dynamic,
        n: u64,
        with_events: bool,
    ) -> (Name, bool, u32) {
        if self.last.0 == n && (self.last.3 != NOT_HERE || !with_events || self.last.2) {
            return (self.last.1, self.last.2, self.last.3);
        }
        if let Some(&(_, name, cx, lp)) = self.cache.iter().find(|c| c.0 == n) {
            if lp != NOT_HERE || !with_events || cx {
                self.last = (n, name, cx, lp);
                return (name, cx, lp);
            }
        }
        let (canonical, cx) = g.person_of(dynamic, n);
        let mut lp = NOT_HERE;
        if !cx {
            if let Some(i) = self.people.iter().position(|&p| p == canonical) {
                lp = i as u32;
            } else if with_events {
                lp = self.people.len() as u32;
                self.people.push(canonical);
            }
        }
        if let Some(c) = self.cache.iter_mut().find(|c| c.0 == n) {
            *c = (n, canonical, cx, lp);
        } else {
            self.cache.push((n, canonical, cx, lp));
        }
        self.last = (n, canonical, cx, lp);
        (canonical, cx, lp)
    }
}

/// Thread-local writers for the general path.
struct Complex<'a> {
    slab: Slab<'a>,
    pays: PartWriter<Pay>,
    clks: PartWriter<Clk>,
}

impl<'a> Complex<'a> {
    fn new(ctx: &'a Ctx) -> Self {
        Complex {
            slab: Slab::new(&ctx.arena),
            pays: PartWriter::new(XP),
            clks: PartWriter::new(XP),
        }
    }

    #[inline(always)]
    unsafe fn event(&mut self, name: Name, w: u64) {
        self.pays.push(xpart(name), &mut self.slab, Pay { name, w });
    }

    #[inline(always)]
    unsafe fn click(&mut self, name: Name, click: Click) {
        self.clks
            .push(xpart(name), &mut self.slab, Clk { name, click });
    }
}

/// Runs the whole pipeline. `None` means nothing was written and the
/// general pipeline must run.
#[allow(clippy::too_many_arguments)]
pub fn run(
    ctx: &Ctx,
    dims: &Dims,
    keys: Keys,
    event_files: &[(PathBuf, Buf)],
    click_files: &[(PathBuf, Buf)],
    threads: usize,
    w1: i64,
    user_dir: &Path,
    timing: bool,
) -> Option<(usize, Vec<[i64; 4]>)> {
    let mut last = Instant::now();
    let mut lap = |what: &str| {
        if timing {
            let now = Instant::now();
            eprintln!("{what:>10} {:>8.1} ms", (now - last).as_secs_f64() * 1e3);
            last = now;
        }
    };
    // Record layout limits: worker index in a side reference, categories
    // next to a price in 29 bits, and the window in 31 bits of ms.
    if threads > MAX_THREADS || ctx.cat_bits > 5 || ctx.wlen >= (1 << 31) - 1 {
        return None;
    }
    let links = &ctx.links;
    let cap = links.cap();
    let nb = (keys.nmax >> BSHIFT) as usize;
    let ncb = ((keys.nmax / 16) >> CSHIFT)
        .next_power_of_two()
        .clamp(16, 4096) as usize;
    // People whose IDs do not all decode to one user number. Each alias is
    // decoded once; a root's own number is its canonical name's.
    let nums: Vec<AtomicU32> = (0..cap).map(|_| AtomicU32::new(u32::MAX)).collect();
    let linked_users = par_map(threads, |t| unsafe {
        let mut most = 0u64;
        for s in cap * t / threads..cap * (t + 1) / threads {
            let (k, _) = links.slot(s);
            if k == 0 {
                continue;
            }
            let nk = name_number(k, keys.user);
            if nk != u64::MAX {
                nums[s].store(nk as u32, Relaxed);
                most = most.max((nk >> 9) + 1);
            }
        }
        most
    })
    .into_iter()
    .max()
    .unwrap_or(0);
    let cx_roots: Vec<AtomicU64> = (0..cap / 64 + 1).map(|_| AtomicU64::new(0)).collect();
    par_map(threads, |t| unsafe {
        for s in cap * t / threads..cap * (t + 1) / threads {
            let (k, r) = links.slot(s);
            if k == 0 {
                continue;
            }
            let (nk, nc) = (nums[s].load(Relaxed), nums[r].load(Relaxed));
            if nk == u32::MAX || nc == u32::MAX || nk >> 9 != nc >> 9 {
                set_bit(&cx_roots, r);
            }
        }
    });
    // Every user number with a linked alias gets an entry.
    let cx_users: Vec<AtomicU64> = (0..(MAX_N >> 9) as usize / 64)
        .map(|_| AtomicU64::new(0))
        .collect();
    let users: Vec<UserInfo> = (0..linked_users).map(|_| UserInfo::default()).collect();
    par_map(threads, |t| unsafe {
        for s in cap * t / threads..cap * (t + 1) / threads {
            let nk = nums[s].load(Relaxed);
            if nk == u32::MAX {
                continue;
            }
            let (_, r) = links.slot(s);
            let u = (nk >> 9) as usize;
            let complex = bit(&cx_roots, r);
            if complex {
                set_bit(&cx_users, u);
            }
            let e = &users[u];
            let c = links.canonical(r);
            if let Err(other) = e.canon.compare_exchange(0, c, Relaxed, Relaxed) {
                if other != c {
                    e.bits.fetch_or(MULTI, Relaxed);
                }
            }
            let a = nk & 511;
            let alias = if a < 8 { 1 << a } else { LINKED_HIGH };
            e.bits
                .fetch_or(alias | if complex { CX } else { 0 }, Relaxed);
        }
    });
    drop(nums);
    let g = G {
        ctx,
        keys,
        ing_base: ctx.w0.div_euclid(1000) - 4 * 86_400,
        cx_roots,
        cx_users,
        users,
        ncb,
    };
    lap("gx-links");

    // Parse: one record per event row, filed under its index.
    let chunk = crate::env_usize("SS_CHUNK").unwrap_or(2 << 20).max(4096);
    let sub = crate::env_usize("SS_SUB").unwrap_or(96 << 10).max(64);
    let mut tasks: Vec<(Buf, usize, usize, &Path)> = Vec::new();
    for (path, buf) in event_files {
        for (a, b) in chunks(buf.len, chunk) {
            tasks.push((*buf, a, b, path));
        }
    }
    let n_event_tasks = tasks.len();
    for (path, buf) in click_files {
        for (a, b) in chunks(buf.len, chunk) {
            tasks.push((*buf, a, b, path));
        }
    }
    let next = AtomicUsize::new(0);
    let parsed = par_map(threads, |tid| {
        let mut sc = Scan::new();
        let mut reader = sys::EventReader::default();
        let mut w = GWork {
            slab: Slab::new(&ctx.arena),
            recs: PartWriter::new(nb),
            clicks: PartWriter::new(ncb),
            xclk: PartWriter::new(XP),
            odd: Vec::new(),
            side: Vec::new(),
            tid: tid as u64,
            overflow: false,
            dates: ts::Dates::new(),
            max_u: 0,
        };
        loop {
            let task = next.fetch_add(1, Relaxed);
            let Some(&(buf, a, b, path)) = tasks.get(task) else {
                break;
            };
            let is_click = task >= n_event_tasks;
            unsafe {
                let (input, start, len) = if buf.can_pread && buf.len >= 256 << 10 {
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
                        scan::rows::<4, _>(&sc, len - pos, |f| ck_row(&g, &mut w, base, mapped, f));
                    } else {
                        scan::rows::<7, _>(&sc, len - pos, |f| ev_row(&g, &mut w, base, mapped, f));
                    }
                    pos = next;
                }
            }
        }
        (
            w.recs.finish(),
            w.clicks.finish(),
            w.xclk.finish(),
            w.odd,
            w.max_u,
            w.side,
            w.overflow,
        )
    });
    let mut recs = Vec::new();
    let mut cks = Vec::new();
    let mut xclk_lists = Vec::new();
    let mut odd: Vec<OddEv> = Vec::new();
    let mut sides: Vec<Vec<u64>> = Vec::new();
    let mut overflow = false;
    let mut max_u = 0;
    for (r, c, x, o, m, s, f) in parsed {
        recs.push(r);
        cks.push(c);
        xclk_lists.push(x);
        odd.extend(o);
        max_u = max_u.max(m);
        sides.push(s);
        overflow |= f;
    }
    if overflow {
        return None;
    }
    let recs = merge_blocks(recs, nb);
    let cks = merge_blocks(cks, ncb);
    lap("gx-parse");

    // Undecodable event IDs: exact dedup among themselves. Their people take
    // the general path, with all of their other rows.
    let mut dynamic = Dynamic::default();
    let mut odd_x = Complex::new(ctx);
    unsafe {
        let mut best: HashMap<&[u8], usize> = HashMap::with_capacity(odd.len());
        for (k, e) in odd.iter().enumerate() {
            let id = std::slice::from_raw_parts(e.id as *const u8, e.len as usize);
            best.entry(id)
                .and_modify(|b| {
                    if e.ingest < odd[*b].ingest {
                        *b = k;
                    }
                })
                .or_insert(k);
        }
        for &k in best.values() {
            let e = odd[k];
            if e.ts == NO_TS {
                continue;
            }
            let name = if e.user & 0xFF == REG {
                name_of_number(e.user >> 8, keys.user)
            } else {
                e.user
            };
            let (canonical, _) = g.resolve(name);
            odd_x.event(canonical, g.word(e.ts, e.pc));
            dynamic.names.insert(canonical);
            let n = name_number(canonical, keys.user);
            if n != u64::MAX {
                dynamic.users.push(n >> 9);
            }
        }
    }
    dynamic.users.sort_unstable();
    dynamic.users.dedup();
    let odd_events = odd.len();
    drop(odd);

    // Dedup and aggregation run together: each worker takes a range of
    // index buckets, keeps the earliest copy per slot in L2-resident arrays,
    // and aggregates the survivors in index order straight away. A range's
    // first and last user may continue in a neighbouring range; their
    // simple people's events are set aside (complex events go to the general
    // path at once) and finished afterwards, in one pool with the complex
    // people, largest work first.
    let nranges = (threads * 16).min(nb).max(1);
    let rows_hint = keys.nmax as usize / 5 / threads + 4096;
    let (next_r, next_t) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let barrier = Barrier::new(threads);
    let ranges: Mutex<Vec<(usize, u64, u64, bool, usize)>> = Mutex::new(Vec::new());
    let deferred: Mutex<Vec<Deferred>> = Mutex::new(Vec::new());
    let complex_lists: Mutex<Vec<(Vec<Vec<Blk>>, Vec<Vec<Blk>>)>> = Mutex::new(Vec::new());
    // (ordered, live events, set-aside runs, tasks): a task is a user's
    // set-aside runs `(start, end)` or a complex partition `(XP + p, 0)`.
    type Plan = (bool, usize, Vec<Deferred>, Vec<(usize, usize)>);
    let plan: OnceLock<Plan> = OnceLock::new();
    let odd_x = Mutex::new(Some(odd_x));
    let outs = par_map(threads, |t| {
        let t0 = Instant::now();
        let mut marks = [0f64; 5];
        let mut agg = ManuallyDrop::new(Agg::new(ctx, dims, ctx.w0, w1, rows_hint));
        // Simple people arrive in time order (sorted here if not).
        agg.presorted = true;
        let mut cx = Complex::new(ctx);
        let mut run = Run::default();
        let mut clicks = Clicks::new();
        // Clicks of people made complex by an undecodable event ID go to
        // the general path here, whatever else those users have.
        if t == 0 {
            for &u in &dynamic.users {
                run.clear(0);
                for &(n, click) in unsafe { clicks.of(&g, &cks, u) } {
                    let (name, complex, _) = unsafe { run.person(&g, &dynamic, n, false) };
                    if complex {
                        unsafe { cx.click(name, click) };
                    }
                }
            }
        }

        let mut tags = vec![0u16; 1 << BSHIFT];
        let mut vals = vec![EvRec { w0: 0, w1: 0 }; 1 << BSHIFT];
        let mut gen = 0u16;
        let mut mine = Vec::new();
        let mut meta: Vec<u64> = Vec::new();
        loop {
            let k = next_r.fetch_add(1, Relaxed);
            if k >= nranges {
                break;
            }
            let (b0, b1) = (nb * k / nranges, nb * (k + 1) / nranges);
            let r = unsafe {
                range_people(
                    &g,
                    &dynamic,
                    &recs[b0..b1],
                    &cks,
                    &mut clicks,
                    &mut tags,
                    &mut vals,
                    &mut gen,
                    &sides,
                    &mut agg,
                    &mut meta,
                    &mut cx,
                    &mut run,
                    &mut mine,
                    k,
                )
            };
            ranges.lock().unwrap().push((k, r.0, r.1, r.2, r.3));
        }
        deferred.lock().unwrap().append(&mut mine);
        {
            // Nothing reaches the general path after this point: set-aside
            // runs hold simple people only, and complex clicks were routed
            // while parsing or before the ranges.
            let done = std::mem::replace(&mut cx, Complex::new(ctx));
            let lists = (done.pays.finish(), done.clks.finish());
            let mut all = complex_lists.lock().unwrap();
            all.push(lists);
            if let Some(o) = odd_x.lock().unwrap().take() {
                all.push((o.pays.finish(), o.clks.finish()));
            }
        }
        marks[0] = t0.elapsed().as_secs_f64() * 1e3;
        barrier.wait();

        if t == 0 {
            let mut rs = std::mem::take(&mut *ranges.lock().unwrap());
            rs.sort_unstable_by_key(|r| r.0);
            let mut ordered = rs.iter().all(|r| r.3);
            let mut prev = NO_U;
            for r in &rs {
                if r.1 != NO_U {
                    if prev != NO_U && r.1 < prev {
                        ordered = false;
                    }
                    prev = r.2;
                }
            }
            let live = rs.iter().map(|r| r.4).sum();
            let mut d = std::mem::take(&mut *deferred.lock().unwrap());
            d.sort_by_key(|x| (x.0, x.1));
            // (size, task)
            let mut tasks: Vec<(usize, (usize, usize))> = Vec::new();
            let mut s = 0;
            for i in 1..=d.len() {
                if i == d.len() || d[i].2 != d[s].2 {
                    let size = d[s..i].iter().map(|x| x.3.len()).sum();
                    tasks.push((size, (s, i)));
                    s = i;
                }
            }
            let lists = complex_lists.lock().unwrap();
            for p in 0..XP {
                let mut size = 0usize;
                for (lp, lc) in lists.iter() {
                    size += lp[p]
                        .iter()
                        .chain(&lc[p])
                        .map(|b| b.n as usize)
                        .sum::<usize>();
                }
                for lc in &xclk_lists {
                    size += lc[p].iter().map(|b| b.n as usize).sum::<usize>();
                }
                tasks.push((size, (XP + p, 0)));
            }
            tasks.sort_by_key(|x| std::cmp::Reverse(x.0));
            let _ = plan.set((ordered, live, d, tasks.into_iter().map(|x| x.1).collect()));
        }
        barrier.wait();
        let (ordered, _, d, tasks) = plan.get().unwrap();
        if !*ordered {
            return None;
        }

        loop {
            let Some(&(s, e)) = tasks.get(next_t.fetch_add(1, Relaxed)) else {
                break;
            };
            unsafe {
                if e == 0 {
                    // A complex partition: grouped from scattered rows, so
                    // its people need sorting.
                    let p = s - XP;
                    consume_all(&mut agg);
                    meta.clear();
                    let (mut pays, mut clks) = (Vec::new(), Vec::new());
                    for (lp, lc) in complex_lists.lock().unwrap().iter() {
                        pays.extend_from_slice(&lp[p]);
                        clks.extend_from_slice(&lc[p]);
                    }
                    for lc in &xclk_lists {
                        clks.extend_from_slice(&lc[p]);
                    }
                    agg.presorted = false;
                    agg.partition(&pays, &clks);
                    agg.presorted = true;
                    // `partition` consumed its people but leaves them listed.
                    agg.names.clear();
                    agg.ev_end.clear();
                    agg.ck_end.clear();
                    agg.evs.clear();
                    agg.cks.clear();
                } else {
                    // A user whose events span ranges, from the earliest
                    // range on: index order again.
                    let u = d[s].2;
                    meta.resize(agg.evs.len(), 0);
                    let mut o = Open {
                        u,
                        a: agg.evs.len(),
                        aliases: 0,
                        wide: false,
                        sorted: true,
                        last_t: 0,
                    };
                    for item in &d[s..e] {
                        for w in &item.3 {
                            agg.evs.push(g.word(w.ts, w.pc));
                            let n = w.user >> 8;
                            meta.push(n);
                            let alias = n & 511;
                            if alias < 16 {
                                o.aliases |= 1 << alias;
                            } else {
                                o.wide = true;
                            }
                            let t = u64::from(w.ts);
                            o.sorted &= t >= o.last_t;
                            o.last_t = t;
                        }
                    }
                    end_user(
                        &g,
                        &dynamic,
                        &o,
                        &cks,
                        &mut clicks,
                        &mut agg,
                        &mut meta,
                        &mut cx,
                        &mut run,
                    );
                    if agg.names.len() >= 4096 {
                        consume_all(&mut agg);
                        meta.clear();
                    }
                }
            }
        }
        unsafe { consume_all(&mut agg) };
        debug_assert!(cx
            .pays
            .finish()
            .iter()
            .chain(&cx.clks.finish())
            .all(|b| b.is_empty()));
        marks[2] = t0.elapsed().as_secs_f64() * 1e3;
        if agg.out.rows > 0 || t == 0 {
            let rows = agg.out.rows;
            agg.out.revenue.finish(rows);
            write_user_file(user_dir, t, &agg.out, agg.day0, agg.ndays, agg.cat_names);
        }
        marks[3] = t0.elapsed().as_secs_f64() * 1e3;
        Some((agg.out.rows, std::mem::take(&mut agg.out.camp), marks))
    });
    lap("gx-agg");
    if outs.iter().any(|o| o.is_none()) {
        if timing {
            eprintln!("  gx: survivors not ordered by user number");
        }
        return None;
    }
    let outs: Vec<_> = outs.into_iter().flatten().collect();
    if timing {
        let worst = |i: usize| outs.iter().map(|o| o.2[i]).fold(0f64, f64::max);
        let best = |i: usize| outs.iter().map(|o| o.2[i]).fold(f64::MAX, f64::min);
        let lists = complex_lists.lock().unwrap();
        let xe: usize = lists
            .iter()
            .map(|l| l.0.iter().flatten().map(|b| b.n as usize).sum::<usize>())
            .sum();
        let (_, live, d, _) = plan.get().unwrap();
        eprintln!(
            "  gx: {live} live, {odd_events} odd, {} split runs; ranges {:.1}..{:.1} rest {:.1} write {:.1} ms; complex events {xe}",
            d.len(),
            best(0),
            worst(0),
            worst(2),
            worst(3)
        );
    }

    let mut total = vec![[0i64; 4]; outs[0].1.len()];
    let mut user_rows = 0usize;
    for (rows, camp, _) in &outs {
        user_rows += rows;
        for (t, c) in total.iter_mut().zip(camp) {
            for i in 0..4 {
                t[i] += c[i];
            }
        }
    }
    Some((user_rows, total))
}

/// A run set aside because it may continue in a neighbouring range:
/// (range, 0 for a range's first run or 1 for its last, user number,
/// events). A first run holds every event of the user in that range, still
/// unprocessed; a last run holds only its simple people's events, since its
/// complex events were already passed on.
type Deferred = (usize, u8, u64, Vec<Win>);

/// One click bucket's clicks grouped by user number, rebuilt in place when
/// a worker moves on to the next bucket. Workers visit users in increasing
/// order, so each bucket is grouped about once and stays in cache.
struct Clicks {
    bucket: usize,
    base: u64,
    offs: Vec<u32>,
    fill: Vec<u32>,
    items: Vec<(u64, Click)>,
}

impl Clicks {
    fn new() -> Self {
        Clicks {
            bucket: usize::MAX,
            base: 0,
            offs: vec![0],
            fill: Vec::new(),
            items: Vec::new(),
        }
    }

    unsafe fn build(&mut self, blocks: &[Blk]) {
        let recs = || {
            blocks
                .iter()
                .flat_map(|b| std::slice::from_raw_parts(b.ptr as *const ClRec, b.n as usize))
        };
        let (mut lo, mut hi) = (u64::MAX, 0);
        for c in recs() {
            let u = c.user >> 17;
            lo = lo.min(u);
            hi = hi.max(u);
        }
        self.offs.clear();
        self.items.clear();
        if lo > hi {
            self.base = 0;
            self.offs.push(0);
            return;
        }
        let span = (hi - lo + 1) as usize;
        self.offs.resize(span + 1, 0);
        let offs = self.offs.as_mut_ptr();
        for c in recs() {
            *offs.add(((c.user >> 17) - lo) as usize + 1) += 1;
        }
        for i in 1..=span {
            *offs.add(i) += *offs.add(i - 1);
        }
        let total = *offs.add(span) as usize;
        self.fill.clear();
        self.fill.extend_from_slice(&self.offs);
        if self.items.capacity() < total {
            self.items.reserve(total);
        }
        let items = self.items.as_mut_ptr();
        let fill = self.fill.as_mut_ptr();
        for c in recs() {
            let at = fill.add(((c.user >> 17) - lo) as usize);
            items.add(*at as usize).write((c.user >> 8, c.click));
            *at += 1;
        }
        self.items.set_len(total);
        self.base = lo;
    }

    /// User `u`'s clicks.
    #[inline(always)]
    unsafe fn of(&mut self, g: &G, cks: &[Vec<Blk>], u: u64) -> &[(u64, Click)] {
        let b = g.cbucket(u);
        if b != self.bucket {
            self.build(&cks[b]);
            self.bucket = b;
        }
        if u < self.base || u - self.base + 1 >= self.offs.len() as u64 {
            return &[];
        }
        let i = (u - self.base) as usize;
        &self.items[self.offs[i] as usize..self.offs[i + 1] as usize]
    }
}

unsafe fn consume_all(agg: &mut Agg) {
    let people = agg.names.len();
    if people > 0 {
        agg.consume::<false>(people, 0);
    }
    agg.names.clear();
    agg.ev_end.clear();
    agg.ck_end.clear();
    agg.evs.clear();
    agg.cks.clear();
}

/// Adds one surviving event to the current run. Returns whether it went to
/// a simple person.
#[inline(always)]
unsafe fn add_event(
    g: &G,
    dynamic: &Dynamic,
    run: &mut Run,
    agg: &mut Agg,
    cx: &mut Complex,
    w: Win,
) -> bool {
    let (name, complex, lp) = run.person(g, dynamic, w.user >> 8, true);
    let word = g.word(w.ts, w.pc);
    if complex {
        cx.event(name, word);
        return false;
    }
    let t = u64::from(w.ts);
    run.sorted &= t >= run.last_t;
    run.last_t = t;
    if lp == 0 && !run.multi {
        agg.evs.push(word);
    } else {
        if !run.multi {
            run.multi = true;
            let start = run.start;
            run.events.extend(agg.evs.drain(start..).map(|w| (0, w)));
        }
        run.events.push((lp, word));
    }
    true
}

/// The user whose events are being appended.
struct Open {
    u: u64,
    /// Index of the user's first event in `agg.evs`.
    a: usize,
    /// Alias numbers below 16 seen, one bit each.
    aliases: u32,
    /// An alias of 16 or more was seen.
    wide: bool,
    sorted: bool,
    last_t: u64,
}

/// Which of a user's clicks belong to the user's one simple person.
#[derive(Clone, Copy)]
enum Owner {
    /// Every linked alias below 8, as a bit mask.
    Linked(u32),
    /// Exactly this unlinked user number.
    Single(u64),
}

/// The events `agg.evs[from..]` as rows again, with their user numbers.
fn rows_of(agg: &Agg, meta: &[u64], from: usize, shift: u32) -> Vec<Win> {
    (from..agg.evs.len())
        .map(|i| {
            let w = agg.evs[i];
            Win {
                ts: (w >> shift) as u32,
                pc: (w & ((1 << shift) - 1)) as u32,
                user: (meta[i] << 8) | REG,
            }
        })
        .collect()
}

/// Finishes user `o`, whose events are `agg.evs[o.a..]`.
#[allow(clippy::too_many_arguments)]
unsafe fn end_user(
    g: &G,
    dynamic: &Dynamic,
    o: &Open,
    cks: &[Vec<Blk>],
    clicks: &mut Clicks,
    agg: &mut Agg,
    meta: &mut Vec<u64>,
    cx: &mut Complex,
    run: &mut Run,
) {
    let shift = PRICE_BITS + g.ctx.cat_bits;
    let u = o.u;
    let dynamic_user = !dynamic.users.is_empty() && dynamic.users.binary_search(&u).is_ok();
    if !o.wide && !dynamic_user {
        let bits = g.users.get(u as usize).map_or(0, |e| e.bits.load(Relaxed));
        if bits & (MULTI | LINKED_HIGH) == 0 {
            let linked = bits & 0xFF;
            let one = if linked != 0 && o.aliases & !linked == 0 {
                let c = g.users[u as usize].canon.load(Relaxed);
                Some((c, bits & CX != 0, Owner::Linked(linked)))
            } else if o.aliases & linked == 0 && o.aliases.count_ones() == 1 {
                let n = meta[o.a];
                Some((name_of_number(n, g.keys.user), false, Owner::Single(n)))
            } else {
                None
            };
            if let Some((person, complex, owner)) = one {
                if !complex && !dynamic.has(person) {
                    let e = agg.evs.len();
                    agg.names.push(person);
                    if !o.sorted {
                        agg.evs[o.a..e].sort_unstable();
                    }
                    agg.ev_end.push(e as u32);
                    for &(n, click) in clicks.of(g, cks, u) {
                        let mine = match owner {
                            Owner::Linked(m) => {
                                let a = n & 511;
                                a < 8 && (m >> a) & 1 != 0
                            }
                            Owner::Single(only) => n == only,
                        };
                        // Clicks of the user's other people: those people
                        // have no events here, and a complex one had its
                        // clicks routed while parsing.
                        if mine {
                            agg.cks.push(click);
                        }
                    }
                    agg.ck_end.push(agg.cks.len() as u32);
                    return;
                }
            }
        }
    }
    // Several people, a complex person, wide aliases, or a person made
    // complex by an undecodable event: one event at a time.
    let rows = rows_of(agg, meta, o.a, shift);
    agg.evs.truncate(o.a);
    run.clear(o.a);
    for w in rows {
        add_event(g, dynamic, run, agg, cx, w);
    }
    // A dynamic user's complex clicks were passed on before the ranges.
    finish_run(
        g,
        dynamic,
        run,
        clicks.of(g, cks, u),
        agg,
        cx,
        !dynamic_user,
    );
    meta.resize(agg.evs.len(), 0);
}

/// Sets aside user `o`, which may continue in a neighbouring range. Its
/// complex people's events go to the general path now, so the complex
/// partitions are complete once every range is done.
#[allow(clippy::too_many_arguments)]
unsafe fn defer_user(
    g: &G,
    dynamic: &Dynamic,
    o: &Open,
    which: u8,
    k: usize,
    agg: &mut Agg,
    meta: &mut Vec<u64>,
    cx: &mut Complex,
    run: &mut Run,
    deferred: &mut Vec<Deferred>,
) {
    let shift = PRICE_BITS + g.ctx.cat_bits;
    let rows = rows_of(agg, meta, o.a, shift);
    agg.evs.truncate(o.a);
    meta.truncate(o.a);
    run.clear(0);
    let kept: Vec<Win> = rows
        .into_iter()
        .filter(|w| {
            let (name, complex, _) = run.person(g, dynamic, w.user >> 8, false);
            if complex {
                cx.event(name, g.word(w.ts, w.pc));
            }
            !complex
        })
        .collect();
    deferred.push((k, which, o.u, kept));
}

/// Dedups and aggregates the buckets of one range. Survivors are appended
/// to `agg.evs` in index order, with their user numbers in `meta`, so each
/// user's events form one slice. The range's first and last user may
/// continue in a neighbouring range and are set aside. Returns the first
/// and last decoded user number, whether they never decrease, and the
/// number of surviving events.
#[allow(clippy::too_many_arguments)]
unsafe fn range_people(
    g: &G,
    dynamic: &Dynamic,
    recs: &[Vec<Blk>],
    cks: &[Vec<Blk>],
    clicks: &mut Clicks,
    tags: &mut [u16],
    vals: &mut [EvRec],
    gen: &mut u16,
    sides: &[Vec<u64>],
    agg: &mut Agg,
    meta: &mut Vec<u64>,
    cx: &mut Complex,
    run: &mut Run,
    deferred: &mut Vec<Deferred>,
    k: usize,
) -> (u64, u64, bool, usize) {
    let shift = PRICE_BITS + g.ctx.cat_bits;
    let (mut first, mut last, mut ordered, mut live) = (NO_U, NO_U, true, 0usize);
    let mut runs = 0usize;
    meta.clear();
    let mut o = Open {
        u: NO_U,
        a: 0,
        aliases: 0,
        wide: false,
        sorted: true,
        last_t: 0,
    };
    for blocks in recs {
        // A slot belongs to this bucket when its tag is this bucket's
        // generation, so the slots never need clearing.
        *gen = gen.wrapping_add(1);
        if *gen == 0 {
            tags.fill(0);
            *gen = 1;
        }
        let tag = *gen;
        for blk in blocks {
            for r in std::slice::from_raw_parts(blk.ptr as *const EvRec, blk.n as usize) {
                let s = (r.w1 & u64::from(BMASK)) as usize;
                if *tags.get_unchecked(s) != tag || r.ingest() < vals.get_unchecked(s).ingest() {
                    *tags.get_unchecked_mut(s) = tag;
                    *vals.get_unchecked_mut(s) = *r;
                }
            }
        }
        // This bucket's slots in index order, sixteen tags at a time.
        let tagv = _mm256_set1_epi16(tag as i16);
        let mut group = 0;
        while group < tags.len() {
            let v = _mm256_loadu_si256(tags.as_ptr().add(group).cast());
            let eq = _mm256_movemask_epi8(_mm256_cmpeq_epi16(v, tagv)) as u32;
            let mut full = _pext_u32(eq, 0x5555_5555);
            let first_slot = group;
            group += 16;
            while full != 0 {
                let s = first_slot + full.trailing_zeros() as usize;
                full &= full - 1;
                let r = *vals.get_unchecked(s);
                if r.w0 == W0_INVALID {
                    continue;
                }
                live += 1;
                let ts = r.w0 >> 33;
                let word = (ts << shift) | ((r.w0 >> 4) & PC_MASK);
                let refr = r.w1 >> USER_SHIFT;
                let n = if refr & SIDE == 0 {
                    (refr << 9) | (r.w0 & 15)
                } else {
                    let uw = *sides
                        .get_unchecked(((refr >> SIDE_TID) & 15) as usize)
                        .get_unchecked((refr & (SIDE_MAX as u64 - 1)) as usize);
                    if uw & 0xFF != REG {
                        // An undecodable user ID: always the general path.
                        let (canonical, _) = g.resolve(uw);
                        cx.event(canonical, word);
                        continue;
                    }
                    uw >> 8
                };
                let u = n >> 9;
                if u != o.u {
                    if o.u != NO_U {
                        if runs == 1 {
                            defer_user(g, dynamic, &o, 0, k, agg, meta, cx, run, deferred);
                        } else {
                            end_user(g, dynamic, &o, cks, clicks, agg, meta, cx, run);
                        }
                    }
                    if first == NO_U {
                        first = u;
                    } else if u < last {
                        ordered = false;
                    }
                    last = u;
                    runs += 1;
                    o = Open {
                        u,
                        a: agg.evs.len(),
                        aliases: 0,
                        wide: false,
                        sorted: true,
                        last_t: 0,
                    };
                }
                agg.evs.push(word);
                meta.push(n);
                let alias = n & 511;
                if alias < 16 {
                    o.aliases |= 1 << alias;
                } else {
                    o.wide = true;
                }
                o.sorted &= ts >= o.last_t;
                o.last_t = ts;
            }
        }
    }
    if o.u != NO_U {
        defer_user(
            g,
            dynamic,
            &o,
            if runs == 1 { 0 } else { 1 },
            k,
            agg,
            meta,
            cx,
            run,
            deferred,
        );
    }
    consume_all(agg);
    meta.clear();
    (first, last, ordered, live)
}

#[allow(clippy::too_many_arguments)]
unsafe fn finish_run(
    g: &G,
    dynamic: &Dynamic,
    run: &mut Run,
    clicks: &[(u64, Click)],
    agg: &mut Agg,
    cx: &mut Complex,
    route: bool,
) {
    for &(n, click) in clicks {
        let (name, complex, lp) = run.person(g, dynamic, n, false);
        if complex {
            if route {
                cx.click(name, click);
            }
        } else if lp != NOT_HERE {
            if run.multi {
                run.clicks.push((lp, click));
            } else {
                agg.cks.push(click);
            }
        }
    }
    let people = run.people.len();
    if people == 0 {
        return;
    }
    // The aggregator skips sorting; restore time order where the input
    // did not provide it.
    if !run.multi {
        agg.names.push(run.people[0]);
        if !run.sorted {
            agg.evs[run.start..].sort_unstable();
        }
        agg.ev_end.push(agg.evs.len() as u32);
        agg.ck_end.push(agg.cks.len() as u32);
        return;
    }
    for p in 0..people as u32 {
        agg.names.push(run.people[p as usize]);
        let from = agg.evs.len();
        agg.evs
            .extend(run.events.iter().filter(|e| e.0 == p).map(|e| e.1));
        if !run.sorted {
            agg.evs[from..].sort_unstable();
        }
        agg.ev_end.push(agg.evs.len() as u32);
        agg.cks
            .extend(run.clicks.iter().filter(|c| c.0 == p).map(|c| c.1));
        agg.ck_end.push(agg.cks.len() as u32);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(prefix: u8, v: u64) -> Vec<u8> {
        let mut out = vec![prefix];
        let mut digits = Vec::new();
        let mut rest = v;
        loop {
            digits.push(DIGITS[(rest % 36) as usize]);
            rest /= 36;
            if rest == 0 {
                break;
            }
        }
        digits.reverse();
        out.extend(digits);
        out
    }

    #[test]
    fn inverses_and_scramble_round_trip() {
        assert_eq!(M1.wrapping_mul(M1I) & MASK40, 1);
        assert_eq!(M2.wrapping_mul(M2I) & MASK40, 1);
        for key in [0u64, 0x1234_5678_9abc, u64::MAX] {
            for i in [0u64, 1, 2, 511, 512, 1 << 20, (1 << 39) + 3, MASK40] {
                assert_eq!(index_of(outer(scramble(i, key)), key), i);
            }
        }
    }

    #[test]
    fn base36_matches_canonical_ids_only() {
        for v in [
            0u64,
            1,
            35,
            36,
            1295,
            1296,
            999_999,
            MASK40,
            36u64.pow(8) - 1,
        ] {
            let mut id = encode(b'e', v);
            let len = id.len();
            id.resize(64, b',');
            assert_eq!(unsafe { id_value(id.as_ptr(), len, b'e') }, v, "{v}");
        }
        for bad in [
            &b"e"[..],
            b"e00",
            b"eA",
            b"u1",
            b"e1-",
            b"e123456789",
            b"e1 ",
        ] {
            let mut id = bad.to_vec();
            let len = id.len();
            id.resize(64, b',');
            assert_eq!(unsafe { id_value(id.as_ptr(), len, b'e') }, u64::MAX);
        }
    }

    #[test]
    fn names_round_trip_through_user_numbers() {
        let key = 0xdead_beef_1234;
        for n in [0u64, 1, 511, 512 * 77 + 3, MAX_N - 1] {
            assert_eq!(name_number(name_of_number(n, key), key), n);
        }
    }

    #[test]
    fn keys_are_recovered_from_close_pairs() {
        let key = 0x5a5a_1234_9876 & MASK40;
        let mut state = 0x1234_5678u64;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state >> 33
        };
        let mut pairs = Vec::new();
        let mut xs = Vec::new();
        for _ in 0..200 {
            let i = next() % 20_000_000;
            let d = next() % 30;
            pairs.push((outer(scramble(i, key)), outer(scramble(i + d + 1, key))));
            xs.push(outer(scramble(i, key)));
        }
        let found = solve(&pairs, 64, 11, |k| {
            xs.iter().all(|&x| index_of(x, k) < 25_000_000)
        });
        assert_eq!(found, Some(key));
    }
}
