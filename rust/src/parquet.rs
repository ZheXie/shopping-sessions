//! A minimal Parquet writer: PLAIN, uncompressed, one data page per column
//! chunk. Strings are UTF8 BYTE_ARRAY, `day` is INT32 DATE.

use std::fs::File;
use std::io::{BufWriter, Write};

use crate::sys::die;

/// Thrift compact protocol
struct TW {
    b: Vec<u8>,
    last: Vec<i32>,
}
impl TW {
    fn new() -> Self {
        TW { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v as u64) << 1) ^ ((v >> 63) as u64));
    }
    fn fh(&mut self, id: i32, ty: u8) {
        let d = id - *self.last.last().unwrap();
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i32, v: i64) {
        self.fh(id, 5);
        self.zz(v);
    }
    fn i64f(&mut self, id: i32, v: i64) {
        self.fh(id, 6);
        self.zz(v);
    }
    fn str(&mut self, id: i32, s: &str) {
        self.fh(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s.as_bytes());
    }
    fn sbeg(&mut self, id: i32) {
        self.fh(id, 12);
        self.last.push(0);
    }
    fn send(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn lbeg(&mut self, id: i32, et: u8, n: usize) {
        self.fh(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | et);
        } else {
            self.b.push(0xF0 | et);
            self.varint(n as u64);
        }
    }
    fn ebeg(&mut self) {
        self.last.push(0);
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Str,
    OptStr,
    Date,
    I32,
    I64,
}
fn ptype(k: Kind) -> i64 {
    match k {
        Kind::Str | Kind::OptStr => 6,
        Kind::I64 => 2,
        _ => 1,
    }
}
#[derive(Default)]
pub struct ColBuf {
    pub data: Vec<u8>,
    pub defs: Vec<u8>,
}
impl ColBuf {
    #[inline(always)]
    pub fn put_str(&mut self, s: &[u8]) {
        self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.data.extend_from_slice(s);
    }
    #[inline(always)]
    pub fn put_i32(&mut self, v: i32) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }
    #[inline(always)]
    pub fn put_i64(&mut self, v: i64) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }
}

struct ChunkMeta {
    page_off: i64,
    size: i64,
    nvals: i64,
}
pub struct PqWriter {
    f: BufWriter<File>,
    cols: Vec<(&'static str, Kind)>,
    off: i64,
    nrows: i64,
    groups: Vec<(i64, Vec<ChunkMeta>)>,
}
impl PqWriter {
    pub fn open(path: &str, cols: Vec<(&'static str, Kind)>) -> Self {
        let f = File::create(path).unwrap_or_else(|_| die(&format!("create {path}")));
        let mut f = BufWriter::with_capacity(1 << 22, f);
        f.write_all(b"PAR1").unwrap();
        PqWriter { f, cols, off: 4, nrows: 0, groups: Vec::new() }
    }
    pub fn row_group(&mut self, bufs: &[ColBuf], rows: i64) {
        if rows == 0 {
            return;
        }
        let mut metas = Vec::new();
        for (c, cb) in bufs.iter().enumerate().take(self.cols.len()) {
            let mut levels = Vec::new();
            if self.cols[c].1 == Kind::OptStr {
                // RLE/bit-packed hybrid: one bit-packed run, bit width 1
                let ng = (rows as usize).div_ceil(8);
                let mut t = TW::new();
                t.varint(((ng as u64) << 1) | 1);
                let mut run = t.b;
                let h = run.len();
                run.resize(h + ng, 0);
                for i in 0..rows as usize {
                    if cb.defs[i] != 0 {
                        run[h + i / 8] |= 1 << (i % 8);
                    }
                }
                levels.extend_from_slice(&(run.len() as u32).to_le_bytes());
                levels.extend_from_slice(&run);
            }
            let psize = (levels.len() + cb.data.len()) as i64;
            let mut h = TW::new();
            h.i32f(1, 0); // DATA_PAGE
            h.i32f(2, psize);
            h.i32f(3, psize);
            h.sbeg(5); // DataPageHeader
            h.i32f(1, rows);
            h.i32f(2, 0); // PLAIN
            h.i32f(3, 3); // RLE
            h.i32f(4, 3);
            h.send();
            h.b.push(0);
            let m = ChunkMeta { page_off: self.off, size: h.b.len() as i64 + psize, nvals: rows };
            self.f.write_all(&h.b).unwrap();
            self.f.write_all(&levels).unwrap();
            self.f.write_all(&cb.data).unwrap();
            self.off += m.size;
            metas.push(m);
        }
        self.groups.push((rows, metas));
        self.nrows += rows;
    }
    pub fn close(mut self) {
        let mut t = TW::new();
        t.i32f(1, 1);
        t.lbeg(2, 12, self.cols.len() + 1);
        t.ebeg();
        t.str(4, "schema");
        t.i32f(5, self.cols.len() as i64);
        t.send();
        for &(name, k) in &self.cols {
            t.ebeg();
            t.i32f(1, ptype(k));
            t.i32f(3, if k == Kind::OptStr { 1 } else { 0 });
            t.str(4, name);
            if k == Kind::Str || k == Kind::OptStr {
                t.i32f(6, 0); // UTF8
                t.sbeg(10); // LogicalType STRING
                t.sbeg(1);
                t.send();
                t.send();
            } else if k == Kind::Date {
                t.i32f(6, 6); // DATE
                t.sbeg(10); // LogicalType DATE
                t.sbeg(6);
                t.send();
                t.send();
            }
            t.send();
        }
        t.i64f(3, self.nrows);
        t.lbeg(4, 12, self.groups.len());
        for (rows, metas) in &self.groups {
            t.ebeg();
            t.lbeg(1, 12, self.cols.len());
            let mut total = 0;
            for (c, m) in metas.iter().enumerate() {
                total += m.size;
                t.ebeg();
                t.i64f(2, m.page_off);
                t.sbeg(3);
                t.i32f(1, ptype(self.cols[c].1));
                t.lbeg(2, 5, 2);
                t.zz(0);
                t.zz(3);
                t.lbeg(3, 8, 1);
                t.varint(self.cols[c].0.len() as u64);
                t.b.extend_from_slice(self.cols[c].0.as_bytes());
                t.i32f(4, 0);
                t.i64f(5, m.nvals);
                t.i64f(6, m.size);
                t.i64f(7, m.size);
                t.i64f(9, m.page_off);
                t.send();
                t.send();
            }
            t.i64f(2, total);
            t.i64f(3, *rows);
            t.send();
        }
        t.str(6, "shopping-sessions-rust");
        t.b.push(0);
        self.f.write_all(&t.b).unwrap();
        self.f.write_all(&(t.b.len() as u32).to_le_bytes()).unwrap();
        self.f.write_all(b"PAR1").unwrap();
        self.f.flush().unwrap_or_else(|_| die("write output"));
    }
}
