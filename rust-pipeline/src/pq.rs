//! Minimal Parquet writer: one row group, one PLAIN uncompressed v1 data page
//! per column. Callers hand over each column already PLAIN-encoded.

use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    I32,
    I64,
    Date,
    Str,
    /// Nullable string: the payload is the definition levels as one bit per
    /// row (LSB first), then the PLAIN values of the non-null rows.
    OptStr,
}

pub struct Col<'a> {
    pub name: &'static str,
    pub kind: Kind,
    pub parts: Vec<&'a [u8]>,
}

pub fn bytes_of<T: Copy>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

/// Thrift compact protocol.
struct Thrift {
    buf: Vec<u8>,
    last: i16,
    stack: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift {
            buf: Vec::with_capacity(1024),
            last: 0,
            stack: Vec::new(),
        }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push(v as u8 | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let delta = id - self.last;
        if (1..=15).contains(&delta) {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            self.zigzag(id as i64);
        }
        self.last = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zigzag(v);
    }
    fn string(&mut self, id: i16, s: &str) {
        self.field(id, T_BINARY);
        self.varint(s.len() as u64);
        self.buf.extend_from_slice(s.as_bytes());
    }
    fn list_header(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        self.list_header(elem, n);
    }
    fn begin(&mut self) {
        self.stack.push(self.last);
        self.last = 0;
    }
    fn begin_field(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.begin();
    }
    fn end(&mut self) {
        self.buf.push(0);
        self.last = self.stack.pop().unwrap();
    }
}

const TYPE_INT32: i32 = 1;
const TYPE_INT64: i32 = 2;
const TYPE_BYTE_ARRAY: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;

fn physical(kind: Kind) -> i32 {
    match kind {
        Kind::I32 | Kind::Date => TYPE_INT32,
        Kind::I64 => TYPE_INT64,
        Kind::Str | Kind::OptStr => TYPE_BYTE_ARRAY,
    }
}

fn write_all(file: &mut File, bytes: &[u8]) {
    file.write_all(bytes).expect("write parquet");
}

pub fn write_file(path: &Path, rows: usize, cols: &[Col]) {
    let mut file = File::create(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
    write_all(&mut file, b"PAR1");
    let mut offset = 4u64;
    // (page offset, chunk size) per column.
    let mut chunks: Vec<(u64, u64)> = Vec::with_capacity(cols.len());
    if rows > 0 {
        for col in cols {
            let mut prefix: Vec<u8> = Vec::new();
            let mut parts: &[&[u8]] = &col.parts;
            if col.kind == Kind::OptStr {
                // RLE/bit-packed hybrid, bit width 1: a single bit-packed run.
                let groups = rows.div_ceil(8);
                let mut run = Thrift::new();
                run.varint(((groups as u64) << 1) | 1);
                let levels = &parts[0][..groups];
                prefix.extend_from_slice(&((run.buf.len() + groups) as u32).to_le_bytes());
                prefix.extend_from_slice(&run.buf);
                prefix.extend_from_slice(levels);
                parts = &parts[1..];
            }
            let size: usize = prefix.len() + parts.iter().map(|p| p.len()).sum::<usize>();
            assert!(size < i32::MAX as usize, "parquet page too large");
            let mut h = Thrift::new();
            h.i32(1, 0);
            h.i32(2, size as i32);
            h.i32(3, size as i32);
            h.begin_field(5);
            h.i32(1, rows as i32);
            h.i32(2, ENC_PLAIN);
            h.i32(3, ENC_RLE);
            h.i32(4, ENC_RLE);
            h.end();
            h.buf.push(0);
            write_all(&mut file, &h.buf);
            write_all(&mut file, &prefix);
            for p in parts {
                write_all(&mut file, p);
            }
            let chunk = (h.buf.len() + size) as u64;
            chunks.push((offset, chunk));
            offset += chunk;
        }
    }

    let mut m = Thrift::new();
    m.i32(1, 1);
    m.list(2, T_STRUCT, cols.len() + 1);
    m.begin();
    m.i32(3, 0);
    m.string(4, "schema");
    m.i32(5, cols.len() as i32);
    m.end();
    for col in cols {
        m.begin();
        m.i32(1, physical(col.kind));
        m.i32(3, if col.kind == Kind::OptStr { 1 } else { 0 });
        m.string(4, col.name);
        match col.kind {
            Kind::Str | Kind::OptStr => {
                m.i32(6, 0);
                m.begin_field(10);
                m.begin_field(1);
                m.end();
                m.end();
            }
            Kind::Date => {
                m.i32(6, 6);
                m.begin_field(10);
                m.begin_field(6);
                m.end();
                m.end();
            }
            _ => {}
        }
        m.end();
    }
    m.i64(3, rows as i64);
    if rows > 0 {
        m.list(4, T_STRUCT, 1);
        m.begin();
        m.list(1, T_STRUCT, cols.len());
        for (col, &(page, size)) in cols.iter().zip(&chunks) {
            m.begin();
            m.i64(2, page as i64);
            m.begin_field(3);
            m.i32(1, physical(col.kind));
            m.list(2, T_I32, 2);
            m.zigzag(ENC_PLAIN as i64);
            m.zigzag(ENC_RLE as i64);
            m.list(3, T_BINARY, 1);
            m.varint(col.name.len() as u64);
            m.buf.extend_from_slice(col.name.as_bytes());
            m.i32(4, 0);
            m.i64(5, rows as i64);
            m.i64(6, size as i64);
            m.i64(7, size as i64);
            m.i64(9, page as i64);
            m.end();
            m.end();
        }
        m.i64(2, (offset - 4) as i64);
        m.i64(3, rows as i64);
        m.end();
    } else {
        m.list(4, T_STRUCT, 0);
    }
    m.string(6, "shopping-sessions");
    m.buf.push(0);
    let len = m.buf.len() as u32;
    m.buf.extend_from_slice(&len.to_le_bytes());
    m.buf.extend_from_slice(b"PAR1");
    write_all(&mut file, &m.buf);
}
