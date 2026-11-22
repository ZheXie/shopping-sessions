//! Minimal Parquet writer with one row group and one v1 data page per column.
//! Strings can use an additional dictionary page with RLE indices.

use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    I32,
    I64,
    /// A pre-encoded raw Snappy block of PLAIN int64 values.
    SnappyI64,
    Date,
    Str,
    /// Number of distinct values, followed in `parts` by their PLAIN bytes and the index stream.
    DictStr(usize),
    DictInt(usize),
    DictDate(usize),
    /// Nullable string: the payload is the definition levels as one bit per
    /// row (LSB first), then the PLAIN values of the non-null rows.
    OptStr,
    /// Dictionary, definition levels, then the indices for non-null rows.
    OptDictStr(usize),
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
const ENC_RLE_DICTIONARY: i32 = 8;

fn physical(kind: Kind) -> i32 {
    match kind {
        Kind::I32 | Kind::Date | Kind::DictInt(_) | Kind::DictDate(_) => TYPE_INT32,
        Kind::I64 | Kind::SnappyI64 => TYPE_INT64,
        Kind::Str | Kind::DictStr(_) | Kind::OptDictStr(_) | Kind::OptStr => TYPE_BYTE_ARRAY,
    }
}

fn write_all(file: &mut File, bytes: &[u8]) {
    file.write_all(bytes).expect("write parquet");
}

/// Header for a single run of dictionary indices packed in groups of eight.
pub fn packed_header(width: u8, rows: usize) -> Vec<u8> {
    let mut h = Thrift::new();
    h.buf.push(width);
    h.varint(2 * rows.div_ceil(8) as u64 + 1);
    h.buf
}

/// Encodes a sparse column of little-endian INT64 words as a raw Snappy block.
/// COPY_2 repeats the preceding zero byte, including overlapping copies.
pub struct SparseRevenue {
    data: Vec<u8>,
    emitted: usize,
    last_zero: bool,
}

impl SparseRevenue {
    pub fn new(rows: usize) -> Self {
        Self {
            data: Vec::with_capacity(rows * 2),
            emitted: 0,
            last_zero: false,
        }
    }

    fn zeros(&mut self, to: usize) {
        let mut bytes = (to - self.emitted) * 8;
        if bytes == 0 {
            return;
        }
        if !self.last_zero {
            self.data.extend_from_slice(&[0, 0]); // one literal zero
            bytes -= 1;
        }
        while bytes != 0 {
            let count = bytes.min(64);
            self.data.extend_from_slice(&[(((count - 1) << 2) | 2) as u8, 1, 0]);
            bytes -= count;
        }
    }

    #[inline]
    pub fn push(&mut self, row: usize, value: u64) {
        if value != 0 {
            self.zeros(row);
            self.data.push(7 << 2); // eight literal bytes
            self.data.extend_from_slice(&value.to_le_bytes());
            self.last_zero = value >> 56 == 0;
            self.emitted = row + 1;
        }
    }

    pub fn finish(&mut self, rows: usize) {
        if rows > self.emitted {
            self.zeros(rows);
            self.emitted = rows;
            self.last_zero = true;
        }
    }

    pub fn parts(&self, rows: usize) -> (Vec<u8>, &[u8]) {
        let mut header = Thrift::new();
        header.varint(rows as u64 * 8);
        (header.buf, &self.data)
    }
}

pub fn write_file(path: &Path, rows: usize, cols: &[Col]) {
    let mut file = File::create(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
    write_all(&mut file, b"PAR1");
    let mut offset = 4u64;
    // Column start, data-page offset, compressed and uncompressed chunk sizes.
    let mut chunks: Vec<(u64, u64, u64, u64)> = Vec::with_capacity(cols.len());
    if rows > 0 {
        for col in cols {
            let start = offset;
            let mut encoding = ENC_PLAIN;
            let mut prefix: Vec<u8> = Vec::new();
            let mut parts: &[&[u8]] = &col.parts;
            if let Kind::DictStr(entries)
            | Kind::OptDictStr(entries)
            | Kind::DictInt(entries)
            | Kind::DictDate(entries) = col.kind
            {
                let dictionary = parts[0];
                let size = i32::try_from(dictionary.len()).expect("dictionary page too large");
                let mut header = Thrift::new();
                header.i32(1, 2); // DICTIONARY_PAGE
                header.i32(2, size);
                header.i32(3, size);
                header.begin_field(7);
                header.i32(
                    1,
                    i32::try_from(entries).expect("too many dictionary entries"),
                );
                header.i32(2, ENC_PLAIN);
                header.end();
                header.buf.push(0);
                write_all(&mut file, &header.buf);
                write_all(&mut file, dictionary);
                offset += (header.buf.len() + dictionary.len()) as u64;
                encoding = ENC_RLE_DICTIONARY;
                parts = &parts[1..];
            }
            if matches!(col.kind, Kind::OptStr | Kind::OptDictStr(_)) {
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
            let plain_size = if col.kind == Kind::SnappyI64 { rows * 8 } else { size };
            let mut h = Thrift::new();
            h.i32(1, 0);
            h.i32(2, i32::try_from(plain_size).expect("uncompressed page too large"));
            h.i32(3, size as i32);
            h.begin_field(5);
            h.i32(1, rows as i32);
            h.i32(2, encoding);
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
            let data_page = offset;
            offset += chunk;
            let compressed = offset - start;
            let plain = compressed - size as u64 + plain_size as u64;
            chunks.push((start, data_page, compressed, plain));
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
        m.i32(3, if matches!(col.kind, Kind::OptStr | Kind::OptDictStr(_)) { 1 } else { 0 });
        m.string(4, col.name);
        match col.kind {
            Kind::Str | Kind::DictStr(_) | Kind::OptDictStr(_) | Kind::OptStr => {
                m.i32(6, 0);
                m.begin_field(10);
                m.begin_field(1);
                m.end();
                m.end();
            }
            Kind::Date | Kind::DictDate(_) => {
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
        for (col, &(start, page, size, plain)) in cols.iter().zip(&chunks) {
            m.begin();
            m.i64(2, start as i64);
            m.begin_field(3);
            m.i32(1, physical(col.kind));
            let dictionary = matches!(
                col.kind,
                Kind::DictStr(_) | Kind::OptDictStr(_) | Kind::DictDate(_) | Kind::DictInt(_)
            );
            m.list(2, T_I32, if dictionary { 3 } else { 2 });
            m.zigzag(ENC_PLAIN as i64);
            m.zigzag(ENC_RLE as i64);
            if dictionary {
                m.zigzag(ENC_RLE_DICTIONARY as i64);
            }
            m.list(3, T_BINARY, 1);
            m.varint(col.name.len() as u64);
            m.buf.extend_from_slice(col.name.as_bytes());
            m.i32(4, i32::from(col.kind == Kind::SnappyI64));
            m.i64(5, rows as i64);
            m.i64(6, plain as i64);
            m.i64(7, size as i64);
            m.i64(9, page as i64);
            if dictionary {
                m.i64(11, start as i64);
            }
            m.end();
            m.end();
        }
        m.i64(2, chunks.iter().map(|&(_, _, _, plain)| plain as i64).sum());
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
