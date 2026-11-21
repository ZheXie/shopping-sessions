//! Memory mapping, a bump slab backed by huge pages, partition writers, threads.

use std::arch::x86_64::{__m256i, _mm256_load_si256, _mm256_stream_si256, _mm_sfence};
use std::fs::File;
use std::marker::PhantomData;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Readable slack guaranteed after every `Buf`, so wide loads never fault.
pub const SLACK: usize = 64;
const PAGE: usize = 4096;

/// File contents. Invariants: `len == 0` or the last byte is `\n`, and
/// `SLACK` bytes after `len` are readable.
#[derive(Clone, Copy)]
pub struct Buf {
    pub ptr: *const u8,
    pub len: usize,
}
unsafe impl Send for Buf {}
unsafe impl Sync for Buf {}

static EMPTY: [u8; SLACK] = [0; SLACK];

fn heap_copy(src: &[u8]) -> Buf {
    let mut v = Vec::with_capacity(src.len() + 1 + SLACK);
    v.extend_from_slice(src);
    if v.last() != Some(&b'\n') {
        v.push(b'\n');
    }
    let len = v.len();
    v.resize(len + SLACK, 0);
    Buf { ptr: v.leak().as_ptr(), len }
}

pub fn map_file(path: &Path) -> Buf {
    let f = File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
    if len == 0 {
        return Buf { ptr: EMPTY.as_ptr(), len: 0 };
    }
    unsafe {
        let pop = if std::env::var("SS_POP").is_ok() { libc::MAP_POPULATE } else { 0 };
        // Reserve one zero page past the file so the slack is always readable.
        let span = ((len + PAGE - 1) & !(PAGE - 1)) + PAGE;
        let base = libc::mmap(
            null_mut(),
            span,
            libc::PROT_READ,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        let p = if base == libc::MAP_FAILED {
            libc::MAP_FAILED
        } else {
            libc::mmap(base, len, libc::PROT_READ, libc::MAP_PRIVATE | libc::MAP_FIXED | pop, f.as_raw_fd(), 0)
        };
        if p == libc::MAP_FAILED {
            let v = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            return heap_copy(&v);
        }
        let ptr = p as *const u8;
        if *ptr.add(len - 1) != b'\n' {
            let b = heap_copy(std::slice::from_raw_parts(ptr, len));
            libc::munmap(base, span);
            return b;
        }
        Buf { ptr, len }
    }
}

/// Zero-filled anonymous memory, 2 MiB aligned, huge pages requested.
pub fn anon(bytes: usize) -> *mut u8 {
    const HP: usize = 2 << 20;
    let size = (bytes.max(1) + HP - 1) & !(HP - 1);
    unsafe {
        let p = libc::mmap(
            null_mut(),
            size + HP,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        if p == libc::MAP_FAILED {
            panic!("anonymous mmap of {size} bytes failed");
        }
        let aligned = (p as usize + HP - 1) & !(HP - 1);
        libc::madvise(aligned as *mut libc::c_void, size, libc::MADV_HUGEPAGE);
        aligned as *mut u8
    }
}

/// One reserved range of address space that every thread's records are
/// carved from, so a record is named by a small offset.
pub struct Arena {
    pub base: usize,
    size: usize,
    next: AtomicUsize,
}

impl Arena {
    pub const UNIT: usize = 8;
    /// Offsets in `UNIT`s fit in 31 bits.
    const MAX: usize = 16 << 30;

    pub fn new() -> Arena {
        const HP: usize = 2 << 20;
        let mut size = Self::MAX;
        loop {
            let p = unsafe {
                libc::mmap(
                    null_mut(),
                    size + HP,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                    -1,
                    0,
                )
            };
            if p != libc::MAP_FAILED {
                let base = (p as usize + HP - 1) & !(HP - 1);
                unsafe { libc::madvise(base as *mut libc::c_void, size, libc::MADV_HUGEPAGE) };
                return Arena { base, size: size - HP, next: AtomicUsize::new(HP) };
            }
            size /= 2;
            assert!(size >= 64 << 20, "cannot reserve address space");
        }
    }

    fn chunk(&self, bytes: usize) -> usize {
        let at = self.next.fetch_add(bytes, Relaxed);
        assert!(at + bytes <= self.size, "record arena exhausted");
        self.base + at
    }
}

/// Bump allocator over chunks of an `Arena`. Memory is never returned.
pub struct Slab<'a> {
    arena: &'a Arena,
    cur: usize,
    end: usize,
}

impl<'a> Slab<'a> {
    pub fn new(arena: &'a Arena) -> Self {
        Slab { arena, cur: 0, end: 0 }
    }

    #[inline(always)]
    pub fn alloc(&mut self, bytes: usize) -> usize {
        if self.end - self.cur < bytes {
            self.grow(bytes);
        }
        let p = self.cur;
        self.cur += bytes;
        p
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self, bytes: usize) {
        let size = bytes.max(4 << 20);
        self.cur = self.arena.chunk(size);
        self.end = self.cur + size;
    }
}

/// Backs `[ptr, ptr + len)` with zeroed memory now. A region first touched by
/// a read is mapped to the shared zero page and then never gets huge pages;
/// writing it first does.
pub fn populate_write(ptr: *mut u8, len: usize) {
    const MADV_POPULATE_WRITE: libc::c_int = 23;
    unsafe {
        if libc::madvise(ptr as *mut libc::c_void, len, MADV_POPULATE_WRITE) != 0 {
            let mut at = 0;
            while at < len {
                ptr.add(at).write_volatile(0);
                at += PAGE;
            }
        }
    }
}

/// Maps the file pages behind `[ptr, ptr + len)` in one call, which is cheaper
/// than faulting them in one by one. Best effort.
pub fn populate(ptr: *const u8, len: usize) {
    const MADV_POPULATE_READ: libc::c_int = 22;
    let from = ptr as usize & !(PAGE - 1);
    let to = (ptr as usize + len + PAGE - 1) & !(PAGE - 1);
    unsafe {
        libc::madvise(from as *mut libc::c_void, to - from, MADV_POPULATE_READ);
    }
}

/// A run of `n` records at `ptr`.
#[derive(Clone, Copy)]
pub struct Blk {
    pub ptr: usize,
    pub n: u32,
}

#[derive(Clone, Copy)]
struct Head {
    /// Where the staged records will land.
    dst: usize,
    /// End of the current block.
    end: usize,
    /// Start of the current block; 0 before the first.
    start: usize,
    /// Bytes staged.
    fill: usize,
}

const BLOCK_BYTES: usize = 6144;

/// Appends records to one of `parts` partitions. Records are staged in a
/// small per-partition buffer and moved to their block with non-temporal
/// stores a few cache lines at a time, so the scatter touches few pages and
/// never reads its destination. A record's final address is known when it is
/// pushed and never changes.
pub struct PartWriter<T> {
    heads: Vec<Head>,
    stage: *mut u8,
    /// Staging bytes per partition: a multiple of both 64 and the record size.
    wc: usize,
    done: Vec<Vec<Blk>>,
    _marker: PhantomData<T>,
}

impl<T: Copy> PartWriter<T> {
    pub fn new(parts: usize) -> Self {
        let size = std::mem::size_of::<T>();
        let mut wc = 64;
        while wc % size != 0 {
            wc += 64;
        }
        assert!(BLOCK_BYTES % wc == 0);
        PartWriter {
            heads: vec![Head { dst: 0, end: 0, start: 0, fill: 0 }; parts],
            stage: anon(parts * wc),
            wc,
            done: (0..parts).map(|_| Vec::new()).collect(),
            _marker: PhantomData,
        }
    }

    /// Returns the address the record will have once it is flushed.
    #[inline(always)]
    pub unsafe fn push(&mut self, part: usize, slab: &mut Slab, value: T) -> usize {
        let wc = self.wc;
        let mut h = *self.heads.get_unchecked(part);
        if h.dst == h.end {
            self.refill(part, slab);
            h = *self.heads.get_unchecked(part);
        }
        let stage = self.stage.add(part * wc);
        (stage.add(h.fill) as *mut T).write_unaligned(value);
        let addr = h.dst + h.fill;
        h.fill += std::mem::size_of::<T>();
        if h.fill == wc {
            let mut i = 0;
            while i < wc {
                let v = _mm256_load_si256(stage.add(i) as *const __m256i);
                _mm256_stream_si256((h.dst + i) as *mut __m256i, v);
                i += 32;
            }
            h.dst += wc;
            h.fill = 0;
        }
        *self.heads.get_unchecked_mut(part) = h;
        addr
    }

    #[cold]
    #[inline(never)]
    fn refill(&mut self, part: usize, slab: &mut Slab) {
        self.close(part);
        let b = slab.alloc(BLOCK_BYTES);
        self.heads[part] = Head { dst: b, end: b + BLOCK_BYTES, start: b, fill: 0 };
    }

    fn close(&mut self, part: usize) {
        let h = self.heads[part];
        if h.start == 0 {
            return;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(self.stage.add(part * self.wc), h.dst as *mut u8, h.fill);
        }
        let n = (h.dst + h.fill - h.start) / std::mem::size_of::<T>();
        if n > 0 {
            self.done[part].push(Blk { ptr: h.start, n: n as u32 });
        }
    }

    pub fn finish(mut self) -> Vec<Vec<Blk>> {
        for part in 0..self.heads.len() {
            self.close(part);
        }
        unsafe { _mm_sfence() };
        self.done
    }
}

/// Runs `f(thread_index)` on `threads` threads and returns the results in order.
pub fn par_map<R: Send, F: Fn(usize) -> R + Sync>(threads: usize, f: F) -> Vec<R> {
    if threads <= 1 {
        return vec![f(0)];
    }
    std::thread::scope(|scope| {
        let handles: Vec<_> = (1..threads)
            .map(|t| {
                let f = &f;
                scope.spawn(move || f(t))
            })
            .collect();
        let mut out = Vec::with_capacity(threads);
        out.push(f(0));
        for h in handles {
            out.push(h.join().expect("worker thread panicked"));
        }
        out
    })
}
