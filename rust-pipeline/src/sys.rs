//! Memory mapping, a bump slab backed by huge pages, partition writers, threads.

use std::fs::File;
use std::marker::PhantomData;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr::null_mut;

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
            libc::mmap(base, len, libc::PROT_READ, libc::MAP_PRIVATE | libc::MAP_FIXED, f.as_raw_fd(), 0)
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

/// Bump allocator over `anon` chunks. Memory is never returned.
pub struct Slab {
    cur: usize,
    end: usize,
}

impl Slab {
    pub const fn new() -> Self {
        Slab { cur: 0, end: 0 }
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
        let size = bytes.max(32 << 20);
        let p = anon(size) as usize;
        self.cur = p;
        self.end = p + size;
    }
}

/// A run of `n` records at `ptr`.
#[derive(Clone, Copy)]
pub struct Blk {
    pub ptr: usize,
    pub n: u32,
}

/// Appends records to one of `parts` partitions, in fixed-size blocks carved
/// from a slab. Records never move, so pointers to them stay valid.
pub struct PartWriter<T> {
    cur: Vec<(usize, usize)>,
    start: Vec<usize>,
    done: Vec<Vec<Blk>>,
    block_bytes: usize,
    _marker: PhantomData<T>,
}

impl<T: Copy> PartWriter<T> {
    pub fn new(parts: usize, records_per_block: usize) -> Self {
        PartWriter {
            cur: vec![(0, 0); parts],
            start: vec![0; parts],
            done: (0..parts).map(|_| Vec::new()).collect(),
            block_bytes: records_per_block * std::mem::size_of::<T>(),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub unsafe fn push(&mut self, part: usize, slab: &mut Slab, value: T) -> usize {
        if {
            let c = self.cur.get_unchecked(part);
            c.0 == c.1
        } {
            self.refill(part, slab);
        }
        let c = self.cur.get_unchecked_mut(part);
        let dst = c.0;
        (dst as *mut T).write_unaligned(value);
        c.0 = dst + std::mem::size_of::<T>();
        dst
    }

    #[cold]
    #[inline(never)]
    fn refill(&mut self, part: usize, slab: &mut Slab) {
        self.close(part);
        let bytes = (self.block_bytes + 15) & !15;
        let b = slab.alloc(bytes);
        self.start[part] = b;
        self.cur[part] = (b, b + self.block_bytes);
    }

    fn close(&mut self, part: usize) {
        let start = self.start[part];
        let cur = self.cur[part].0;
        if start != 0 && cur > start {
            let n = ((cur - start) / std::mem::size_of::<T>()) as u32;
            self.done[part].push(Blk { ptr: start, n });
        }
    }

    pub fn finish(mut self) -> Vec<Vec<Blk>> {
        for part in 0..self.cur.len() {
            self.close(part);
        }
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
