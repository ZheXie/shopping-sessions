//! Memory mapping, a bump slab backed by huge pages, partition writers, threads.

use std::fs::File;
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
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
    /// Exactly the bytes on disk, including the terminating newline.
    pub can_pread: bool,
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
    Buf {
        ptr: v.leak().as_ptr(),
        len,
        can_pread: false,
    }
}

pub fn map_file(path: &Path) -> Buf {
    let f = File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
    if len == 0 {
        return Buf {
            ptr: EMPTY.as_ptr(),
            len: 0,
            can_pread: false,
        };
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
            libc::mmap(
                base,
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE | libc::MAP_FIXED,
                f.as_raw_fd(),
                0,
            )
        };
        if p == libc::MAP_FAILED {
            let v = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            return heap_copy(&v);
        }
        let ptr = p as *const u8;
        if *ptr.add(len - 1) != b'\n' {
            // Extend the last line privately. Only its last page needs a
            // copy, even when the file is several gigabytes long. For an
            // exact page multiple, the newline lands on the reserved page.
            let tail = ptr.add(len) as *mut u8;
            let page = (tail as usize & !(PAGE - 1)) as *mut libc::c_void;
            if libc::mprotect(page, PAGE, libc::PROT_READ | libc::PROT_WRITE) == 0 {
                tail.write(b'\n');
                return Buf {
                    ptr,
                    len: len + 1,
                    can_pread: false,
                };
            }
            let b = heap_copy(std::slice::from_raw_parts(ptr, len));
            libc::munmap(base, span);
            return b;
        }
        Buf {
            ptr,
            len,
            can_pread: true,
        }
    }
}

/// One reusable read buffer and file handle per parser thread. A pread
/// avoids installing and later tearing down a PTE for every event-file page.
#[derive(Default)]
pub struct EventReader {
    file: Option<(usize, File)>,
    bytes: Vec<u8>,
}

impl EventReader {
    pub fn read(&mut self, path: &Path, buf: Buf, start: usize, len: usize) -> *const u8 {
        let id = buf.ptr as usize;
        if self.file.as_ref().map(|(key, _)| *key) != Some(id) {
            let f = File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
            self.file = Some((id, f));
        }
        if self.bytes.len() < len + SLACK {
            self.bytes.resize(len + SLACK, 0);
        }
        self.file
            .as_ref()
            .unwrap()
            .1
            .read_exact_at(&mut self.bytes[..len], start as u64)
            .unwrap_or_else(|e| panic!("read {} at {start}: {e}", path.display()));
        self.bytes.as_ptr()
    }
}

/// Page-aligned zero-filled memory. Large allocations request huge pages.
pub fn anon(bytes: usize) -> *mut u8 {
    const HP: usize = 2 << 20;
    if bytes < HP / 2 {
        let p = unsafe {
            libc::mmap(
                null_mut(),
                bytes.max(PAGE),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        assert_ne!(p, libc::MAP_FAILED, "cannot allocate {bytes} bytes");
        return p.cast();
    }
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
                return Arena {
                    base,
                    size: size - HP,
                    next: AtomicUsize::new(HP),
                };
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
        Slab {
            arena,
            cur: 0,
            end: 0,
        }
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

// 63 cache lines. Successive blocks start at different cache set indices;
// both the 16-byte and 24-byte records divide the block exactly.
const BLOCK_BYTES: usize = 63 * 64;

#[derive(Clone, Copy, Default)]
struct Cursor {
    next: usize,
    end: usize,
}

/// One cursor per partition keeps the next store and its block boundary
/// together. Record addresses never change, so dedup can invalidate a
/// payload in place even after its writer has moved on to another block.
pub struct PartWriter<T> {
    cursors: Vec<Cursor>,
    blocks: Vec<Vec<Blk>>,
    _record: PhantomData<T>,
}

impl<T: Copy> PartWriter<T> {
    pub fn new(parts: usize) -> Self {
        assert_eq!(BLOCK_BYTES % std::mem::size_of::<T>(), 0);
        PartWriter {
            cursors: vec![Cursor::default(); parts],
            blocks: (0..parts).map(|_| Vec::new()).collect(),
            _record: PhantomData,
        }
    }

    #[inline(always)]
    pub unsafe fn push(&mut self, part: usize, slab: &mut Slab, value: T) -> usize {
        let mut cursor = *self.cursors.get_unchecked(part);
        if cursor.next == cursor.end {
            cursor = self.new_block(part, slab);
        }
        (cursor.next as *mut T).write(value);
        let at = cursor.next;
        cursor.next += std::mem::size_of::<T>();
        *self.cursors.get_unchecked_mut(part) = cursor;
        at
    }

    #[cold]
    #[inline(never)]
    fn new_block(&mut self, part: usize, slab: &mut Slab) -> Cursor {
        let ptr = slab.alloc(BLOCK_BYTES);
        self.blocks[part].push(Blk {
            ptr,
            n: (BLOCK_BYTES / std::mem::size_of::<T>()) as u32,
        });
        Cursor {
            next: ptr,
            end: ptr + BLOCK_BYTES,
        }
    }

    pub fn finish(mut self) -> Vec<Vec<Blk>> {
        for (blocks, cursor) in self.blocks.iter_mut().zip(self.cursors) {
            if let Some(last) = blocks.last_mut() {
                last.n = ((cursor.next - last.ptr) / std::mem::size_of::<T>()) as u32;
            }
        }
        self.blocks
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
