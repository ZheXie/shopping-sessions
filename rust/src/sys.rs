//! Threads, memory segments, arenas and block lists.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    pub fn _exit(code: i32) -> !;
}
const PROT_READ: i32 = 1;
const PROT_WRITE: i32 = 2;
const MAP_PRIVATE: i32 = 2;
const MAP_ANONYMOUS: i32 = 0x20;
const MADV_DONTNEED: i32 = 4;
const MADV_HUGEPAGE: i32 = 14;

pub fn die(msg: &str) -> ! {
    eprintln!("fatal: {msg}");
    std::process::exit(2)
}

/// A value several threads may reach; each thread only touches its own
/// slot or its own disjoint part, which the code guarantees.
pub struct Shared<T>(UnsafeCell<T>);
unsafe impl<T> Sync for Shared<T> {}
unsafe impl<T> Send for Shared<T> {}
impl<T> Shared<T> {
    pub fn new(v: T) -> Self {
        Shared(UnsafeCell::new(v))
    }
    #[allow(clippy::mut_from_ref)]
    #[inline(always)]
    pub fn get(&self) -> &mut T {
        unsafe { &mut *self.0.get() }
    }
    pub fn into_inner(self) -> T {
        self.0.into_inner()
    }
}
/// One per thread, on its own cache line (no false sharing).
#[repr(align(64))]
pub struct Pad<T>(pub T);

pub fn per_thread<T, F: Fn() -> T>(nt: usize, f: F) -> Vec<Shared<T>> {
    (0..nt).map(|_| Shared::new(f())).collect()
}

/// Run tasks 0..ntasks on nt threads; fn(task, thread).
pub fn parallel_for<F: Fn(usize, usize) + Sync>(ntasks: usize, nt: usize, f: F) {
    let next = AtomicUsize::new(0);
    let work = |tid: usize| loop {
        let i = next.fetch_add(1, Ordering::Relaxed);
        if i >= ntasks {
            break;
        }
        f(i, tid);
    };
    std::thread::scope(|s| {
        for t in 1..nt {
            let w = &work;
            s.spawn(move || w(t));
        }
        work(0);
    });
}

// ------------------------------------------------------------------ segments
/// Memory comes in 64 MB segments from a shared pool. A released arena hands
/// its segments back and the next arena reuses them, so pages are faulted
/// (and zeroed by the kernel) once, not once per phase.
pub const SEG: usize = 64 << 20;
static POOL: Mutex<Vec<usize>> = Mutex::new(Vec::new());
pub static mut NOTHP: bool = false;
pub static mut NOFREE: bool = false;

fn pool_get() -> *mut u8 {
    if let Some(p) = POOL.lock().unwrap().pop() {
        return p as *mut u8;
    }
    unsafe {
        let p = mmap(std::ptr::null_mut(), SEG, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if p as isize == -1 {
            die("arena mmap");
        }
        if !NOTHP {
            madvise(p, SEG, MADV_HUGEPAGE);
        }
        p
    }
}

pub struct Arena {
    segs: Vec<usize>,
    cur: usize,
    left: usize,
}
impl Arena {
    pub fn new() -> Self {
        Arena { segs: Vec::new(), cur: 0, left: 0 }
    }
    #[inline]
    pub fn alloc(&mut self, n: usize, align: usize) -> *mut u8 {
        let mut pad = (align - (self.cur & (align - 1))) & (align - 1);
        if n + pad > self.left {
            if n > SEG {
                die("allocation larger than a segment");
            }
            let p = pool_get() as usize;
            self.segs.push(p);
            self.cur = p;
            self.left = SEG;
            pad = 0;
        }
        let r = self.cur + pad;
        self.cur += n + pad;
        self.left -= n + pad;
        r as *mut u8
    }
    pub fn release(&mut self) {
        let mut pool = POOL.lock().unwrap();
        pool.extend(self.segs.drain(..));
        self.cur = 0;
        self.left = 0;
    }
}

// ------------------------------------------------------------------ block lists
/// Append-only list of 64 KB blocks (page aligned) from an Arena.
/// Block layout: next pointer at 0, item count at 8, items from 16.
pub struct BList<T> {
    head: *mut u8,
    tail: *mut u8,
    pub cnt: usize,
    _p: PhantomData<T>,
}
unsafe impl<T> Send for BList<T> {}
unsafe impl<T> Sync for BList<T> {}
impl<T: Copy> Default for BList<T> {
    fn default() -> Self {
        BList { head: std::ptr::null_mut(), tail: std::ptr::null_mut(), cnt: 0, _p: PhantomData }
    }
}
const BLK: usize = 65536;
impl<T: Copy> BList<T> {
    const BS: u32 = ((BLK - 16) / std::mem::size_of::<T>()) as u32;

    #[inline(always)]
    pub fn push(&mut self, v: T, a: &mut Arena) {
        unsafe {
            if self.tail.is_null() || *(self.tail.add(8) as *const u32) == Self::BS {
                let b = a.alloc(BLK, 4096);
                *(b as *mut *mut u8) = std::ptr::null_mut();
                *(b.add(8) as *mut u32) = 0;
                if self.tail.is_null() {
                    self.head = b;
                } else {
                    *(self.tail as *mut *mut u8) = b;
                }
                self.tail = b;
            }
            let n = self.tail.add(8) as *mut u32;
            (self.tail.add(16) as *mut T).add(*n as usize).write(v);
            *n += 1;
        }
        self.cnt += 1;
    }
    #[inline(always)]
    pub fn each<F: FnMut(&T)>(&self, mut f: F) {
        let mut b = self.head;
        while !b.is_null() {
            unsafe {
                let n = *(b.add(8) as *const u32) as usize;
                let it = b.add(16) as *const T;
                for i in 0..n {
                    f(&*it.add(i));
                }
                b = *(b as *const *mut u8);
            }
        }
    }
    /// Like each(), but each block's pages go back to the kernel right after
    /// it is read, so the freeing runs on all workers instead of serially in
    /// the exit teardown. Only for the last read of a list.
    #[inline(always)]
    pub fn consume<F: FnMut(&T)>(&mut self, mut f: F) {
        let mut b = self.head;
        while !b.is_null() {
            unsafe {
                let n = *(b.add(8) as *const u32) as usize;
                let it = b.add(16) as *const T;
                for i in 0..n {
                    f(&*it.add(i));
                }
                let nx = *(b as *const *mut u8);
                if !NOFREE {
                    madvise(b, BLK, MADV_DONTNEED);
                }
                b = nx;
            }
        }
        self.head = std::ptr::null_mut();
        self.tail = std::ptr::null_mut();
    }
}
/// [thread][bucket] lists
pub type Buckets<T> = Vec<Shared<Vec<BList<T>>>>;
pub fn make_buckets<T: Copy>(nt: usize, nb: usize) -> Buckets<T> {
    (0..nt).map(|_| Shared::new((0..nb).map(|_| BList::default()).collect())).collect()
}
