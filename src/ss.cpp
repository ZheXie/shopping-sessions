// Shopping Sessions: fast pipeline (C++17, Linux, AVX2).
//
// Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet,
// exactly as rules.md defines. Logs phase timings to stderr.
//
// Phases:
//   L. links: parse, intern IDs per hash bucket, then union-find on a
//      background thread (person name = byte-wise smallest ID) while ...
//   1. events and clicks are parsed in parallel with pread into small
//      per-thread buffers. Each valid, non-late event copy goes straight to its
//      user-ID bucket; every well-formed copy also gets a small dedup entry in
//      its event-ID bucket. Valid clicks go to their user-ID bucket.
//   2. dedup per event-ID bucket: the earliest ingest_ts wins, every other
//      valid copy is marked dead in place.
//   3. per user-ID bucket: map events and clicks to their person.
//   4. per person bucket: sessionize, credit, assists, daily aggregates.
//   5. Parquet output (hand-written writer, PLAIN, uncompressed).
//
// No record points into the input: IDs travel inline (first 16 bytes), and
// the rare longer ID is copied to an arena. So input buffers are reused and
// nothing large has to be unmapped at exit.

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <mutex>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>
#if defined(__AVX2__)
#include <immintrin.h>
#endif

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int64_t i64;

static const i64 DAY_MS = 86400000LL;
static const i64 GAP_MS = 1800000LL;
static const i64 WEEK_MS = 7 * DAY_MS;
static const int NB = 256;     // hash buckets
static const int BSH = 56;     // bucket = hash >> BSH
static const int LBITS = 22;   // packed link node: bucket << LBITS | local

static auto T0 = std::chrono::steady_clock::now();
static void lap(const char* what) {
  static auto last = T0;
  auto now = std::chrono::steady_clock::now();
  fprintf(stderr, "%-12s %7.3fs  (total %.3fs)\n", what,
          std::chrono::duration<double>(now - last).count(), std::chrono::duration<double>(now - T0).count());
  last = now;
}
static void die(const char* msg) {
  fprintf(stderr, "fatal: %s (errno %d)\n", msg, errno);
  exit(2);
}

// ---------------------------------------------------------------- hashing
static inline u64 ld64(const char* p) { u64 v; memcpy(&v, p, 8); return v; }
static inline u64 mum(u64 a, u64 b) {
  __uint128_t r = (__uint128_t)a * b;
  return (u64)r ^ (u64)(r >> 64);
}
static const u64 K0 = 0xa0761d6478bd642fULL, K1 = 0xe7037ed1a0b428dbULL,
                 K2 = 0x8ebc6af09c88c6e3ULL, K3 = 0x589965cc75374cc3ULL;
static inline u64 pow2ge(u64 x) { u64 c = 16; while (c < x) c <<= 1; return c; }

// wyhash-style hash of any byte string; reads only inside [p, p+n)
static inline u64 ld32(const char* p) { u32 v; memcpy(&v, p, 4); return v; }
static inline u64 hbytes(const char* p, u32 n, u64 seed0 = K0) {
  u64 seed = seed0 ^ ((u64)n * K3);
  u64 a, b;
  if (n <= 16) {
    if (n >= 8) { a = ld64(p); b = ld64(p + n - 8); }
    else if (n >= 4) { a = (ld32(p) << 32) | ld32(p + n - 4); b = 0; }
    else if (n > 0) { a = ((u64)(u8)p[0] << 16) | ((u64)(u8)p[n >> 1] << 8) | (u8)p[n - 1]; b = 0; }
    else { a = b = 0; }
  } else {
    u32 i = n;
    const char* q = p;
    while (i > 16) { seed = mum(ld64(q) ^ K1, ld64(q + 8) ^ seed); q += 16; i -= 16; }
    a = ld64(p + n - 16); b = ld64(p + n - 8);
  }
  u64 h = mum(a ^ K1, b ^ seed);
  return mum(h ^ K2, (u64)n ^ K1);
}
static inline u64 hstr(const char* p, u32 n) { return hbytes(p, n); }

// An ID in exactly 16 bytes plus a length byte L:
//   L <= 16: the bytes themselves, zero padded (exact, no pointer)
//   L == LONG: a = pointer to a LongId copy in an arena, b = hash of the bytes
// Equal strings always get the same form, so comparing (L, a, b) is exact for
// short IDs; long ones also compare their bytes.
struct Id { u64 a, b; };
struct LongId { u32 n; char s[4]; };
static const u8 LONG = 255;
static inline bool sless(const char* a, u32 an, const char* b, u32 bn) {  // byte-wise order
  u32 m = an < bn ? an : bn;
  int c = memcmp(a, b, m);
  return c < 0 || (c == 0 && an < bn);
}
static inline bool seq(const char* a, u32 an, const char* b, u32 bn) {
  return an == bn && (a == b || memcmp(a, b, an) == 0);
}
static inline u64 id_hash(const Id& k, u8 L) {
  if (L == LONG) return k.b;
  u64 h = mum(k.a ^ K1, k.b ^ K0 ^ ((u64)L * K3));
  return mum(h ^ K2, (u64)L ^ K1);
}
static inline bool id_eq(const Id& x, u8 lx, const Id& y, u8 ly) {
  if (lx != ly || x.b != y.b) return false;
  if (lx != LONG) return x.a == y.a;
  const LongId* p = (const LongId*)x.a;
  const LongId* q = (const LongId*)y.a;
  return p == q || (p->n == q->n && memcmp(p->s, q->s, p->n) == 0);
}
static inline const char* id_bytes(const Id& k, u8 L, u32& n) {
  if (L != LONG) { n = L; return (const char*)&k; }
  const LongId* p = (const LongId*)k.a;
  n = p->n;
  return p->s;
}
static inline bool id_less(const Id& x, u8 lx, const Id& y, u8 ly) {
  u32 xn, yn;
  const char* xp = id_bytes(x, lx, xn);
  const char* yp = id_bytes(y, ly, yn);
  return sless(xp, xn, yp, yn);
}

// ---------------------------------------------------------------- threads
static int NT = 8;
template <class F>
static void parallel_for(size_t ntasks, F fn) {
  std::atomic<size_t> next(0);
  auto work = [&](int tid) {
    for (;;) {
      size_t i = next.fetch_add(1, std::memory_order_relaxed);
      if (i >= ntasks) break;
      fn(i, tid);
    }
  };
  std::vector<std::thread> ts;
  for (int t = 1; t < NT; t++) ts.emplace_back(work, t);
  work(0);
  for (auto& t : ts) t.join();
}

// ---------------------------------------------------------------- arenas
// Bump allocator over 64 MB segments. A released arena returns its segments
// to a shared pool and the next arena reuses them, so pages are faulted (and
// zeroed by the kernel) once, not once per phase: without transparent
// hugepages, faults and munmap are a large part of the run time.
static const size_t SEG = (size_t)64 << 20;
struct SegPool {
  std::mutex m;
  std::vector<char*> free, all;
  char* get() {
    {
      std::lock_guard<std::mutex> g(m);
      if (!free.empty()) { char* p = free.back(); free.pop_back(); return p; }
    }
    void* p = mmap(nullptr, SEG, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED) die("arena mmap");
    if (!getenv("SS_NOTHP")) madvise(p, SEG, MADV_HUGEPAGE);
    std::lock_guard<std::mutex> g(m);
    all.push_back((char*)p);
    return (char*)p;
  }
  void put(char* p) { std::lock_guard<std::mutex> g(m); free.push_back(p); }
};
static SegPool POOL;
struct Arena {
  std::vector<char*> segs;
  char* cur = nullptr;
  size_t left = 0;
  void* alloc(size_t n, size_t align = 64) {
    size_t pad = (align - ((uintptr_t)cur & (align - 1))) & (align - 1);
    if (n + pad > left) {
      if (n > SEG) die("allocation larger than a segment");
      char* p = POOL.get();
      segs.push_back(p);
      cur = p; left = SEG; pad = 0;
    }
    void* r = cur + pad;
    cur += n + pad; left -= n + pad;
    return r;
  }
  void release() {
    for (char* s : segs) POOL.put(s);
    segs.clear(); cur = nullptr; left = 0;
  }
};
template <class T>
struct BList {  // append-only list of fixed-size blocks from an Arena
  enum { BS = (65536 - 16) / sizeof(T) };
  struct Blk { Blk* next; u32 n; T it[BS]; };
  Blk* head = nullptr;
  Blk* tail = nullptr;
  size_t cnt = 0;
  inline T* push(const T& v, Arena& a) {
    if (!tail || tail->n == BS) {
      Blk* b = (Blk*)a.alloc(sizeof(Blk));
      b->next = nullptr; b->n = 0;
      if (tail) tail->next = b; else head = b;
      tail = b;
    }
    T* r = &tail->it[tail->n++];
    *r = v;
    cnt++;
    return r;
  }
  template <class F> inline void each(F f) {
    for (Blk* b = head; b; b = b->next) for (u32 i = 0; i < b->n; i++) f(b->it[i]);
  }
};
template <class T> using Buckets = std::vector<std::vector<BList<T>>>;  // [thread][bucket]
template <class T> static Buckets<T> make_buckets() { return Buckets<T>(NT, std::vector<BList<T>>(NB)); }

// ---------------------------------------------------------------- CSV dialect
struct Fld { const char* p; u32 n; };
// Split one line (no '\n') into exactly nf fields. Quotes are syntax only when
// they wrap a whole field; any other quote, a wrong count, or an empty line is
// malformed.
static inline bool split(const char* s, const char* e, Fld* f, int nf) {
  if (s == e) return false;
  int k = 0;
  const char* fs = s;
  int q = 0;
  for (const char* c = s;; ++c) {
    if (c == e || *c == ',') {
      if (k >= nf) return false;
      u32 len = (u32)(c - fs);
      if (q) {
        if (q != 2 || len < 2 || fs[0] != '"' || c[-1] != '"') return false;
        f[k].p = fs + 1; f[k].n = len - 2;
      } else { f[k].p = fs; f[k].n = len; }
      ++k;
      if (c == e) break;
      fs = c + 1; q = 0;
    } else if (*c == '"') {
      ++q;
    }
  }
  return k == nf;
}
#if defined(__AVX2__)
typedef unsigned __int128 u128;
static inline u64 mask64(const char* p, __m256i v) {
  __m256i a = _mm256_loadu_si256((const __m256i*)p), b = _mm256_loadu_si256((const __m256i*)(p + 32));
  return (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) |
         ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) << 32);
}
static inline int ctz128(u128 x) {
  u64 lo = (u64)x;
  return lo ? __builtin_ctzll(lo) : 64 + __builtin_ctzll((u64)(x >> 64));
}
static inline int pop128(u128 x) { return __builtin_popcountll((u64)x) + __builtin_popcountll((u64)(x >> 64)); }
// The line at p and the 128 bytes from p are readable. 1 ok, 0 malformed,
// -1 no '\n' in those 128 bytes. *le = the line's '\n' unless -1.
static inline int split_simd(const char* p, Fld* f, int nf, const char** le) {
  const __m256i NL = _mm256_set1_epi8('\n'), CM = _mm256_set1_epi8(','), QT = _mm256_set1_epi8('"');
  u128 N = mask64(p, NL);
  u128 C = mask64(p, CM), Q = mask64(p, QT);
  if (!N) {
    N = (u128)mask64(p + 64, NL) << 64;
    if (!N) return -1;
    C |= (u128)mask64(p + 64, CM) << 64;
    Q |= (u128)mask64(p + 64, QT) << 64;
  }
  int L = ctz128(N);
  *le = p + L;
  if (L == 0) return 0;
  u128 lm = ((u128)1 << L) - 1;
  C &= lm; Q &= lm;
  if (pop128(C) != nf - 1) return 0;
  int a = 0;
  for (int k = 0; k < nf; k++) {
    int b;
    if (k < nf - 1) { b = ctz128(C); C &= C - 1; } else b = L;
    if (Q) {
      u128 rm = (((u128)1 << b) - 1) & ~(((u128)1 << a) - 1);
      u128 q = Q & rm;
      if (q) {
        if (b - a < 2 || q != (((u128)1 << a) | ((u128)1 << (b - 1)))) return 0;
        f[k].p = p + a + 1; f[k].n = (u32)(b - a - 2);
        a = b + 1;
        continue;
      }
    }
    f[k].p = p + a; f[k].n = (u32)(b - a);
    a = b + 1;
  }
  return 1;
}
#endif

// ---------------------------------------------------------------- timestamps
static inline i64 days_from_civil(i64 y, i64 m, i64 d) {
  y -= m <= 2;
  i64 era = (y >= 0 ? y : y - 399) / 400;
  i64 yoe = y - era * 400;
  i64 doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  i64 doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}
static inline bool d2(const char* p, int& v) {
  unsigned a = (unsigned)(u8)p[0] - '0', b = (unsigned)(u8)p[1] - '0';
  if (a > 9 || b > 9) return false;
  v = (int)(a * 10 + b);
  return true;
}
static const int MDAYS[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
// F1 / F2 / F3 -> epoch ms; false = bad. An F3 value of 19+ significant
// digits is >= 1e18 ms, outside every window, so it is reported bad.
static inline bool parse_ts(const char* p, u32 n, i64& out) {
  if (n == 0) return false;
  if ((unsigned)(u8)p[0] - '0' <= 9 && (n < 5 || p[4] != '-')) {
    u32 i = 0;
    while (i < n && p[i] == '0') i++;
    if (n - i > 18) return false;
    i64 v = 0;
    for (; i < n; i++) {
      unsigned c = (unsigned)(u8)p[i] - '0';
      if (c > 9) return false;
      v = v * 10 + c;
    }
    out = v;
    return true;
  }
  if (n != 20 && n != 25) return false;
  if (p[4] != '-' || p[7] != '-' || p[10] != 'T' || p[13] != ':' || p[16] != ':') return false;
  int y1, y2, mo, d, h, mi, se;
  if (!d2(p, y1) || !d2(p + 2, y2) || !d2(p + 5, mo) || !d2(p + 8, d) || !d2(p + 11, h) ||
      !d2(p + 14, mi) || !d2(p + 17, se))
    return false;
  int y = y1 * 100 + y2;
  if (mo < 1 || mo > 12 || h > 23 || mi > 59 || se > 59 || d < 1) return false;
  bool leap = (y % 4 == 0) && (y % 100 != 0 || y % 400 == 0);
  int md = (mo == 2 && leap) ? 29 : MDAYS[mo - 1];
  if (d > md) return false;
  i64 off = 0;
  if (n == 20) {
    if (p[19] != 'Z') return false;
  } else {
    char sg = p[19];
    if ((sg != '+' && sg != '-') || p[22] != ':') return false;
    int oh, om;
    if (!d2(p + 20, oh) || !d2(p + 23, om) || oh > 23 || om > 59) return false;
    off = (i64)(oh * 60 + om) * 60000;
    if (sg == '-') off = -off;
  }
  out = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60000LL + se * 1000LL - off;
  return true;
}

// ---------------------------------------------------------------- small maps
struct SMap {  // exact string -> int, open addressing, read-only after build
  struct E { u64 h; const char* p; u32 n; int v; };
  std::vector<E> t;
  u64 mask = 0;
  void init(size_t n) { t.assign(pow2ge(n * 2 + 1), E{0, nullptr, 0, -1}); mask = t.size() - 1; }
  bool insert(const char* p, u32 n, int v) {
    u64 h = hstr(p, n);
    for (u64 i = h & mask;; i = (i + 1) & mask) {
      if (t[i].v < 0) { t[i] = E{h, p, n, v}; return true; }
      if (t[i].h == h && seq(t[i].p, t[i].n, p, n)) return false;
    }
  }
  int find(const char* p, u32 n) const {
    u64 h = hstr(p, n);
    for (u64 i = h & mask;; i = (i + 1) & mask) {
      if (t[i].v < 0) return -1;
      if (t[i].h == h && seq(t[i].p, t[i].n, p, n)) return t[i].v;
    }
  }
};
// Exact fast path for IDs shaped <prefix><1-9><0-9>* (p123, c_45): a dense
// array built from every file entry of that shape. Any other string, and any
// shaped string the file lacks, is still decided exactly.
struct NumMap {
  std::string pre;
  std::vector<int> arr;
  bool on = false;
  static inline bool shape(const char* p, u32 n, const std::string& pre, u64& v) {
    u32 pl = (u32)pre.size();
    if (n <= pl || n - pl > 9 || memcmp(p, pre.data(), pl) != 0) return false;
    if (p[pl] < '1' || p[pl] > '9') return false;
    u64 x = 0;
    for (u32 i = pl; i < n; i++) {
      unsigned c = (unsigned)(u8)p[i] - '0';
      if (c > 9) return false;
      x = x * 10 + c;
    }
    v = x;
    return true;
  }
  void build(const std::vector<std::pair<std::string, int>>& ids) {
    if (ids.empty()) return;
    const std::string& f = ids[0].first;
    size_t i = 0;
    while (i < f.size() && (f[i] < '0' || f[i] > '9')) i++;
    if (i > 8) return;
    pre = f.substr(0, i);
    u64 mx = 0, v;
    for (auto& e : ids) if (shape(e.first.data(), (u32)e.first.size(), pre, v)) mx = std::max(mx, v);
    if (mx >= (1u << 24)) return;
    arr.assign(mx + 1, -1);
    for (auto& e : ids) if (shape(e.first.data(), (u32)e.first.size(), pre, v)) arr[v] = e.second;
    on = true;
  }
  // -2: not shaped (ask the hash map); -1: absent; else the value
  inline int find(const char* p, u32 n) const {
    u64 v;
    if (!on || !shape(p, n, pre, v)) return -2;
    return v < arr.size() ? arr[v] : -1;
  }
};

// ---------------------------------------------------------------- records
// A purchase is an event with price > 0 (a valid purchase costs at least 1).
struct SvEv {  // a valid, non-late event copy, in its user-ID bucket (32 B)
  Id uk; u32 ts, price, seq; u16 cat; u8 ul;
};
struct DEnt {  // dedup entry of every well-formed event copy, in its event-ID bucket (20 B)
  u32 h0, h1, h2;  // 96-bit hash of event_id
  u32 ing;         // ingest_ts in seconds (relative)
  u32 seq;         // copy number: thread << SEQB | local
};
static const int SEQB = 26;  // up to 64M event copies per thread
struct ClRec {  // a valid click, in its user-ID bucket (40 B)
  Id uk, ck; u32 ts; u16 camp; u8 ul, cl;
};
struct LSide { Id k; u32 slot; u8 l; };  // slot = edge*2+side
struct PEv { Id nk; u32 ts, price; u16 cat; u8 nl; };            // 32 B
struct PCl { Id nk, ck; u32 ts; u16 camp; u8 nl, cl; };          // 40 B
struct UserRow { Id nk; int day; u32 sessions, events; i64 rev; int top; u8 nl; };

// ---------------------------------------------------------------- parquet writer
struct TW {  // thrift compact protocol
  std::string b;
  std::vector<int> last{0};
  void varint(u64 v) { while (v >= 0x80) { b.push_back((char)(v | 0x80)); v >>= 7; } b.push_back((char)v); }
  void zz(i64 v) { varint(((u64)v << 1) ^ (u64)(v >> 63)); }
  void fh(int id, int type) {
    int d = id - last.back();
    if (d > 0 && d <= 15) b.push_back((char)((d << 4) | type));
    else { b.push_back((char)type); zz(id); }
    last.back() = id;
  }
  void i32(int id, i64 v) { fh(id, 5); zz(v); }
  void i64f(int id, i64 v) { fh(id, 6); zz(v); }
  void str(int id, const std::string& s) { fh(id, 8); varint(s.size()); b += s; }
  void sbeg(int id) { fh(id, 12); last.push_back(0); }
  void send() { b.push_back(0); last.pop_back(); }
  void lbeg(int id, int et, size_t n) {
    fh(id, 9);
    if (n < 15) b.push_back((char)((n << 4) | et));
    else { b.push_back((char)(0xF0 | et)); varint(n); }
  }
  void ebeg() { last.push_back(0); }  // struct element inside a list
};
enum ColKind { C_STR, C_OSTR, C_DATE, C_I32, C_I64 };
struct ColSpec { std::string name; ColKind kind; };
struct ColBuf { std::string data; std::vector<u8> defs; size_t nvals = 0; };
struct PqWriter {
  FILE* f = nullptr;
  std::vector<ColSpec> cols;
  i64 off = 0, nrows = 0;
  struct ChunkMeta { i64 page_off, size, nvals; };
  std::vector<std::pair<i64, std::vector<ChunkMeta>>> groups;
  void open(const std::string& path, const std::vector<ColSpec>& c) {
    cols = c;
    f = fopen(path.c_str(), "wb");
    if (!f) die(("fopen " + path).c_str());
    setvbuf(f, nullptr, _IOFBF, 1 << 22);
    fwrite("PAR1", 1, 4, f);
    off = 4;
  }
  static int ptype(ColKind k) { return (k == C_STR || k == C_OSTR) ? 6 : (k == C_I64 ? 2 : 1); }
  void row_group(std::vector<ColBuf>& bufs, i64 rows) {
    if (rows == 0) return;
    std::vector<ChunkMeta> metas;
    for (size_t c = 0; c < cols.size(); c++) {
      ColBuf& cb = bufs[c];
      std::string levels;
      if (cols[c].kind == C_OSTR) {  // RLE/bit-packed hybrid: one bit-packed run, bit width 1
        size_t ng = (rows + 7) / 8;
        TW t; t.varint((ng << 1) | 1);
        std::string run = t.b;
        run.append(ng, '\0');
        for (i64 i = 0; i < rows; i++)
          if (cb.defs[i]) run[t.b.size() + i / 8] |= (char)(1 << (i % 8));
        u32 L = (u32)run.size();
        levels.append((const char*)&L, 4);
        levels += run;
      }
      i64 psize = (i64)(levels.size() + cb.data.size());
      TW h;
      h.i32(1, 0);  // DATA_PAGE
      h.i32(2, psize);
      h.i32(3, psize);
      h.sbeg(5);    // DataPageHeader
      h.i32(1, rows);
      h.i32(2, 0);  // PLAIN
      h.i32(3, 3);  // RLE
      h.i32(4, 3);
      h.send();
      h.b.push_back(0);
      ChunkMeta m{off, (i64)h.b.size() + psize, rows};
      fwrite(h.b.data(), 1, h.b.size(), f);
      fwrite(levels.data(), 1, levels.size(), f);
      fwrite(cb.data.data(), 1, cb.data.size(), f);
      off += m.size;
      metas.push_back(m);
    }
    groups.push_back({rows, metas});
    nrows += rows;
  }
  void close() {
    TW t;
    t.i32(1, 1);
    t.lbeg(2, 12, cols.size() + 1);
    t.ebeg(); t.str(4, "schema"); t.i32(5, (i64)cols.size()); t.send();
    for (auto& c : cols) {
      t.ebeg();
      t.i32(1, ptype(c.kind));
      t.i32(3, c.kind == C_OSTR ? 1 : 0);
      t.str(4, c.name);
      if (c.kind == C_STR || c.kind == C_OSTR) {
        t.i32(6, 0);                                 // UTF8
        t.sbeg(10); t.sbeg(1); t.send(); t.send();   // LogicalType STRING
      } else if (c.kind == C_DATE) {
        t.i32(6, 6);                                 // DATE
        t.sbeg(10); t.sbeg(6); t.send(); t.send();   // LogicalType DATE
      }
      t.send();
    }
    t.i64f(3, nrows);
    t.lbeg(4, 12, groups.size());
    for (auto& g : groups) {
      t.ebeg();
      t.lbeg(1, 12, cols.size());
      i64 total = 0;
      for (size_t c = 0; c < cols.size(); c++) {
        auto& m = g.second[c];
        total += m.size;
        t.ebeg();
        t.i64f(2, m.page_off);
        t.sbeg(3);
        t.i32(1, ptype(cols[c].kind));
        t.lbeg(2, 5, 2); t.zz(0); t.zz(3);
        t.lbeg(3, 8, 1); t.varint(cols[c].name.size()); t.b += cols[c].name;
        t.i32(4, 0);
        t.i64f(5, m.nvals);
        t.i64f(6, m.size);
        t.i64f(7, m.size);
        t.i64f(9, m.page_off);
        t.send();
        t.send();
      }
      t.i64f(2, total);
      t.i64f(3, g.first);
      t.send();
    }
    t.str(6, "shopping-sessions-cpp");
    t.b.push_back(0);
    fwrite(t.b.data(), 1, t.b.size(), f);
    u32 L = (u32)t.b.size();
    fwrite(&L, 1, 4, f);
    fwrite("PAR1", 1, 4, f);
    if (fclose(f) != 0) die("fclose");
  }
};
static inline void put_str(ColBuf& c, const char* p, u32 n) {
  c.data.append((const char*)&n, 4); c.data.append(p, n); c.nvals++;
}
static inline void put_i32(ColBuf& c, int32_t v) { c.data.append((const char*)&v, 4); c.nvals++; }
static inline void put_i64(ColBuf& c, i64 v) { c.data.append((const char*)&v, 8); c.nvals++; }

// ---------------------------------------------------------------- globals
static i64 WS, WE, CWS;  // event window start/end, click window start
static int DAY0, NDAYS;
static SMap products, campaigns;
static NumMap prodNum, campNum;
static std::vector<u16> prodCat;  // product idx -> category idx
static std::vector<std::string> catNames;
static std::vector<int> catRank;  // byte-wise order of category names
static int NCAT, UNKNOWN_CAT;
static std::vector<std::string> campIds, campChan;
static int NCAMP;

static std::string read_file(const std::string& path) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) die(("open " + path).c_str());
  std::string s;
  char buf[1 << 16];
  for (;;) {
    ssize_t r = read(fd, buf, sizeof buf);
    if (r < 0) die("read");
    if (r == 0) break;
    s.append(buf, (size_t)r);
  }
  close(fd);
  return s;
}
static std::string json_str(const std::string& j, const char* key) {
  size_t k = j.find(std::string("\"") + key + "\"");
  if (k == std::string::npos) die("manifest key");
  size_t c = j.find(':', k);
  size_t q1 = j.find('"', c + 1), q2 = j.find('"', q1 + 1);
  return j.substr(q1 + 1, q2 - q1 - 1);
}
template <class F>
static void each_line(const char* d, size_t n, F fn) {  // data lines, header skipped
  const char* e = d + n;
  const char* p = (const char*)memchr(d, '\n', n);
  if (!p) return;
  p++;
  while (p < e) {
    const char* nl = (const char*)memchr(p, '\n', e - p);
    const char* le = nl ? nl : e;
    fn(p, le);
    p = le + 1;
  }
}
static std::vector<std::string> list_csv(const std::string& dir) {
  std::vector<std::string> out;
  DIR* d = opendir(dir.c_str());
  if (!d) return out;
  while (dirent* e = readdir(d)) {
    std::string n = e->d_name;
    if (n.size() >= 4 && n.compare(n.size() - 4, 4, ".csv") == 0) out.push_back(dir + "/" + n);
  }
  closedir(d);
  std::sort(out.begin(), out.end());
  return out;
}

// ---------------------------------------------------------------- chunk reader
// Streams the lines that START inside file bytes [b, e) through a reusable
// buffer. The line starting at offset 0 is the header and is skipped.
struct Chunk { int kind, fd; size_t size, b, e; };
static size_t RBUF = 1 << 20, MINLEFT = 1 << 16;  // test hooks: SS_RBUF
static const size_t PAD = 256;
struct Reader {
  std::vector<char> buf;
  Reader() : buf(RBUF + PAD) {}
  template <class F>  // F(const char* line_start, const char* buf_end, bool at_eof) -> next line start
  void run(const Chunk& c, F fn) {
    size_t off = c.b ? c.b - 1 : 0;  // file offset of buf[0]
    size_t len = 0;                  // valid bytes in buf
    auto fill = [&](size_t keep_from) {  // keep buf[keep_from..len), read more after it
      size_t keep = len - keep_from;
      if (keep_from) memmove(buf.data(), buf.data() + keep_from, keep);
      off += keep_from;
      len = keep;
      if (buf.size() < len + MINLEFT + PAD) buf.resize(2 * buf.size());
      size_t room = buf.size() - PAD - len;
      while (room > 0 && off + len < c.size) {
        ssize_t r = pread(c.fd, buf.data() + len, std::min(room, c.size - off - len), (off_t)(off + len));
        if (r < 0) { if (errno == EINTR) continue; die("pread"); }
        if (r == 0) break;
        len += (size_t)r; room -= (size_t)r;
      }
      memset(buf.data() + len, 0, PAD);
    };
    fill(0);
    size_t p;  // index of the current line start
    if (c.b == 0 || buf[0] != '\n') {
      // skip the header (b == 0) or the tail of a line that started before b
      size_t i = c.b ? 1 : 0;
      for (;;) {
        const char* nl = (const char*)memchr(buf.data() + i, '\n', len - i);
        if (nl) { p = (size_t)(nl - buf.data()) + 1; break; }
        if (off + len >= c.size) return;
        i = len;
        fill(0);
      }
    } else {
      p = 1;
    }
    for (;;) {
      if (off + p >= c.e || off + p >= c.size) return;
      bool eof = off + len >= c.size;
      if (!eof && len - p < MINLEFT) { fill(p); p = 0; eof = off + len >= c.size; }
      const char* np = fn(buf.data() + p, buf.data() + len, eof);
      if (!np) {  // the line runs past the buffer: grow and read more, then retry it
        size_t old = len - p;  // bytes of this line already buffered
        fill(p); p = 0;
        if (len == old && off + len < c.size) die("reader stuck");
        continue;
      }
      p = (size_t)(np - buf.data());
    }
  }
};
// Split the line at p (the buffer ends at fe). 1 ok, 0 malformed (skip it),
// -2 the line's end is not buffered yet and this is not the end of the file.
static inline int split_line(const char* p, const char* fe, bool eof, Fld* f, int nf, const char** next) {
#if defined(__AVX2__)
  if (fe - p >= 128) {
    const char* le;
    int r = split_simd(p, f, nf, &le);
    if (r >= 0) { *next = le + 1; return r; }
  }
#endif
  const char* nl = (const char*)memchr(p, '\n', fe - p);
  if (!nl && !eof) return -2;
  const char* le = nl ? nl : fe;
  *next = le + 1;
  return split(p, le, f, nf) ? 1 : 0;
}


int main(int argc, char** argv) {
  const char* ev = getenv("DATA_DIR");
  std::string DATA = argc >= 3 ? argv[argc - 2] : (ev ? ev : "/data");
  ev = getenv("OUTPUT_DIR");
  std::string OUT = argc >= 3 ? argv[argc - 1] : (ev ? ev : "/output");
  ev = getenv("NCPU");
  NT = ev ? atoi(ev) : (int)std::thread::hardware_concurrency();
  if (NT < 1) NT = 1;
  if (NT > 64) NT = 64;

  // ---- manifest, products, campaigns
  {
    std::string man = read_file(DATA + "/manifest.json");
    std::string a = json_str(man, "window_start"), b = json_str(man, "window_end");
    if (!parse_ts(a.data(), (u32)a.size(), WS) || !parse_ts(b.data(), (u32)b.size(), WE)) die("manifest ts");
  }
  CWS = WS - WEEK_MS;
  if (WE - CWS > 4000000000LL || WE <= WS) die("window not supported");
  auto floordiv = [](i64 a, i64 b) { return a >= 0 ? a / b : -((-a + b - 1) / b); };
  DAY0 = (int)floordiv(WS, DAY_MS);
  NDAYS = (int)floordiv(WE - 1, DAY_MS) - DAY0 + 1;

  static std::string prodText = read_file(DATA + "/products.csv");
  static std::string campText = read_file(DATA + "/campaigns.csv");
  {
    SMap cats; cats.init(1024);
    size_t cnt = 0;
    each_line(prodText.data(), prodText.size(), [&](const char*, const char*) { cnt++; });
    products.init(cnt);
    std::vector<std::pair<const char*, u32>> catv;
    std::vector<std::pair<std::string, int>> prodIds;
    each_line(prodText.data(), prodText.size(), [&](const char* s, const char* e) {
      Fld f[2];
      if (!split(s, e, f, 2)) return;
      int c = cats.find(f[1].p, f[1].n);
      if (c < 0) {
        if (catv.size() >= 60000) die("too many categories");
        c = (int)catv.size(); cats.insert(f[1].p, f[1].n, c); catv.push_back({f[1].p, f[1].n});
      }
      if (products.insert(f[0].p, f[0].n, (int)prodCat.size())) {
        prodIds.push_back({std::string(f[0].p, f[0].n), (int)prodCat.size()});
        prodCat.push_back((u16)c);
      }
    });
    for (auto& c : catv) catNames.push_back(std::string(c.first, c.second));
    prodNum.build(prodIds);
    UNKNOWN_CAT = (int)catNames.size();
    catNames.push_back("unknown");
    NCAT = (int)catNames.size();
    std::vector<int> ord(NCAT);
    for (int i = 0; i < NCAT; i++) ord[i] = i;
    std::sort(ord.begin(), ord.end(), [&](int a, int b) { return catNames[a] < catNames[b]; });
    catRank.assign(NCAT, 0);
    for (int i = 0; i < NCAT; i++) catRank[ord[i]] = i;

    std::vector<std::pair<std::string, int>> campPairs;
    size_t cc = 0;
    each_line(campText.data(), campText.size(), [&](const char*, const char*) { cc++; });
    campaigns.init(cc);
    each_line(campText.data(), campText.size(), [&](const char* s, const char* e) {
      Fld f[2];
      if (!split(s, e, f, 2)) return;
      if (campaigns.insert(f[0].p, f[0].n, (int)campIds.size())) {
        campPairs.push_back({std::string(f[0].p, f[0].n), (int)campIds.size()});
        campIds.push_back(std::string(f[0].p, f[0].n));
        campChan.push_back(std::string(f[1].p, f[1].n));
      }
    });
    NCAMP = (int)campIds.size();
    if (NCAMP >= 65535) die("too many campaigns");
    campNum.build(campPairs);
  }
  lap("setup");

  // ---- open inputs, make chunks
  std::vector<Chunk> linkChunks, dataChunks;
  size_t CH = 8u << 20;
  if (const char* x = getenv("SS_CHUNK")) CH = std::max<size_t>(1, strtoull(x, nullptr, 10));  // test hook
  if (const char* x = getenv("SS_RBUF")) { RBUF = std::max<size_t>(64, strtoull(x, nullptr, 10)); MINLEFT = RBUF / 4; }
  const char* kinds[3] = {"events", "clicks", "links"};
  for (int k = 0; k < 3; k++) {
    for (auto& path : list_csv(DATA + "/" + kinds[k])) {
      int fd = open(path.c_str(), O_RDONLY);
      if (fd < 0) die(("open " + path).c_str());
      struct stat st;
      if (fstat(fd, &st) != 0) die("fstat");
      size_t n = (size_t)st.st_size;
      posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
      for (size_t b = 0; b < n; b += CH) (k == 2 ? linkChunks : dataChunks).push_back(Chunk{k, fd, n, b, std::min(n, b + CH)});
    }
  }
  std::vector<Arena> arE(NT), arC(NT), arL(NT), arS(NT), arP(NT), arStr(NT);
  std::vector<Reader> readers(NT);
  // An ID from a field (long ones are copied to the thread's string arena).
  auto make_id = [](const Fld& f, Arena& strs, u8& L) -> Id {
    Id r{0, 0};
    if (f.n <= 16) {
      // the reader leaves >= 256 readable bytes after the buffer: load 16, mask to n
      static const u8 M[32] = {255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255};
      __m128i v = _mm_loadu_si128((const __m128i*)f.p);
      __m128i m = _mm_loadu_si128((const __m128i*)(M + 16 - f.n));
      _mm_storeu_si128((__m128i*)&r, _mm_and_si128(v, m));
      L = (u8)f.n;
    } else {
      LongId* q = (LongId*)strs.alloc(sizeof(LongId) + f.n, 8);
      q->n = f.n;
      memcpy(q->s, f.p, f.n);
      r.a = (u64)q;
      r.b = hbytes(f.p, f.n);
      L = LONG;
    }
    return r;
  };
  lap("open");

  // ---- phase L: links
  auto lkb = make_buckets<LSide>();
  std::vector<u32> nedges(NT, 0);
  parallel_for(linkChunks.size(), [&](size_t ci, int tid) {
    auto& LB = lkb[tid];
    Arena& AL = arL[tid];
    Arena& AT = arStr[tid];
    readers[tid].run(linkChunks[ci], [&](const char* p, const char* fe, bool eof) -> const char* {
      Fld f[2];
      const char* next;
      int r = split_line(p, fe, eof, f, 2, &next);
      if (r < 0) return nullptr;
      if (!r || f[0].n == 0 || f[1].n == 0) return next;
      u32 e = nedges[tid]++;
      for (int sd = 0; sd < 2; sd++) {
        u8 L;
        Id id = make_id(f[sd], AT, L);
        LB[id_hash(id, L) >> BSH].push(LSide{id, e * 2 + sd, L}, AL);
      }
      return next;
    });
  });
  lap("links");

  struct LNode { Id k; u64 h; u8 l; };
  std::vector<std::vector<LNode>> lnodes(NB);
  std::vector<std::vector<u32>> ltab(NB);  // slot -> local+1
  std::vector<u64> lmask(NB);
  std::vector<std::vector<u32>> edgeEnd(NT);
  for (int t = 0; t < NT; t++) edgeEnd[t].assign((size_t)nedges[t] * 2, 0);
  parallel_for(NB, [&](size_t b, int) {
    size_t tot = 0;
    for (int t = 0; t < NT; t++) tot += lkb[t][b].cnt;
    auto& tab = ltab[b];
    tab.assign(pow2ge(tot * 2 + 1), 0);
    u64 mask = tab.size() - 1;
    lmask[b] = mask;
    auto& nodes = lnodes[b];
    nodes.reserve(tot);
    for (int t = 0; t < NT; t++) {
      auto& EE = edgeEnd[t];
      lkb[t][b].each([&](LSide& s) {
        u64 h = id_hash(s.k, s.l);
        u32 local = 0;
        for (u64 i = h & mask;; i = (i + 1) & mask) {
          u32 v = tab[i];
          if (v == 0) { nodes.push_back(LNode{s.k, h, s.l}); tab[i] = (u32)nodes.size(); local = (u32)nodes.size() - 1; break; }
          LNode& x = nodes[v - 1];
          if (x.h == h && id_eq(x.k, x.l, s.k, s.l)) { local = v - 1; break; }
        }
        if (local >= (1u << LBITS)) die("link bucket too large");
        EE[s.slot] = ((u32)b << LBITS) | local;
      });
    }
  });
  for (auto& a : arL) a.release();
  lap("intern");

  // union-find on its own thread while events parse. Root = smallest ID.
  struct PName { Id k; u8 l; };
  std::vector<std::vector<PName>> pnames(NB);
  std::vector<u32> base(NB + 1, 0);
  std::thread ufThread([&]() {
    for (int b = 0; b < NB; b++) base[b + 1] = base[b] + (u32)lnodes[b].size();
    u32 NN = base[NB];
    std::vector<u32> par(NN);
    std::vector<const LNode*> node(NN);
    for (int b = 0; b < NB; b++)
      for (u32 i = 0; i < lnodes[b].size(); i++) { par[base[b] + i] = base[b] + i; node[base[b] + i] = &lnodes[b][i]; }
    auto gid = [&](u32 packed) { return base[packed >> LBITS] + (packed & ((1u << LBITS) - 1)); };
    auto find = [&](u32 x) {
      while (par[x] != x) { par[x] = par[par[x]]; x = par[x]; }
      return x;
    };
    for (int t = 0; t < NT; t++) {
      auto& E = edgeEnd[t];
      for (size_t i = 0; i < E.size(); i += 2) {
        u32 a = find(gid(E[i])), b = find(gid(E[i + 1]));
        if (a == b) continue;
        if (id_less(node[b]->k, node[b]->l, node[a]->k, node[a]->l)) std::swap(a, b);
        par[b] = a;
      }
      std::vector<u32>().swap(E);
    }
    for (int b = 0; b < NB; b++) {
      pnames[b].resize(lnodes[b].size());
      for (u32 i = 0; i < lnodes[b].size(); i++) {
        const LNode* r = node[find(base[b] + i)];
        pnames[b][i] = PName{r->k, r->l};
      }
    }
  });

  // ---- phase 1: events and clicks
  auto evb = make_buckets<DEnt>();
  auto svb = make_buckets<SvEv>();
  auto clb = make_buckets<ClRec>();
  const i64 ING0 = WS / 1000 - ((i64)1 << 31);  // ingest seconds origin (u32 range around the window)
  std::vector<u32> nseq(NT, 0);
  std::stable_sort(dataChunks.begin(), dataChunks.end(), [](const Chunk& a, const Chunk& b) { return a.e - a.b > b.e - b.b; });
  parallel_for(dataChunks.size(), [&](size_t ci, int tid) {
    const Chunk& c = dataChunks[ci];
    auto& EB = evb[tid];
    auto& SB = svb[tid];
    auto& CB = clb[tid];
    Arena& AE = arE[tid];
    Arena& AS = arS[tid];
    Arena& AC = arC[tid];
    Arena& AT = arStr[tid];
    if (c.kind == 0) {
      readers[tid].run(c, [&](const char* p, const char* fe, bool eof) -> const char* {
        Fld f[7];
        const char* next;
        int sr = split_line(p, fe, eof, f, 7, &next);
        if (sr < 0) return nullptr;
        if (!sr || f[0].n == 0) return next;
        if (nseq[tid] >= (1u << SEQB)) die("too many events for one thread");
        DEnt de;
        {
          u8 el;
          Id ek = make_id(f[0], AT, el);
          u64 h = id_hash(ek, el);
          u32 h2 = el == LONG ? (u32)hbytes(f[0].p, f[0].n, K2) : (u32)mum(ek.a ^ K3, ek.b ^ K2 ^ el);
          de.h0 = (u32)h; de.h1 = (u32)(h >> 32); de.h2 = h2;
        }
        de.seq = ((u32)tid << SEQB) | nseq[tid]++;
        i64 ing;
        // ingest_ts is always valid F1 (whole seconds); anything else sorts last
        if (!parse_ts(f[3].p, f[3].n, ing)) ing = INT64_MAX;
        i64 is = ing == INT64_MAX ? -1 : ing / 1000 - ING0;
        de.ing = (is < 0 || is > (i64)UINT32_MAX) ? UINT32_MAX : (u32)is;
        SvEv r;
        r.price = 0; r.seq = de.seq;
        i64 ts;
        bool ok = f[1].n > 0 && f[5].n > 0 && parse_ts(f[2].p, f[2].n, ts) && ts >= WS && ts < WE;
        if (ok) {
          const char* t = f[4].p; u32 tn = f[4].n;
          if (tn == 4 && (memcmp(t, "view", 4) == 0 || memcmp(t, "cart", 4) == 0)) {
          } else if (tn == 8 && memcmp(t, "purchase", 8) == 0) {
            const char* q = f[6].p; u32 qn = f[6].n;
            if (qn == 0) ok = false;
            for (u32 i = 0; ok && i < qn; i++) if ((unsigned)(u8)q[i] - '0' > 9) ok = false;
            if (ok) {
              u32 i = 0;
              while (i < qn && q[i] == '0') i++;
              if (qn - i > 8) ok = false;
              else {
                u32 v = 0;
                for (; i < qn; i++) v = v * 10 + (u32)(q[i] - '0');
                if (v < 1 || v > 10000000) ok = false;
                r.price = v;
              }
            }
          } else ok = false;
        }
        if (ok && ing - ts > DAY_MS) ok = false;  // late: strictly over 24 h
        if (ok) {
          r.uk = make_id(f[1], AT, r.ul);
          r.ts = (u32)(ts - WS);
          int pi = prodNum.find(f[5].p, f[5].n);
          if (pi == -2) pi = products.find(f[5].p, f[5].n);
          r.cat = (u16)(pi < 0 ? UNKNOWN_CAT : prodCat[pi]);
          SB[id_hash(r.uk, r.ul) >> BSH].push(r, AS);
        }
        EB[de.h1 >> (BSH - 32)].push(de, AE);
        return next;
      });
    } else {
      readers[tid].run(c, [&](const char* p, const char* fe, bool eof) -> const char* {
        Fld f[4];
        const char* next;
        int sr = split_line(p, fe, eof, f, 4, &next);
        if (sr < 0) return nullptr;
        if (!sr || f[0].n == 0 || f[1].n == 0) return next;
        int camp = campNum.find(f[2].p, f[2].n);
        if (camp == -2) camp = campaigns.find(f[2].p, f[2].n);
        if (camp < 0) return next;
        i64 ts;
        if (!parse_ts(f[3].p, f[3].n, ts) || ts < CWS || ts >= WE) return next;
        ClRec r;
        r.uk = make_id(f[1], AT, r.ul);
        r.ck = make_id(f[0], AT, r.cl);
        r.ts = (u32)(ts - CWS); r.camp = (u16)camp;
        CB[id_hash(r.uk, r.ul) >> BSH].push(r, AC);
        return next;
      });
    }
  });
  lap("parse");

  // ---- phase 2: dedup. The earliest ingest_ts wins; every other copy is
  // marked dead in a per-thread bitmap of copy numbers (dedup comes before
  // validation, so an invalid winner kills valid later copies too).
  // Equal ingest_ts copies are identical, so either may win. Event IDs are
  // compared by a 96-bit hash (collision odds ~1e-14 at 60M copies).
  std::vector<std::vector<u64>> dead(NT);
  for (int t = 0; t < NT; t++) dead[t].assign(nseq[t] / 64 + 1, 0);
  auto kill = [&](u32 seq) {
    __atomic_fetch_or(&dead[seq >> SEQB][(seq & ((1u << SEQB) - 1)) >> 6], (u64)1 << (seq & 63), __ATOMIC_RELAXED);
  };
  auto isDead = [&](u32 seq) -> bool {
    return (dead[seq >> SEQB][(seq & ((1u << SEQB) - 1)) >> 6] >> (seq & 63)) & 1;
  };
  parallel_for(NB, [&](size_t b, int) {
    struct DT { u64 h; DEnt* d; };
    static thread_local std::vector<DT> tab;
    size_t tot = 0;
    for (int t = 0; t < NT; t++) tot += evb[t][b].cnt;
    size_t cap = pow2ge(tot + tot / 2 + 1);
    tab.assign(cap, DT{0, nullptr});
    u64 mask = cap - 1;
    for (int t = 0; t < NT; t++) {
      evb[t][b].each([&](DEnt& r) {
        u64 h = ((u64)r.h1 << 32) | r.h0;
        for (u64 i = h & mask;; i = (i + 1) & mask) {
          DT& x = tab[i];
          if (!x.d) { x.h = h; x.d = &r; return; }
          if (x.h == h && x.d->h2 == r.h2) {
            if (r.ing < x.d->ing) { kill(x.d->seq); x.d = &r; }
            else kill(r.seq);
            return;
          }
        }
      });
    }
  });
  for (auto& a : arE) a.release();
  lap("dedup");
  ufThread.join();
  lap("unionfind");

  // ---- phase 3: the person of every event and click
  auto peb = make_buckets<PEv>();
  auto pcb = make_buckets<PCl>();
  parallel_for(NB, [&](size_t b, int tid) {
    auto& tab = ltab[b];
    auto& nodes = lnodes[b];
    auto& pn = pnames[b];
    u64 mask = lmask[b];
    // person name of an ID and its hash
    auto person = [&](const Id& k, u8 l, Id& nk, u8& nl) -> u64 {
      u64 h = id_hash(k, l);
      for (u64 i = h & mask;; i = (i + 1) & mask) {
        u32 v = tab[i];
        if (v == 0) { nk = k; nl = l; return h; }
        LNode& x = nodes[v - 1];
        if (x.h == h && id_eq(x.k, x.l, k, l)) {
          const PName& r = pn[v - 1];
          nk = r.k; nl = r.l;
          return id_hash(nk, nl);
        }
      }
    };
    auto& PE = peb[tid];
    auto& PC = pcb[tid];
    Arena& AP = arP[tid];
    for (int t = 0; t < NT; t++) {
      svb[t][b].each([&](SvEv& e) {
        if (isDead(e.seq)) return;
        PEv o;
        u64 ph = person(e.uk, e.ul, o.nk, o.nl);
        o.ts = e.ts; o.price = e.price; o.cat = e.cat;
        PE[ph >> BSH].push(o, AP);
      });
      clb[t][b].each([&](ClRec& c) {
        PCl o;
        u64 ph = person(c.uk, c.ul, o.nk, o.nl);
        o.ck = c.ck; o.cl = c.cl; o.ts = c.ts; o.camp = c.camp;
        PC[ph >> BSH].push(o, AP);
      });
    }
  });
  for (auto& a : arS) a.release();
  for (auto& a : arC) a.release();
  lap("person");

  // ---- phase 4: sessions per person bucket
  struct CD { i64 sessions, purch, rev, assisted; };
  int NCD = (NCAMP + 1) * NDAYS;  // campaign idx NCAMP = organic
  std::vector<std::vector<CD>> cdT(NT, std::vector<CD>(NCD, CD{0, 0, 0, 0}));
  std::vector<std::vector<UserRow>> urT(NT);
  parallel_for(NB, [&](size_t b, int tid) {
    struct PT { Id k; u32 idx; u8 l; };
    struct E { u32 ts, price; u16 cat; };
    struct C { Id ck; u32 ts; u16 camp; u8 cl; };
    struct S { u32 start; int day; i64 rev; u8 purch; };
    static thread_local std::vector<PT> tab;
    struct PK { Id k; u8 l; };
    static thread_local std::vector<PK> pkeys;
    static thread_local std::vector<u32> ecount, ccount, eidx, cidx, eoff, coff, pos;
    static thread_local std::vector<E> ev;
    static thread_local std::vector<C> cl;
    static thread_local std::vector<S> ss;
    static thread_local std::vector<int> cnt, act;
    static thread_local std::vector<u8> touchedF;
    static thread_local std::vector<u16> touched, cattouch;
    static thread_local std::vector<i64> catsum;
    static thread_local std::vector<u32> fd;
    if (cnt.size() != (size_t)NCAMP || catsum.size() != (size_t)NCAT) {  // first task on this thread
      cnt.assign(NCAMP, 0); act.assign(NCAMP, -1); touchedF.assign(NCAMP, 0);
      catsum.assign(NCAT, 0); fd.assign(NDAYS + 1, 0);
    }
    size_t tot = 0, totc = 0;
    for (int t = 0; t < NT; t++) { tot += peb[t][b].cnt; totc += pcb[t][b].cnt; }
    if (tot == 0) return;
    // person table: sized for persons (not events) and grown at load 1/2,
    // so it stays in L2
    tab.assign(pow2ge(tot / 16 + 64), PT{Id{0, 0}, UINT32_MAX, 0});
    u64 mask = tab.size() - 1;
    ecount.clear(); pkeys.clear();
    eidx.resize(tot); cidx.resize(totc);
    auto slot = [&](u64 h) { return (h * 0x9E3779B97F4A7C15ULL) >> 20; };
    auto grow = [&]() {
      tab.assign(tab.size() * 2, PT{Id{0, 0}, UINT32_MAX, 0});
      mask = tab.size() - 1;
      for (u32 id = 0; id < pkeys.size(); id++) {
        u64 i = slot(id_hash(pkeys[id].k, pkeys[id].l)) & mask;
        while (tab[i].idx != UINT32_MAX) i = (i + 1) & mask;
        tab[i] = PT{pkeys[id].k, id, pkeys[id].l};
      }
    };
    auto lookup = [&](const Id& kk, u8 l, bool ins) -> u32 {
      u64 h = id_hash(kk, l);
      for (u64 i = slot(h) & mask;; i = (i + 1) & mask) {
        PT& x = tab[i];
        if (x.idx == UINT32_MAX) {
          if (!ins) return UINT32_MAX;
          u32 id = (u32)ecount.size();
          x.k = kk; x.l = l; x.idx = id;
          ecount.push_back(0); pkeys.push_back(PK{kk, l});
          if (pkeys.size() * 2 > tab.size()) grow();
          return id;
        }
        if (id_eq(x.k, x.l, kk, l)) return x.idx;
      }
    };
    size_t k = 0;
    for (int t = 0; t < NT; t++)
      peb[t][b].each([&](PEv& e) { u32 id = lookup(e.nk, e.nl, true); ecount[id]++; eidx[k++] = id; });
    u32 NP = (u32)ecount.size();
    ccount.assign(NP, 0);
    k = 0;
    for (int t = 0; t < NT; t++)
      pcb[t][b].each([&](PCl& c) { u32 id = lookup(c.nk, c.nl, false); cidx[k++] = id; if (id != UINT32_MAX) ccount[id]++; });
    eoff.assign(NP + 1, 0); coff.assign(NP + 1, 0);
    for (u32 i = 0; i < NP; i++) { eoff[i + 1] = eoff[i] + ecount[i]; coff[i + 1] = coff[i] + ccount[i]; }
    ev.resize(tot);
    cl.resize(coff[NP]);
    pos.assign(eoff.begin(), eoff.end() - 1);
    k = 0;
    for (int t = 0; t < NT; t++)
      peb[t][b].each([&](PEv& e) { ev[pos[eidx[k++]]++] = E{e.ts, e.price, e.cat}; });
    pos.assign(coff.begin(), coff.end() - 1);
    k = 0;
    for (int t = 0; t < NT; t++)
      pcb[t][b].each([&](PCl& c) { u32 id = cidx[k++]; if (id != UINT32_MAX) cl[pos[id]++] = C{c.ck, c.ts, c.camp, c.cl}; });

    auto& CDt = cdT[tid];
    auto& UR = urT[tid];
    for (u32 pi = 0; pi < NP; pi++) {
      const PK& who = pkeys[pi];
      E* e0 = &ev[eoff[pi]];
      u32 ne = eoff[pi + 1] - eoff[pi];
      std::sort(e0, e0 + ne, [](const E& a, const E& c) { return a.ts < c.ts; });
      C* c0 = &cl[coff[pi]];
      u32 nc = coff[pi + 1] - coff[pi];
      if (nc > 1)
        std::sort(c0, c0 + nc, [](const C& a, const C& c) {
          if (a.ts != c.ts) return a.ts < c.ts;
          return id_less(a.ck, a.cl, c.ck, c.cl);
        });

      // sessionize + user_daily
      ss.clear();
      int curDay = INT32_MIN;
      u32 dSess = 0, dEv = 0;
      i64 dRev = 0;
      auto flush = [&]() {
        if (curDay == INT32_MIN) return;
        int top = -1;
        i64 best = 0;
        for (u16 c : cattouch) {
          i64 v = catsum[c];
          if (top < 0 || v > best || (v == best && catRank[c] < catRank[top])) { top = c; best = v; }
          catsum[c] = 0;
        }
        cattouch.clear();
        UR.push_back(UserRow{who.k, curDay, dSess, dEv, dRev, top, who.l});
      };
      u32 prev = 0;
      for (u32 i = 0; i < ne; i++) {
        const E& x = e0[i];
        if (i == 0 || x.ts - prev > (u32)GAP_MS) {  // equal ts never splits
          int day = (int)((WS + x.ts) / DAY_MS);
          ss.push_back(S{x.ts, day, 0, 0});
          if (day != curDay) { flush(); curDay = day; dSess = 0; dEv = 0; dRev = 0; }
          dSess++;
        }
        prev = x.ts;
        dEv++;
        if (x.price) {
          S& s = ss.back();
          s.rev += x.price; s.purch = 1;
          dRev += x.price;
          if (catsum[x.cat] == 0) cattouch.push_back(x.cat);
          catsum[x.cat] += x.price;
        }
      }
      flush();

      // credit and assists
      u32 m = (u32)ss.size();
      if (nc == 0) {
        for (auto& s : ss) {
          CD& d = CDt[NCAMP * NDAYS + (s.day - DAY0)];
          d.sessions++; d.purch += s.purch; d.rev += s.rev;
        }
        continue;
      }
      {
        u32 j = 0;
        for (int d = 0; d <= NDAYS; d++) {  // first session of each day
          while (j < m && ss[j].day - DAY0 < d) j++;
          fd[d] = j;
        }
      }
      auto addRange = [&](int camp, u32 i, u32 j) {  // campaign assisted sessions [i, j)
        if (i >= j) return;
        int da = ss[i].day - DAY0, db = ss[j - 1].day - DAY0;
        for (int d = da; d <= db; d++) {
          u32 lo = std::max(i, fd[d]), hi = std::min(j, fd[d + 1]);
          if (hi > lo) CDt[camp * NDAYS + d].assisted += hi - lo;
        }
      };
      // sliding window of the clicks with start - 7d <= ts <= start
      u32 lo = 0, hi = 0;
      for (u32 si = 0; si < m; si++) {
        const S& s = ss[si];
        u32 sc = s.start + (u32)WEEK_MS;  // session start in the click frame
        while (hi < nc && c0[hi].ts <= sc) {
          u16 cp = c0[hi].camp;
          cnt[cp]++;
          if (!touchedF[cp]) { touchedF[cp] = 1; touched.push_back(cp); }
          hi++;
        }
        while (lo < hi && c0[lo].ts < s.start) {  // ts < start - 7d
          u16 cp = c0[lo].camp;
          cnt[cp]--;
          if (!touchedF[cp]) { touchedF[cp] = 1; touched.push_back(cp); }
          lo++;
        }
        for (u16 cp : touched) {
          touchedF[cp] = 0;
          if (cnt[cp] > 0 && act[cp] < 0) act[cp] = (int)si;
          else if (cnt[cp] == 0 && act[cp] >= 0) { addRange(cp, (u32)act[cp], si); act[cp] = -1; }
        }
        touched.clear();
        int camp = NCAMP;
        if (hi > lo) {  // latest ts; ties: smallest click_id (sorted first)
          u32 j = hi - 1;
          u32 t = c0[j].ts;
          while (j > lo && c0[j - 1].ts == t) j--;
          camp = c0[j].camp;
        }
        CD& d = CDt[camp * NDAYS + (s.day - DAY0)];
        d.sessions++; d.purch += s.purch; d.rev += s.rev;
      }
      for (u32 j = lo; j < hi; j++) {
        u16 cp = c0[j].camp;
        if (act[cp] >= 0) { addRange(cp, (u32)act[cp], m); act[cp] = -1; }
        cnt[cp] = 0;
      }
    }
  });
  lap("sessions");

  // ---- phase 5: write outputs
  std::string ud = OUT + "/user_daily", cdd = OUT + "/campaign_daily";
  mkdir(OUT.c_str(), 0777);
  mkdir(ud.c_str(), 0777);
  mkdir(cdd.c_str(), 0777);
  std::vector<ColSpec> ucols = {{"user_id", C_STR}, {"day", C_DATE}, {"sessions", C_I32},
                                {"events", C_I32}, {"revenue_cents", C_I64}, {"top_category", C_OSTR}};
  size_t totalRows = 0;
  for (auto& v : urT) totalRows += v.size();
  std::thread campThread([&]() {
    std::vector<ColSpec> ccols = {{"campaign_id", C_STR}, {"channel", C_STR}, {"day", C_DATE},
                                  {"sessions", C_I64}, {"purchasing_sessions", C_I64},
                                  {"revenue_cents", C_I64}, {"assisted_sessions", C_I64}};
    std::vector<ColBuf> cb(7);
    i64 rows = 0;
    const std::string organic = "organic";
    for (int c = 0; c <= NCAMP; c++) {
      for (int d = 0; d < NDAYS; d++) {
        CD s{0, 0, 0, 0};
        for (int t = 0; t < NT; t++) {
          const CD& x = cdT[t][c * NDAYS + d];
          s.sessions += x.sessions; s.purch += x.purch; s.rev += x.rev; s.assisted += x.assisted;
        }
        if (s.sessions == 0 && s.assisted == 0) continue;
        const std::string& id = c == NCAMP ? organic : campIds[c];
        const std::string& ch = c == NCAMP ? organic : campChan[c];
        put_str(cb[0], id.data(), (u32)id.size());
        put_str(cb[1], ch.data(), (u32)ch.size());
        put_i32(cb[2], DAY0 + d);
        put_i64(cb[3], s.sessions);
        put_i64(cb[4], s.purch);
        put_i64(cb[5], s.rev);
        put_i64(cb[6], s.assisted);
        rows++;
      }
    }
    PqWriter w;
    w.open(cdd + "/part-0.parquet", ccols);
    w.row_group(cb, rows);
    w.close();
  });
  parallel_for(NT, [&](size_t t, int) {
    auto& rows = urT[t];
    if (rows.empty() && !(t == 0 && totalRows == 0)) return;
    PqWriter w;
    w.open(ud + "/part-" + std::to_string(t) + ".parquet", ucols);
    const size_t RG = 1 << 20;
    for (size_t s = 0; s < rows.size(); s += RG) {
      size_t e = std::min(rows.size(), s + RG);
      std::vector<ColBuf> cb(6);
      cb[0].data.reserve((e - s) * 16);
      cb[1].data.reserve((e - s) * 4); cb[2].data.reserve((e - s) * 4);
      cb[3].data.reserve((e - s) * 4); cb[4].data.reserve((e - s) * 8);
      cb[5].defs.resize(e - s);
      for (size_t i = s; i < e; i++) {
        const UserRow& r = rows[i];
        u32 n;
        const char* nb = id_bytes(r.nk, r.nl, n);
        put_str(cb[0], nb, n);
        put_i32(cb[1], r.day);
        put_i32(cb[2], (int32_t)r.sessions);
        put_i32(cb[3], (int32_t)r.events);
        put_i64(cb[4], r.rev);
        if (r.top >= 0) { cb[5].defs[i - s] = 1; put_str(cb[5], catNames[r.top].data(), (u32)catNames[r.top].size()); }
      }
      w.row_group(cb, (i64)(e - s));
    }
    w.close();
  });
  campThread.join();
  lap("write");
  if (getenv("SS_DEBUG")) {
    FILE* f = fopen("/proc/self/smaps_rollup", "r");
    char line[256];
    while (f && fgets(line, sizeof line, f))
      if (strstr(line, "Rss") || strstr(line, "AnonHuge") || strstr(line, "Anonymous")) fputs(line, stderr);
    if (f) fclose(f);
  }
  // Unmap the big segments from several threads: munmap zaps page tables
  // under a shared lock, so this runs in parallel, unlike the exit teardown.
  if (!getenv("SS_SERIAL_EXIT")) {
    std::vector<char*> segs = POOL.all;
    parallel_for(segs.size(), [&](size_t i, int) { munmap(segs[i], SEG); });
    lap("unmap");
  }
  fflush(stdout);
  _exit(0);  // skip destructors: the OS reclaims the rest
}
