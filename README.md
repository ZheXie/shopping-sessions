# Shopping Sessions: C++ pipeline

One static C++17 binary (`src/ss.cpp`, no dependencies) that implements
`rules.md` exactly and writes both Parquet tables with a small built-in writer.

## Results (20M events, warm page cache)

| Setup | Time |
| --- | --- |
| Scorer stand-in: 4 cores, no transparent hugepages | ~1.05 s |
| This machine: 8 cores | ~0.75 s |

The scorer (8 vCPU = 4 Cascade Lake cores with hyperthreads) is much slower
than this machine. `taskset -c 0-3 env SS_NOTHP=1 build/ss DATA OUT` reproduced
the first board score (2.36 s locally, 2.3 s on the board), so it is the local
stand-in. Under it, the versions so far went 2.36 s -> 1.67 s -> 1.05 s.

At 40M events the run takes about 1.35 s on 8 cores, with a peak of 2.0 GB of
memory (limit 24 GB). The PySpark starter baseline on the board is 171.91 s.

## Correctness checks

- 36/36 fixtures (`make fixtures`).
- Exact match with the published dev answer (`make check-dev`).
- Exact match with `reference/reference.py`, a slow, literal Python version of
  the rules, on seeds 2 to 6 (1M events each) and seed 31 (5M events).
- `reference/fuzz.py`: thousands of random, very dirty small datasets (quotes,
  `\r`, bad timestamps and offsets, long and odd IDs, links, duplicates, files
  without a final newline, header-only files), with random chunk and buffer
  sizes to hit every boundary in the reader: 0 differences.
- The dev set also matches with 1-byte chunks and 64-byte read buffers.
- The fast timestamp parser and the fast product lookup were each checked
  against the plain versions on millions of valid and mutated strings.

## How it works

1. **Links first.** Parse `links/` in parallel, intern every ID per hash bucket,
   then run union-find on a background thread while the events parse. The root
   of each set is its byte-wise smallest ID, which is the person's name.
2. **Parse events and clicks** with `pread` into reusable 1 MB per-thread
   buffers. Lines are split with AVX2 masks for commas, quotes and newlines.
   Timestamps use SIMD for epoch-ms and a branch-light fixed-layout parse for
   the ISO forms. Each valid, non-late event goes straight into its user-ID
   bucket (16 bytes). Every well-formed copy, valid or not, also writes a
   16-byte dedup entry into its event-ID bucket: `event_id`, `ingest_ts` in
   seconds, and where the copy's event record is.
3. **Dedup** per event-ID bucket: the copy with the smallest `ingest_ts` wins,
   and every other copy is marked dead in a bitmap. Dedup comes before
   validation, so an invalid winner still removes later valid copies.
4. **Person mapping** per user-ID bucket, through that bucket's slice of the
   link table, which stays in cache. Events and clicks are re-bucketed by person.
5. **Sessions** per person bucket: a radix sort of the bucket by `ts` and a
   stable scatter by person give each person's events in time order; sessions
   split on gaps over 30 minutes and take the day they start. Credit and
   assists use one sliding window over the person's clicks sorted by
   (`ts`, `click_id`). Assists are counted as ranges of sessions per
   campaign, so whales with many clicks cost O(clicks + sessions).
   `user_daily` rows are written as each bucket finishes, one Parquet row
   group per bucket.
6. **Output.** Uncompressed PLAIN Parquet.

**IDs are 8-byte keys.** The generator's IDs are 1 to 10 characters of
`[0-9A-Za-z]`. Each character becomes a 6-bit symbol, ordered like ASCII, so
the key is exact and comparing keys as integers is byte-wise string order.
Packing is done with SIMD. Any other ID becomes a 63-bit hash plus an entry in
a registry of its bytes, so unusual IDs (spaces, other characters, longer than
10) stay correct.

**Memory is the main cost on the scorer.** Without transparent hugepages,
every fresh 4 KB page costs a fault and a zeroing on the way in and a free on
the way out. So records are small (16 to 24 bytes), and 64 MB segments are
pooled and handed from one phase to the next. Each block is given back to the
kernel right after its last read, on all workers in parallel, which leaves
little for the exit teardown.

## Layout

- `src/ss.cpp`: the pipeline.
- `Dockerfile`: a static build on `debian:bookworm-slim`, run from `scratch`.
- `reference/reference.py`, `reference/fuzz.py`: the slow reference and the differential fuzzer.
- `scripts/`, `fixtures/`, `rules.md`: from the official starter.

Test hooks (never set by the scorer): `SS_CHUNK` (task size in bytes),
`SS_RBUF` (read buffer bytes), `SS_NOTHP` (no hugepage advice),
`SS_NOFREE` (keep pages until exit), `SS_DEBUG` (memory stats). Phase
timings go to stderr.
