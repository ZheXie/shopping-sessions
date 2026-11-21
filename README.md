# Shopping Sessions: C++ pipeline

One static C++17 binary (`src/ss.cpp`, no dependencies) that implements
`rules.md` exactly and writes both Parquet tables with a small built-in writer.

## Results (local, 8 cores, warm page cache)

| Dataset | Events | Time |
| --- | --- | --- |
| dev (seed 1) | 1M | ~0.1 s |
| practice-sized (seed 11) | 20M | ~1.0 s |
| seed 21 | 40M | ~2.0 s |

The PySpark starter baseline on the board is 171.91 s. Peak memory is about
4.3 GB at 40M events (about 6.5 GB expected at the 60M final, limit 24 GB).

The scorer (8 vCPU = 4 Cascade Lake cores with hyperthreads) runs about 2x
slower than this machine. `taskset -c 0-3 env SS_NOTHP=1 build/ss DATA OUT`
(4 cores, no transparent hugepages) reproduced the first board score almost
exactly, so it is the local stand-in for the scorer.

## Correctness checks

- 36/36 fixtures (`make fixtures`).
- Exact match with the published dev answer (`make check-dev`).
- Exact match with `reference/reference.py`, a slow, literal Python version of
  the rules, on seeds 2 to 6 (1M events each) and seed 31 (5M events).
- `reference/fuzz.py`: 1000 random, very dirty small datasets (quotes, `\r`,
  bad timestamps and offsets, long IDs, links, duplicates, files without a final
  newline, header-only files), with random chunk and buffer sizes to hit every
  boundary in the reader: 0 differences.
- The dev set also matches with 1-byte chunks and 64-byte read buffers.

## How it works

1. **Links first.** Parse `links/` in parallel, intern every ID per hash bucket,
   then run union-find on a background thread while the events parse. The root
   of each set is its byte-wise smallest ID, which is the person's name.
2. **Parse events and clicks** with `pread` into reusable 1 MB per-thread
   buffers. Lines are split with AVX2 masks for commas, quotes and newlines;
   every quoting rule is checked on those masks. Each valid, non-late event goes
   straight into its user-ID bucket (32 bytes). Every well-formed copy, valid or
   not, also writes a 20-byte dedup entry into its event-ID bucket: a 96-bit
   hash of `event_id`, `ingest_ts` in seconds, and a copy number.
3. **Dedup** per event-ID bucket: the copy with the smallest `ingest_ts` wins,
   and every other copy is marked dead in a bitmap of copy numbers. Dedup comes
   before validation, so an invalid winner still removes later valid copies.
   Event IDs compare by their 96-bit hash; the chance of any collision among
   60M copies is about 1e-14.
4. **Person mapping** per user-ID bucket, through that bucket's slice of the
   link table, which stays in cache. Events and clicks are re-bucketed by person.
5. **Sessions** per person bucket: group by person, sort by `ts`, split on gaps
   over 30 minutes, and give each session the day it starts. Credit and assists
   use one sliding window over the person's clicks sorted by (`ts`, `click_id`).
   Assists are counted as ranges of sessions per campaign, so whales with many
   clicks cost O(clicks + sessions).
6. **Output.** Uncompressed PLAIN Parquet, one `user_daily` file per thread.

An ID is exactly 16 bytes: an ID of up to 16 bytes is stored inline and
zero-padded; a longer one becomes a pointer to an arena copy plus its hash.
Nothing points back into the input, so read buffers are reused. Memory comes
from 64 MB segments in a shared pool: a phase that finishes hands its segments
to the next one, so each page is faulted once. This matters on the scorer,
where page faults and teardown of 4 KB pages were a large share of the time.

## Layout

- `src/ss.cpp`: the pipeline.
- `Dockerfile`: a static build on `debian:bookworm-slim`, run from `scratch`.
- `reference/reference.py`, `reference/fuzz.py`: the slow reference and the differential fuzzer.
- `scripts/`, `fixtures/`, `rules.md`: from the official starter.

Test hooks (never set by the scorer): `SS_CHUNK` (task size in bytes),
`SS_RBUF` (read buffer bytes), `SS_DEBUG` (memory stats). Phase timings go
to stderr.
