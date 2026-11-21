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
5.4 GB at 40M events (about 8 GB expected at the 60M final, limit 24 GB).

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
   straight into its user-ID bucket. Every well-formed copy, valid or not, also
   writes a 56-byte dedup entry into its event-ID bucket.
3. **Dedup** per event-ID bucket: the copy with the smallest `ingest_ts` wins,
   and every other valid copy is marked dead in place. Dedup comes before
   validation, so an invalid winner still removes later valid copies.
4. **Person mapping** per user-ID bucket, through that bucket's slice of the
   link table, which stays in cache. Events and clicks are re-bucketed by person.
5. **Sessions** per person bucket: group by person, sort by `ts`, split on gaps
   over 30 minutes, and give each session the day it starts. Credit and assists
   use one sliding window over the person's clicks sorted by (`ts`, `click_id`).
   Assists are counted as ranges of sessions per campaign, so whales with many
   clicks cost O(clicks + sessions).
6. **Output.** Uncompressed PLAIN Parquet, one `user_daily` file per thread.

IDs are carried inline (their first 16 bytes), and IDs longer than that are
copied to an arena. Nothing points back into the input, so there is no large
mmap to tear down at exit. Allocation uses bump arenas on transparent
hugepages.

## Layout

- `src/ss.cpp`: the pipeline.
- `Dockerfile`: a static build on `debian:bookworm-slim`, run from `scratch`.
- `reference/reference.py`, `reference/fuzz.py`: the slow reference and the differential fuzzer.
- `scripts/`, `fixtures/`, `rules.md`: from the official starter.

Test hooks (never set by the scorer): `SS_CHUNK` (task size in bytes),
`SS_RBUF` (read buffer bytes), `SS_DEBUG` (memory stats). Phase timings go
to stderr.
