# Shopping Sessions (Databricks Buildathon)

Exact ETL for dirty click-stream logs → two daily Parquet tables. Fastest correct container wins.

**Spec:** [rules.md](./rules.md) (authoritative)  
**Plan:** [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md)  
**Track:** https://www.joinbuilders.org/buildathon/tracks/shopping-sessions

## Status

Rust pipeline is the scored entry (`Dockerfile` → `/app/shopping-sessions`).

| Check | Result |
| --- | --- |
| 36 fixtures (binary and Docker image) | pass |
| seed-1 1M `check-dev` | exact MATCH · ~55 ms |
| 20M events + 5M clicks | ~0.9 s in-process, ~1.1–1.3 s `docker run` wall |
| 60M events + 15M clicks | ~3.1 s in-process, 9.5 GB RSS (5.4 GB of it the mapped input) |

Timings are from a 4-core, 16 GB VM with a warm page cache; the scorer has 8 hyperthreads.
5M, 20M and 30M outputs are identical to those of the first (slow, straightforward) Rust version.

Python reference remains in `pipeline/pipeline.py` (correct but slow).

## Quick start

```sh
# Local binary
make build-local
make fixtures          # 36/36
make dev               # needs `generate` on PATH (see third_party/generator)
make run-dev && make check-dev

# Scorer-shaped image
make build
make fixtures-docker
```

## Architecture (speed path)

1. `mmap` the inputs; AVX2 scan marks every `,` `\n` `"` (rules.md dialect, exact).
2. Linked IDs: lock-free union-find in one open-addressing table keyed by a 64-bit ID hash;
   afterwards each slot holds the person's key and name (smallest ID, inline up to 15 bytes).
3. Parse in 2 MiB chunks on `NCPU` threads. Each event becomes a 16-byte dedup key (event-id
   hash, ingest time) scattered by event-id hash and a 24-byte payload scattered by person.
   Scatter uses write-combining buffers and non-temporal stores.
4. Dedup per event-id partition; losers are flagged in their payload.
5. Per person partition: group, sort, sessionize (30 min gap), credit and assists with a
   sliding 7-day click window, emit `user_daily` rows; campaign totals are summed per thread.
6. Own Parquet writer (PLAIN, uncompressed, `day` = DATE): one `user_daily` file per thread.

The only dependency is `libc`. Env: `DATA_DIR` / `OUTPUT_DIR` / `NCPU`.
`SS_TIMING=1` prints stage timings.

## Submit

Public GitHub repo with root `Dockerfile`, e.g. https://github.com/ZheXie/shopping-sessions  
Sync from Origin:

```sh
cd ~/shiaoping && git pull origin main && git push github main
```
