# Shopping Sessions (Databricks Buildathon)

Exact ETL for dirty click-stream logs → two daily Parquet tables. Fastest correct container wins.

**Spec:** [rules.md](./rules.md) (authoritative)  
**Plan:** [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md)  
**Track:** https://www.joinbuilders.org/buildathon/tracks/shopping-sessions

## Status

Rust pipeline is the scored entry (`Dockerfile` → `/app/shopping-sessions`).

| Check | Result |
| --- | --- |
| 36 fixtures | pass |
| seed-1 1M `check-dev` | exact MATCH · ~1.0–1.1 s (8 threads, warm) |
| ~20M local | ~25 s (8 threads) |

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

1. mmap + custom CSV dialect (rules.md exact)
2. Parallel per-file event/click parse → merge dedup
3. Union-Find linked IDs → sessionize (30 min gap) → asof attribution + assists
4. Aggregate → Snappy Parquet (`day` = Date32)

Env: `DATA_DIR` / `OUTPUT_DIR` / `SCRATCH_DIR` / `NCPU`. Optional `SS_TIMING=1` for stage logs.

## Submit

Public GitHub repo with root `Dockerfile`, e.g. https://github.com/ZheXie/shopping-sessions  
Sync from Origin:

```sh
cd ~/shiaoping && git pull origin main && git push github main
```
