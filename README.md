# Shopping Sessions (Databricks Buildathon)

Exact ETL for dirty click-stream logs → two daily Parquet tables. Fastest correct container wins.

**Spec:** [rules.md](./rules.md) (authoritative)  
**Plan:** [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md)  
**Track:** https://www.joinbuilders.org/buildathon/tracks/shopping-sessions

## Status

**Correctness baseline is green:**
- 36/36 fixtures pass
- `make check-dev` / seed-1 1M-event set: **exact MATCH** (~23s in this environment, pure Python parse)

Next: speed (Rust / parallel parse) toward sub-2s practice times. See `IMPLEMENTATION_PLAN.md`.

## Quick start

```sh
# Python for local fixture runs
pip install -r requirements.txt

# All 36 edge-case fixtures (no Docker)
python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py

# Dev set (needs `generate` from third_party/generator)
cd third_party/generator && cargo build --release
export PATH="$PWD/target/release:$PATH"
cd ../..
make dev                 # 1M events, seed 1
DATA_DIR=dev OUTPUT_DIR=out SCRATCH_DIR=scratch python3 pipeline/pipeline.py
make check-dev           # vs ./dev-answer
```

With Docker (scorer-shaped run):

```sh
make build
make fixtures
make run-dev && make check-dev
```

## Layout

| Path | Purpose |
| --- | --- |
| `pipeline/pipeline.py` | Entry point the container runs |
| `fixtures/` | 36 hand-written edge cases |
| `dev-answer/` | Expected tables for seed-1 1M-event set |
| `third_party/generator/` | Rust data generator |
| `Dockerfile` / `run.sh` | Scorer image + entry |

## Strategy (short)

1. **Custom CSV lexer** — do not trust Polars/DuckDB CSV for this dialect  
2. Pass all 36 fixtures, then `make check-dev`  
3. Port hot path to **Rust** for sub-2s practice times (board leaders ~1.2–2s)  
4. Submit public repo + amd64 Dockerfile; keep practice ≪ 60s for the 3-minute final cap

## Limits (scorer)

8 vCPU · 24 GB · no network · `DATA_DIR` / `OUTPUT_DIR` / `SCRATCH_DIR` · uid 1000 · read-only root except `/tmp` and mounts
