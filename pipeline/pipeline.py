#!/usr/bin/env python3
"""
Shopping Sessions: Polars starter (skeleton).

This runs end to end inside the scorer's container and writes both output tables
with the exact schema, but the tables are EMPTY, so the checker will mark it
incorrect. It gives you the plumbing; the rules are yours to implement.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import glob
import json
import os

NCPU = os.environ.get("NCPU")
if NCPU:
    # Must be set before polars is imported.
    os.environ.setdefault("POLARS_MAX_THREADS", NCPU)

import polars as pl  # noqa: E402

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
print("window:", manifest["window_start"], "to", manifest["window_end"])
print("polars threads:", pl.thread_pool_size())

event_files = sorted(glob.glob(os.path.join(DATA, "events", "*.csv")))
click_files = sorted(glob.glob(os.path.join(DATA, "clicks", "*.csv")))
print(f"{len(event_files)} event files, {len(click_files)} click files")

# Implementation order — see IMPLEMENTATION_PLAN.md and rules.md.
#
#   1. Custom CSV lexer (do NOT trust polars read_csv for this dialect)
#   2. F1/F2/F3 timestamp parse → ms UTC; apply event/click windows
#   3. Links → Union-Find → person_id = byte-min ID in component
#   4. Events: parse → dedup(event_id, min ingest_ts) BEFORE validate
#              → validate → late drop (ingest_ts - ts > 24h) → map to person
#   5. Sessionize per person (gap > 30 min on distinct ts); day = UTC date(start)
#   6. Clicks: parse → dedup → validate (campaign in campaigns.csv) → person
#   7. Credit: latest click with ts ≤ start and start - ts ≤ 7d (tie: min click_id)
#      Assists: distinct campaigns in [start - 7d, start]; organic never assists
#   8. Aggregate user_daily + campaign_daily; write Parquet (day = DATE)
#
# Memory: prefer typed columns / LazyFrame streaming; spill to SCRATCH.
# Speed path after fixtures+check-dev: port stages to Rust.

USER_DAILY = {
    "user_id": pl.String, "day": pl.Date, "sessions": pl.Int64,
    "events": pl.Int64, "revenue_cents": pl.Int64, "top_category": pl.String,
}
CAMPAIGN_DAILY = {
    "campaign_id": pl.String, "channel": pl.String, "day": pl.Date,
    "sessions": pl.Int64, "purchasing_sessions": pl.Int64, "revenue_cents": pl.Int64,
    "assisted_sessions": pl.Int64,
}

# Write both tables with the exact schema the checker expects.
for name, schema in (("user_daily", USER_DAILY), ("campaign_daily", CAMPAIGN_DAILY)):
    folder = os.path.join(OUT, name)
    os.makedirs(folder, exist_ok=True)
    pl.DataFrame(schema=schema).write_parquet(os.path.join(folder, "part-0.parquet"))
print("wrote empty user_daily and campaign_daily (skeleton)")
