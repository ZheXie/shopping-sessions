# Shopping Sessions — Implementation Plan

Contest: [Databricks · Shopping Sessions](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)  
Metric: container wall-clock (lower is better). Correctness is binary.  
Final: ~60M events, 3 min hard cap; practice board leaders are ~1.2–2.0 s on ~20M events.

## Goal

Ship a public GitHub repo with a root `Dockerfile` that:

1. Reads `/data` (dirty CSVs + `manifest.json`)
2. Writes exact `/output/user_daily/` and `/output/campaign_daily/` Parquet
3. Finishes under ~60 s on practice (safe for the 3 min final); target **&lt; 5 s** to compete, **&lt; 2 s** to win

This repo starts from the official Polars starter (fixtures + scorer scripts). `rules.md` always wins over starter comments.

---

## Strategy (two tracks)

| Track | Engine | Why | When |
| --- | --- | --- | --- |
| **A — Correctness first** | Polars + **custom CSV lexer** | Fast iteration on 36 fixtures | Days 1–correctness |
| **B — Speed (winning path)** | Rust (custom parse → UF → sessions → attr → parquet) | Leaders are ~1–2 s; Python/JVM startup alone burns budget | After fixtures green, or from day 1 if confident |

**Recommendation:** Implement Track A until all fixtures + `make check-dev` pass, then port the same stages to Rust (Track B) for the timed submission. Do not trust Polars/DuckDB’s built-in CSV readers for the contest dialect.

---

## Pipeline stages (exact order)

```
manifest.json
     │
     ▼
links/*.csv ──► parse ──► Union-Find ──► person_id = min(ID) over component
     │
events/*.csv ──► parse ──► dedup(event_id, min ingest_ts) ──► validate ──► late drop
     │                         │
     │                         └── map user_id → person_id
     ▼
sessionize per person (gap > 30 min on distinct ts) ──► session_day = UTC date(start)
     │
clicks/*.csv ──► parse ──► dedup(click_id) ──► validate(campaign ∈ campaigns)
     │                         │
     │                         └── map user_id → person_id
     ▼
credit: latest click with click.ts ≤ start and start−click.ts ≤ 7d (tie: min click_id)
assists: distinct campaigns with click in [start−7d, start]
     │
     ├──► user_daily    (sessions, events, revenue, top_category)
     └──► campaign_daily (credited + assisted; organic channel = organic)
```

### Stage contracts

1. **CSV dialect** — `\n` only ends lines (`\r` is data). Quotes wrap whole fields only. Wrong field count / bad quotes / empty line → drop. Spaces are significant. Empty field → null.
2. **Timestamps** — F1 / F2 / F3 only → ms UTC. Event window `[W0, W1)`. Click window `[W0−7d, W1)`. Impossible calendar / bad offset → drop.
3. **Links** — transitive UF; person name = byte-min ID (including IDs with no events). Self / duplicate links OK. Empty side → drop.
4. **Events** — drop empty `event_id`; **dedup before validate** (earliest `ingest_ts` wins even if invalid); validate type ∈ {view,cart,purchase}; purchase price ASCII digits 1..1e7; late if `ingest_ts − ts > 24h` (exactly 24h kept).
5. **Sessions** — sort by `ts` within person; new session when gap between **distinct** timestamps &gt; 1_800_000 ms; equal `ts` share a session regardless of read order; day = UTC date of first event `ts`.
6. **Categories** — join `products.csv`; missing → `unknown`.
7. **Attribution** — latest qualifying click; else campaign `organic` / channel `organic`. Assists = distinct campaigns in the same click pool (incl. credited). `organic` never assists.
8. **Outputs** — Parquet under the two folders; `day` must be Parquet DATE; row/file order irrelevant.

---

## Work plan

### Phase 0 — Harness (done in scaffold)

- [x] Copy Polars starter, `rules.md`, fixtures, `dev-answer/`, generator source
- [ ] Fill `TEAM.md`
- [ ] Build generator: `cd third_party/generator && cargo build --release` → put `generate` on `PATH`
- [ ] Local loop: `make fixtures` then `make dev && make run-dev && make check-dev`

### Phase 1 — Custom CSV + timestamps (fixtures E13, E15, E17–E20)

Build a line parser that matches the dialect exactly (do **not** use `pl.read_csv` / DuckDB `read_csv` as the source of truth).

- Split on `\n` only; preserve `\r` inside fields
- Quote rules: `"u1"` → `u1`; `""` → empty/null; quote mid-field → malformed
- Wrong arity → drop
- Timestamp parser: F1 / F2 / F3 with leap-year and offset validation; ASCII digits only for F3 and prices

**Exit:** E13–E20 (and related) pass in isolation.

### Phase 2 — Links (L1–L4)

- Union-Find with path compression + union by rank
- Intern strings; compare byte-wise for min ID
- Map every raw `user_id` → canonical person after links load

**Exit:** all L\* fixtures green.

### Phase 3 — Events + sessions (E1–E16)

- Dedup: `groupby event_id` → argmin `ingest_ts` (pre-validation)
- Validate + late filter
- Sessionize: per person, sort unique timestamps, assign session ids via gap rule, broadcast to equal-`ts` events
- Aggregate per `(person, day)`: sessions, events, revenue, top_category (max spend, tie → min category)

**Exit:** all E\* fixtures green; empty/organic campaign rows as expected.

### Phase 4 — Clicks, credit, assists (A1–A12)

- Dedup clicks; validate against `campaigns.csv` set
- For each session start: choose credit click (asof join semantics with tie-break on `click_id`)
- Assists: explode qualifying campaigns; distinct per `(session, campaign)`
- Build `campaign_daily` including assist-only rows (credited cols = 0)

**Exit:** all 36 fixtures green (`make fixtures`).

### Phase 5 — Dev set (1M events)

```sh
make dev          # seed 1, 1M events / 250k clicks
make run-dev
make check-dev    # exact match vs ./dev-answer
```

Fix any remaining edge cases. Record wall time under 8 CPU / 24 GB if Docker available.

**Self-check:** Σ sessions and Σ revenue_cents equal across the two tables; Σ assisted_sessions ≥ credited non-organic sessions.

### Phase 6 — Speed (Track B)

Target practice: **&lt; 5 s** (safe), aspire **&lt; 2 s**.

Hotspots (in order of impact):

1. **CSV parse** — parallel per-file (rayon / thread pool on `NCPU`); avoid UTF-8 revalidation where safe; string interning for IDs
2. **Avoid Python object churn** — Rust end-to-end, or Polars only after typed Arrow columns are built
3. **Dedup** — hash maps sized for ~60M; spill to `/scratch` only if needed
4. **Sessionize** — sort by `(person, ts)`; single pass gap scan
5. **Attribution** — sort clicks by `(person, ts, click_id)`; asof per session (merge join), not nested loops
6. **Parquet write** — snappy/zstd, few large parts; no `_SUCCESS` needed
7. **Cold start** — thin Docker image; no JVM; pre-warm nothing (no network at run)

The final image starts the Rust binary directly (`CMD ["/app/shopping-sessions"]`); it needs no scratch space.

### Phase 7 — Submit discipline

- Public repo ≤ 50 MB; **no `/data` checked in**
- `linux/amd64`, image ≤ 8 GB
- 2 submits / hour; practice under 20 s → median of 3
- Final: median of 3 on ~60M; any run &gt; 3 min → **no score** → keep practice ≪ 60 s
- Leave every scored commit reachable until results publish (MIT license)

---

## Module layout (target)

```
pipeline/
  pipeline.py          # entry (or thin wrapper calling rust binary)
  csv_lex.py           # dialect-faithful lexer
  timestamps.py        # F1/F2/F3
  links.py             # union-find
  events.py            # parse → dedup → validate → late
  sessions.py          # gap sessionize + day
  clicks.py            # parse → dedup → validate
  attribution.py       # credit + assists
  aggregates.py        # user_daily / campaign_daily
  write.py             # parquet schemas
# later:
src/                   # Rust crate mirroring the same stages
Dockerfile             # CMD → pipeline
```

---

## Fixture map (use while coding)

| Group | Cases | Focus |
| --- | --- | --- |
| Sessions / events | E1–E16 | 30 min edge, midnight, dedup-before-validate, late 24h, offsets, ties |
| Dirty dialect | E17–E20 | offsets, Unicode digits, spaces, `\r\n` |
| Attribution | A1–A12 | 7d window, ties, organic, multi-session credit, assists |
| Links | L1–L4 | transitive chains, min ID name, malformed links |

Run one case while developing:

```sh
DATA_DIR=fixtures/E5/input OUTPUT_DIR=/tmp/out SCRATCH_DIR=/tmp/scratch \
  python3 pipeline/pipeline.py
```

---

## Risks & mitigations

| Risk | Mitigation |
| --- | --- |
| Built-in CSV ≠ rules | Custom lexer; fixtures E13/E15/E19/E20 first |
| `\d` matches Unicode | Use `[0-9]` / ASCII byte checks only |
| Dedup keeps bad row | Explicitly dedup **before** validate (E5) |
| Equal timestamps | Sessionize on distinct `ts`, then join back (E16) |
| Shared-device mega-person | UF + careful memory; generator warns of thousands of IDs in one component |
| OOM at 60M | Streaming / chunked parse; `/scratch` spills; avoid holding 3 string copies |
| Almost-right → 0 score | Never submit until fixtures + `check-dev` exact |

---

## Success criteria

1. `make fixtures` — 36/36 exact
2. `make check-dev` — exact vs `dev-answer/`
3. Practice board time **&lt; 60 s** (final-safe), ideally **&lt; 2 s**
4. Dockerfile builds offline-runnable `linux/amd64` image under contest limits
