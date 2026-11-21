# Shopping Sessions (Databricks Buildathon)

Exact ETL for dirty click-stream logs → two daily Parquet tables. Fastest correct container wins.

**Spec:** [rules.md](./rules.md) (authoritative)  
**Plan:** [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md)  
**Track:** https://www.joinbuilders.org/buildathon/tracks/shopping-sessions

## Performance and validation

The root Dockerfile builds the Rust entry with profile-guided optimization.
The following medians compare the same generated inputs and count elapsed time
from launch through exit, including writing both tables.

| Generated events + clicks | Seed | Process `d24a0b7` | Process `478c814` | Process candidate | OCI `d24a0b7` | OCI candidate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 20M + 5M | 89017 | 306.1 ms | 268.7 ms | **234.2 ms** | 379.3 ms | **317.2 ms** |
| 60M + 15M | 53197 | 1006.5 ms | 913.3 ms | **758.5 ms** | 1077.0 ms | **831.9 ms** |

Measurements used Ryzen 7 7800X3D, WSL2, rustc 1.100.0 and eight workers.
All three executables used the repository's PGO build and were stripped.
Each binary was warmed once, then run nine times in rotating order with a
fresh output directory. The OCI pair ran seven times each in alternating
order, with warm input pages, rootless `runc`, a read-only root and input,
a private network namespace and host runtime libraries. The Docker daemon
was inaccessible to this user. The competition specifies eight pinned
vCPUs and 24 GB RAM. Its hidden input, CPU model and cache state may differ.

On the 20M seed, restricting eight workers to four local cores and forcing
AVX2 gave 529.6 → 439.3 ms. Additionally disabling transparent huge pages
for each process gave 816.0 → 725.6 ms. Both are seven-run medians.
On the 60M seed, peak RSS fell from 3.83 to 2.95 GiB and total Parquet
output from 181.93 to 129.27 MB.

The candidate passed all 36 fixtures in three modes (automatic dispatch,
forced AVX2, and one worker with AVX2), the official 1M answer, nine Rust
unit tests, the regression script and 120 randomized cases against the
Python reference. Both large inputs matched `d24a0b7` in every column with
zero duplicate keys. Seed 53197 produced 6,584,966 user rows and 28,014
campaign rows. A separate reference comparison exercised dense users with
more than 6,000 clicks each, including tied IDs and attribution boundaries.

### Previous measurements at `d24a0b7`

The following historical medians compare `d24a0b7` to `ad075c9`.

| Generated input | Seed | Binary, original → optimized | Rootless OCI, original → optimized |
| --- | ---: | ---: | ---: |
| 20M events + 5M clicks | 714 | 0.562 → **0.359 s** | 0.657 → **0.441 s** |
| 20M events + 5M clicks | 88231 | 0.542 → **0.382 s** | 0.676 → **0.441 s** |
| 60M events + 15M clicks | 4213 | 2.040 → **1.259 s** | 2.097 → **1.549 s** |

WSL2 on a Ryzen 7 7800X3D, eight visible CPUs, warm page cache, Linux
filesystem for input and output, rustc 1.100.0. Each pair used the same input,
alternating the order and creating fresh output directories. The 20M medians use
seven process runs and five OCI runs per binary, after one warmup. The 60M
medians use three runs after warmup. Process timing includes exit and memory
reclamation. OCI timing spans `runc run` through exit with read-only input and
rootfs, private networking, host runtime libraries and eight CPUs. The Docker
daemon was unavailable on this host. Official hidden-set timing is pending.

- All 36 published fixtures pass with automatic SIMD dispatch and with AVX2 forced.
- Both tables on the public seed-1 dev set match the published answer exactly.
- Both 20M datasets and the 60M dataset match the original release output in
  every column with zero duplicate keys. The 60M result contains 6,643,296
  user rows and 28,014 campaign rows. Peak RSS fell from 9.17 to 3.77 GiB
  in the local 60M correctness runs.
- Targeted regressions check UTF-8 and embedded NUL IDs, tied clicks, malformed
  and cross-chunk rows, missing final newlines, and duplicate ingest dates
  outside the inline range. The earliest copy still wins across centuries.
- The Python reference remains in `pipeline/pipeline.py`.

### Memory traffic round (after `d24a0b7`)

These earlier measurements used four local cores and eight workers as a
limited-core proxy (`taskset -c 0-3`). Each figure is the minimum and median
of 9 alternating runs on 20M events + 5M clicks (seed 11), with the PGO build.

| Setup | `d24a0b7` | This round |
| --- | ---: | ---: |
| 4 cores, transparent huge pages | 561 / 577 ms | **488 / 506 ms** |
| 8 cores | 321 / 325 ms | **281 / 293 ms** |
| 4 cores, huge pages disabled (`prctl`) | 787 / 807 ms | 780 / 819 ms |

Page faults per run fell from 61k to about 11k, and peak RSS from 1.35 to
1.16 GB.

- A global allocator gives every block of 1 MB or more its own 2 MB aligned,
  huge-page mapping and grows it with `mremap`. glibc had placed the growing
  vectors (edge lists, output columns, aggregation scratch) in 4 KB pages
  and copied them on every doubling.
- Click files are read with `pread` like event files instead of being mapped
  and populated. Line boundaries come from the read buffer, so the mappings
  are no longer touched on the normal path.
- Each click carries its click ID as an exact, order-preserving key: 1-10
  characters of `[0-9A-Za-z]`, 6 bits each. Same-millisecond tie-breaks
  compare keys and no longer fault in 64 KB of the click file per comparison.
- Output columns are sized up front from the number of live events.

Checks: 36/36 fixtures (automatic dispatch, AVX2 forced, one worker),
`scripts/test_regressions.py`, the dev answer, the reference on six more seeds
up to 5M events, 600 differential fuzz runs, and output identical to
`d24a0b7` on 40M events (4,405,638 user rows, 28,014 campaign rows).

Tried and dropped (no gain or slower): AVX-512 codegen (`cascadelake`),
partitions of 25k-200k rows, 1-4 MB tasks, other lookup batch and scan block
sizes, PGO trained on 5M events, a dedup table at load 3/4, and a person table
sized for people.

## Build and check

The release target is Linux x86-64-v3. The container builds the scorer binary
with profile-guided optimization (PGO). The build trains on the public generator
with both automatic SIMD dispatch and the AVX2 fallback. Only the stripped
binary goes into the runtime image.

```sh
make build
make fixtures-docker

# Fast local development build
make build-local
make fixtures
(cd rust-pipeline && cargo test --release)

# Match the scorer build locally
rustup component add llvm-tools-preview
make build-local-pgo
python3 scripts/test_regressions.py rust-pipeline/target/release/shopping-sessions

# Public dev answer, using the generator on PATH
make dev
NCPU=8 DATA_DIR=dev OUTPUT_DIR=out rust-pipeline/target/release/shopping-sessions
make check-dev
```

Use a fresh output directory for each invocation. The benchmark harness does so
automatically, warms up each binary once, alternates the run order, and times
the entire child process. To build a baseline and reproduce a practice run:

```sh
scratch=$(mktemp -d)
git archive d24a0b7 rust-pipeline scripts/build_pgo.sh third_party/generator | tar -x -C "$scratch"
sh "$scratch/scripts/build_pgo.sh"
make build-local-pgo
cargo build --release --locked --manifest-path third_party/generator/Cargo.toml
third_party/generator/target/release/generate --seed 89017 --events 20000000 --clicks 5000000 --output "$scratch/practice"
python3 scripts/benchmark.py "$scratch/practice" "$scratch/rust-pipeline/target/release/shopping-sessions" rust-pipeline/target/release/shopping-sessions --runs 9
```

## Implementation

- Parser threads read large event files into reusable buffers with `pread`.
  A read-only mapping provides stable addresses for IDs needed after parsing.
  SIMD builds an index of delimiters and wrapping quotes while checking the
  contest dialect. AVX-512 byte compression is selected on capable CPUs.
- Each aligned cache line in the linked-ID table stores four keys and their
  canonical names. AVX2 checks all four inline keys at once, and an overflow
  bit records whether a lookup may need to continue in the next group.
- A click stores its campaign, time and tie-break ID in 16 bytes. Common ASCII
  IDs preserve byte order in a 63-bit key. Other IDs refer to the original
  bytes so a timestamp tie always uses the exact ID. Canonical decimal product
  and campaign IDs use compact direct tables with an exact string fallback.
- Parsing scatters 16-byte event keys and 16-byte payloads. Dedup keeps the
  earliest ingest even when the winning event fails validation. Only purchases
  look up a product category.
- Larger person partitions start first. Small groups can use SIMD sorting
  networks. For dense users, each 1,048,576 ms bin fits inside the 30-minute
  session gap. The bin's first and last event decide whether adjacent occupied
  bins join. Only purchases need individual category aggregation. Clicks with
  the same campaign and first eligible session can retain the latest click,
  breaking equal times by ID. That click covers every later session the others
  could still assist.
- Each worker writes its own Parquet file. A dictionary stores each person ID
  once per worker, and run-length encoding covers consecutive daily rows.
  Byte and 16-bit dictionary indices also compact the day, session and event
  columns. Campaign totals use a final reduction.

The runtime takes `DATA_DIR`, `OUTPUT_DIR` and `NCPU`. Set `SS_TIMING=1` for
stage diagnostics, `SS_AVX2=1` to exercise the AVX2 fallback, or `SS_READ=mmap`
to compare the mapped event reader. The benchmark harness disables timing
diagnostics to exclude memory-reporting overhead.

## Submit

Public GitHub repo with root `Dockerfile`, e.g. https://github.com/ZheXie/shopping-sessions  
Sync from Origin:

```sh
cd ~/shiaoping && git pull origin main && git push github main
```
