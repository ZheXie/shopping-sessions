# Shopping Sessions (Databricks Buildathon)

Exact ETL for dirty click-stream logs → two daily Parquet tables. Fastest correct container wins.

**Spec:** [rules.md](./rules.md) (authoritative)  
**Plan:** [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md)  
**Track:** https://www.joinbuilders.org/buildathon/tracks/shopping-sessions

## Performance and validation

The scored entry is the Rust binary built by the root Dockerfile. These local
medians compare the optimized entry to upstream commit `ad075c9`.

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
git archive ad075c9 rust-pipeline | tar -x -C "$scratch"
(cd "$scratch/rust-pipeline" && cargo build --release --locked)
cargo build --release --locked --manifest-path third_party/generator/Cargo.toml
third_party/generator/target/release/generate --seed 714 --events 20000000 --clicks 5000000 --output "$scratch/practice"
python3 scripts/benchmark.py "$scratch/practice" "$scratch/rust-pipeline/target/release/shopping-sessions" rust-pipeline/target/release/shopping-sessions --runs 7
```

## Implementation

- Parser threads read large event files into reusable buffers with `pread`.
  A read-only mapping provides stable addresses for IDs needed after parsing.
  SIMD builds an index of delimiters and wrapping quotes while checking the
  contest dialect. AVX-512 byte compression is selected on capable CPUs.
- Linked IDs use a compact concurrent union-find. Common short IDs have a
  reversible one-word representation. Other IDs compare by their full bytes.
- Parsing scatters 16-byte event keys and 16-byte payloads. Dedup keeps the
  earliest ingest even when the winning event fails validation. Only purchases
  look up a product category.
- Larger person partitions start first. Small groups can use SIMD sorting
  networks, and large groups use a time radix sort when their timestamp range
  permits it. Sessionization and the seven-day attribution window run per person.
- Each worker writes its own Parquet file. A dictionary stores each person ID
  once per worker, and run-length encoding covers consecutive daily rows.
  Campaign totals use a final reduction.

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
