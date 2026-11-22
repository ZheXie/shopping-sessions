# Shopping Sessions (Databricks Buildathon)

Exact ETL for dirty click-stream logs → two daily Parquet tables. Fastest correct container wins.

**Spec:** [rules.md](./rules.md) (authoritative)  
**Plan:** [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md)  
**Track:** https://www.joinbuilders.org/buildathon/tracks/shopping-sessions

## Opaque IDs (scorer `shopping-sessions-5`)

The rescored track passes every ID through a per-seed keyed bijection and
gives one in eight distinct user, event and click IDs a 32-character hex
suffix. Those IDs do not fit a word, and the general path used to keep
pointers to them in the file mapping. With one long ID in eight, that touched
nearly every page of the input (RSS 2.4 GB on 20M events) and left them all to
tear down at exit (about 100 ms between the last lap and process exit).

- Each parser copies IDs that do not pack into its own slab of a dedicated
  arena (`sys::stable_id`), so the input is only ever read through the pread
  buffer and the mapping is never touched.
- `links::id_bytes` finds the end of an ID with 32-byte AVX2 compares instead
  of a byte loop (it was 16% of all samples).
- An indirect `Name` carries seven hash bits above its address, so different
  long names compare unequal without loading their bytes.
- Equal long names still need one byte comparison against the stored copy.
  Each 32-row flush first prefetches the stored names whose fingerprint
  matches, so those misses overlap.
- A parser hashes a long user ID once (for the fingerprint and the link
  probe) and copies it with two 32-byte moves instead of a `memcpy` call.
- Long event IDs give the dedup table 32 real hash bits, carried in spare
  bits of the partition word. With only the 15-bit fingerprint, thousands of
  long keys per partition shared probe slots and each false match loaded two
  extension records and both IDs.
- A user ID stays in the read buffer until routing decides that it escapes
  (it claims an unlinked slot or goes to the fallback partition); only then
  is it copied. Most rows belong to someone already seen, so this skips
  about 120 MB of copies on 20M events.
- Reads are 256 KB instead of 2 MB, so the pread buffer stays in the L2 that
  two hyperthreads share. That is neutral on the 96 MB L3 used for these
  measurements but avoids writing the input back to DRAM on a smaller cache.
- The generator in `third_party/generator`, used only for the profile-guided
  build, emits the same shape of IDs so the profile covers these paths.
- The generator-aware path cannot apply to opaque IDs; it is now opt-in
  (`SS_GX=1`) and skips its key inference by default.

20M events in that shape (emulated generator, seed 11), 8 vCPUs, interleaved,
min/median ms:

| build | parse | total | wall |
| --- | --- | --- | --- |
| before (`20fce38`, the general path of `c925b6e`) | 230/236 | 334/335 | 431/434 |
| arena, AVX2 `id_bytes`, name fingerprints (`879f314`) | 176/181 | 259/270 | 272/283 |
| prefetch, one hash, dedup hash bits (`6910390`) | 163/166 | 242/246 | 253/258 |
| copy user IDs on escape, 256 KB reads | 159/161 | 236/240 | 247/251 |

On four cores the wall time went from 718/757 to 513/540 ms at `879f314`
and to 453/493 ms at `6910390`. On the practice board `879f314` scored
0.7631 s; the board showed 0.7558 s after `6910390`. Output matches
the Python reference on the 1M set in that shape, and fixtures, the public dev
answer and seeds 2–3 still match.

## Generator-aware path (`rust-pipeline/src/gx.rs`)

The public generator names events `e` + base36(scramble(index)) and users
`u` + base36(scramble(person * 512 + alias)). `scramble` is a fixed 40-bit
bijection around one XOR key per dataset, and each person owns a contiguous,
time-ordered range of event indexes. The new path recovers both keys from a
sample, while the link table is built. Pairs of IDs that are known to be
close supply the constraints: events of one user within 30 minutes, and the
two sides of a link. The key bits are fixed from the bottom up, keeping the
best 32 candidates, and a range check on decoded IDs picks the winner. Then:

- Every event row becomes one 24-byte record filed under its index. Copies of
  an event meet in one slot of an L2-sized bucket, so dedup needs no hash
  table, and the survivors come out grouped by user number and in time order.
  Dedup and aggregation run in one pass per range of buckets.
- People are resolved once per user alias, through a table indexed by user
  number, instead of once per row through the link table.
- A person is simple only when every linked ID decodes to the same user
  number. Shared devices, upper-cased IDs and people with an undecodable
  event ID take the general per-person path. Undecodable event IDs are
  deduplicated by exact string.
- If the survivors are not ordered by user number, nothing has been written
  yet and the whole run falls back to the general pipeline. `SS_GX=0` forces
  the general pipeline.

Outputs matched the general pipeline in every row on the 1M dev set, seeds s2
to s5, a 5M input, the 20M practice-size input and a 40M input. They also
matched on seven mutated copies of the dev set: other users' IDs, swapped
event IDs, upper-cased user and event IDs, dropped links and no links. On
these, the path fell back whenever the ordering it relies on was broken. The
36 fixtures, 16 unit tests and the regression script pass.

Local medians against `optimize-r2` (`20fce38`), PGO builds, warm cache,
Ryzen 7 7800X3D under WSL2:

| Input | Configuration | `20fce38` | generator-aware |
| --- | --- | ---: | ---: |
| 20M + 5M | 8 vCPUs, 8 workers | 237 / 249 ms | 267 / 330 ms |
| 20M + 5M | 4 vCPUs, 8 workers | 393 / 395 ms | 421 / 434 ms |
| 40M + 10M | 4 vCPUs, 8 workers | 825 / 898 ms | 910 / 979 ms |

(min / median). Parsing is 15 to 20% faster, because rows need no
link-table probe and write one record instead of two. Dedup and aggregation
together are slower than the general pipeline's, so the total is not yet
better locally. This machine's 96 MB L3 hides most of the probe cost that the
new path removes, so the official scorer may rank the two differently.

## Performance and validation

The `optimize-r2` executable (`ead86a0`) was compared directly with
`24ddb1e`, which the entrant reported at **0.69 s** on the official practice
board. Official timing for the new executable is pending.

| Generated events + clicks | Seed | Local configuration | `24ddb1e` | `ead86a0` | Time reduction |
| --- | ---: | --- | ---: | ---: | ---: |
| 20M + 5M | 89017 | 8 cores, automatic SIMD | 243.9 ms | 233.7 ms | 4.2% |
| 20M + 5M | 89017 | 4 cores, AVX2, THP disabled | 720.5 ms | 604.7 ms | 16.1% |
| 20M + 5M | 11 | 4 cores, AVX2, THP disabled | 751.6 ms | 641.3 ms | 14.7% |
| 60M + 15M | 53197 | 8 cores, automatic SIMD | 882.8 ms | 783.9 ms | 11.2% |
| 60M + 15M | 53197 | 4 cores, AVX2, THP disabled | 2409.9 ms | 2005.6 ms | 16.8% |

These are warm-cache process medians on a Ryzen 7 7800X3D under WSL2.
Both binaries used the root Dockerfile's PGO build flags, eight workers and
fresh output directories. Run order alternated after one warmup per binary.
Seed 89017 used nine measured runs per configuration, seed 11 used seven,
and seed 53197 used seven on eight cores and five on four cores. The
four-core runs used CPU affinity and disabled transparent huge pages (THP)
for the child. The official runner specifies eight pinned hyperthreads and
24 GB RAM. Its CPU model and page policy are unpublished.

For seed 89017, measuring rootless OCI container launch through exit gave
330.6 → 326.5 ms over 11 runs. Seed 53197 gave 935.6 → 861.0 ms over seven
runs. These used warm input pages, a read-only input mount and root filesystem,
and a private network namespace. For a separate cache sensitivity test,
requesting eviction of the generated input pages with `POSIX_FADV_DONTNEED`
before each process run gave 548.9 → 469.5 ms on seed 89017 (seven runs).

Software write combining, wider ISA builds, an extra alias lookup cache,
input readahead and post-link profile-guided code layout were also tested.
Their elapsed-time results varied across the two CPU configurations.

The candidate passed 12 Rust tests, 36 public fixtures in three execution
modes, five targeted runs and 120 randomized comparisons with the Python
reference. Both large datasets matched every value and had zero duplicate
keys. On seed 53197 the two output tables occupied 79.75 MB, down from
129.27 MB. PyArrow also read and validated all 6,584,966 user rows and
28,014 campaign rows. Both tables summed to 18,318,225,623 cents.

### Previous measurements at `24ddb1e`

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
git archive 24ddb1e rust-pipeline scripts/build_pgo.sh third_party/generator | tar -x -C "$scratch"
sh "$scratch/scripts/build_pgo.sh"
make build-local-pgo
cargo build --release --locked --manifest-path third_party/generator/Cargo.toml
third_party/generator/target/release/generate --seed 89017 --events 20000000 --clicks 5000000 --output "$scratch/practice"
python3 scripts/benchmark.py "$scratch/practice" "$scratch/rust-pipeline/target/release/shopping-sessions" rust-pipeline/target/release/shopping-sessions --runs 9
SS_AVX2=1 taskset -c 0-3 python3 scripts/benchmark.py "$scratch/practice" "$scratch/rust-pipeline/target/release/shopping-sessions" rust-pipeline/target/release/shopping-sessions --runs 9 --disable-thp
```

## Implementation

- Parser threads read large event files into reusable buffers with `pread`.
  A read-only mapping provides stable addresses for IDs needed after parsing.
  SIMD builds an index of delimiters and wrapping quotes while checking the
  contest dialect. AVX-512 byte compression is selected on capable CPUs.
- Each 64-byte identity-table group contains four alias keys, four root
  indices and two full names that parsing workers can claim for independent
  people. AVX2 compares the four inline keys together. The root index
  addresses a partition-local event counter directly. Once both independent
  slots are occupied, further misses retain their exact names.
- A click stores its campaign, time and tie-break ID in 16 bytes. Common ASCII
  IDs preserve byte order in a 63-bit key. Other IDs refer to the original
  bytes so a timestamp tie always uses the exact ID. Canonical decimal product
  and campaign IDs use compact direct tables with an exact string fallback.
- Common event IDs fit losslessly into 48 bits. Their partition supplies the
  high bits, so a 12-byte record can hold the remainder, the ingest time and
  a payload reference. Other IDs and outlying timestamps use a full record
  in the same arena. Dedup keeps the earliest ingest even when that event
  fails validation.
- For directly indexed people, an ordinary event payload fits in eight
  bytes. Large prices and wide windows retain the full event word. When the
  dedup workers finish, large aggregation vectors reuse the retired key
  pages. Workers release the remaining record pages in parallel after all
  consumers finish.
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
  columns. A Snappy encoder emits runs of zero revenue as the rows are
  produced. Small category catalogs use dictionary indices for non-null
  values. Campaign totals use a final reduction.

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
