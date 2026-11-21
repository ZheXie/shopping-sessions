#!/bin/sh
# Build the scored binary using counters collected on public generated input.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
sysroot=$(rustc --print sysroot)
host=$(rustc -vV | sed -n 's/^host: //p')
profdata=${LLVM_PROFDATA:-"$sysroot/lib/rustlib/$host/bin/llvm-profdata"}
if [ ! -x "$profdata" ]; then
    echo "Install rustup component llvm-tools-preview, or set LLVM_PROFDATA." >&2
    exit 1
fi
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
flags=${RUSTFLAGS:--C target-cpu=x86-64-v3}
cd "$repo/rust-pipeline"
cargo build --locked --release --manifest-path "$repo/third_party/generator/Cargo.toml" --target-dir "$scratch/generator"
"$scratch/generator/release/generate" --seed 1 --events 1000000 --clicks 250000 --output "$scratch/input"
RUSTFLAGS="$flags -Cprofile-generate=$scratch/profiles" cargo build --locked --release --target-dir "$scratch/instrumented"
DATA_DIR="$scratch/input" OUTPUT_DIR="$scratch/tables" NCPU=4 \
    LLVM_PROFILE_FILE="$scratch/profiles/native-%p.profraw" "$scratch/instrumented/release/shopping-sessions"
DATA_DIR="$scratch/input" OUTPUT_DIR="$scratch/tables-avx2" NCPU=1 SS_AVX2=1 \
    LLVM_PROFILE_FILE="$scratch/profiles/avx2-%p.profraw" "$scratch/instrumented/release/shopping-sessions"
LD_LIBRARY_PATH="$sysroot/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" "$profdata" merge -o "$scratch/train.profdata" "$scratch/profiles"
RUSTFLAGS="$flags -Cprofile-use=$scratch/train.profdata" cargo build --locked --release
