# Shopping Sessions — Rust pipeline (scorers are linux/amd64 and build natively).
FROM rust:1-bookworm AS builder
WORKDIR /src
ENV CARGO_TERM_COLOR=never \
    RUSTFLAGS="-C target-cpu=x86-64-v3"
COPY rust-pipeline/ rust-pipeline/
COPY third_party/generator/ third_party/generator/
COPY scripts/build_pgo.sh scripts/build_pgo.sh
RUN rustup component add llvm-tools-preview \
 && sh scripts/build_pgo.sh \
 && strip -s rust-pipeline/target/release/shopping-sessions

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /src/rust-pipeline/target/release/shopping-sessions /app/shopping-sessions
WORKDIR /app
# The binary reads DATA_DIR / OUTPUT_DIR / NCPU itself; no shell in between.
CMD ["/app/shopping-sessions"]
