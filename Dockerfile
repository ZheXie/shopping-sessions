# Shopping Sessions — Rust pipeline (scorers are linux/amd64 and build natively).
FROM rust:1-bookworm AS builder
WORKDIR /src
ENV CARGO_TERM_COLOR=never \
    RUSTFLAGS="-C target-cpu=x86-64-v3"
COPY rust-pipeline/ ./
RUN cargo build --release && strip -s target/release/shopping-sessions

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /src/target/release/shopping-sessions /app/shopping-sessions
WORKDIR /app
# The binary reads DATA_DIR / OUTPUT_DIR / NCPU itself; no shell in between.
CMD ["/app/shopping-sessions"]
