# Shopping Sessions — Rust pipeline (scorers are linux/amd64 and build natively).
FROM rust:1-bookworm AS builder
WORKDIR /src
ENV CARGO_TERM_COLOR=never \
    RUSTFLAGS="-C target-cpu=native"
COPY rust-pipeline/ ./
RUN cargo build --release && strip -s target/release/shopping-sessions

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /src/target/release/shopping-sessions /app/shopping-sessions
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/shopping-sessions
WORKDIR /app
CMD ["/app/run.sh"]
