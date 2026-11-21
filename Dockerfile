# Shopping Sessions: Rust pipeline (rust/), one static binary, no crates.
# rust/.cargo/config.toml sets the scorer's CPU baseline (x86-64-v3: AVX2,
# BMI2; the Cascade Lake scorers support it) and static linking.
# The C++ version of the same pipeline builds with Dockerfile.cpp.
FROM rust:1.83-slim-bookworm AS build
WORKDIR /src
COPY rust/ .
RUN cargo build --release && cp target/release/ss /ss

FROM scratch
COPY --from=build /ss /ss
# Runs as uid 1000 with a read-only root; reads $DATA_DIR, writes $OUTPUT_DIR.
CMD ["/ss"]
