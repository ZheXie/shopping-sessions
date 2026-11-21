# Shopping Sessions: Rust pipeline (rust/), one static binary, no crates.
# rust/.cargo/config.toml sets the scorer's CPU baseline (x86-64-v3: AVX2,
# BMI2; the Cascade Lake scorers support it) and static linking.
# The C++ version of the same pipeline builds with Dockerfile.cpp.
FROM rust:1.83-slim-bookworm AS build
WORKDIR /src
COPY rust/ .
# Same flags as rust/.cargo/config.toml, set here too so the build never
# depends on that file: AVX2 baseline and a static binary for scratch.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
RUN cargo build --release --target x86_64-unknown-linux-gnu  && cp target/x86_64-unknown-linux-gnu/release/ss /ss

FROM scratch
COPY --from=build /ss /ss
# Runs as uid 1000 with a read-only root; reads $DATA_DIR, writes $OUTPUT_DIR.
CMD ["/ss"]
