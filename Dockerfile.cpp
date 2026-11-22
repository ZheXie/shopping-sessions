# Shopping Sessions: C++ pipeline, one static binary.
# Build stage compiles with the scorer's CPU baseline (x86-64-v3: AVX2, BMI2;
# the Cascade Lake scorers support it). The run image holds only the binary.
FROM debian:bookworm-slim AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ libc6-dev \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY src/ss.cpp .
RUN g++ -O3 -march=x86-64-v3 -std=c++17 -pthread -static -o /ss ss.cpp

FROM scratch
COPY --from=build /ss /ss
# Runs as uid 1000 with a read-only root; reads $DATA_DIR, writes $OUTPUT_DIR.
CMD ["/ss"]
