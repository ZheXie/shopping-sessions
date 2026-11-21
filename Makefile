IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
PLATFORM ?= linux/amd64
DEV   ?= dev
FIXTURES ?= $(if $(wildcard fixtures),fixtures,../../fixtures)
BIN ?= rust-pipeline/target/release/shopping-sessions
export IMAGE CPUS MEM PLATFORM

.PHONY: build build-local build-local-pgo fixtures dev run-dev check-dev

## Build the scorer image (linux/amd64)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Release binary for local loops (flags in rust-pipeline/.cargo/config.toml)
build-local:
	cd rust-pipeline && cargo build --release

## Match the scorer image's profile-guided build (needs llvm-tools-preview)
build-local-pgo:
	sh scripts/build_pgo.sh

## Run all 36 fixtures against the local binary
fixtures: build-local
	python3 scripts/run_fixtures.py $(FIXTURES) -- $(BIN)

## Generate the 1M-event dev set
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Timed run on the local binary
run-dev: build-local
	rm -rf out && mkdir -p out scratch
	NCPU=$(CPUS) DATA_DIR=$(DEV) OUTPUT_DIR=out SCRATCH_DIR=scratch time $(BIN)

## Exact diff against published dev-answer
ANSWER ?= dev-answer
check-dev:
	PYTHONPATH=scripts python3 scripts/compare.py out $(ANSWER)

## Docker run on fixtures (scorer-shaped)
fixtures-docker: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh
