IMAGE ?= shopping-sessions:latest
CXXFLAGS ?= -O3 -march=x86-64-v3 -std=c++17 -pthread
BIN ?= build/ss
DEV ?= dev
ANSWER ?= dev-answer

.PHONY: bin fixtures check-dev fuzz build run-dev

## Native build (Linux, g++ 11+)
bin:
	mkdir -p build && g++ $(CXXFLAGS) -o $(BIN) src/ss.cpp

## All 36 edge-case fixtures (needs polars for the checker)
fixtures: bin
	python3 scripts/run_fixtures.py fixtures -- $(BIN)

## Run on the 1M dev set and diff with the published answer
check-dev: bin
	rm -rf out && $(BIN) $(DEV) out && python3 scripts/compare.py out $(ANSWER)

## Differential fuzz against the slow reference (needs polars, pyarrow)
fuzz: bin
	COMPARE=scripts/compare.py python3 reference/fuzz.py $(BIN) 500

## Docker image exactly as the scorer builds it
build:
	docker buildx build --platform linux/amd64 --load -t $(IMAGE) .

run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out
