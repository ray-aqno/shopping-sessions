PYTHON ?= .venv/bin/python
BINARY := $(CURDIR)/build/shopping
GENERATOR := tools/generator/target/release/generate
DATA ?= data/dev
OUT ?= output/dev
ANSWER ?= dev-answer
IMAGE ?= shopping-sessions:latest
export NCPU ?= 8

.PHONY: build parser-check fixtures fixtures-container generator dev run-dev run-dev-container check-dev differential image
build:
	cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DPython3_EXECUTABLE=$(abspath $(PYTHON))
	cmake --build build -j8

fixtures: build
	$(PYTHON) scripts/run_fixtures.py tests/fixtures -- $(BINARY)

parser-check: build
	cmake --build build --target csv-check csv-scan-check -j8
	build/csv-check
	build/csv-scan-check

fixtures-container: image
	IMAGE=$(IMAGE) $(PYTHON) scripts/run_fixtures.py tests/fixtures -- scripts/docker_run.sh

generator:
	cargo build --release --manifest-path tools/generator/Cargo.toml

dev: generator
	$(GENERATOR) --seed 1 --events 1000000 --clicks 250000 --output $(DATA)

run-dev: build
	DATA_DIR=$(DATA) OUTPUT_DIR=$(OUT) /usr/bin/time -v $(BINARY)

run-dev-container: image
	IMAGE=$(IMAGE) scripts/docker_run.sh $(DATA) $(OUT)

check-dev:
	$(PYTHON) scripts/compare.py $(OUT) $(ANSWER)

differential: build generator
	$(PYTHON) tests/differential.py

image:
	docker build --platform linux/amd64 -t $(IMAGE) .
