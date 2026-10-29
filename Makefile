# One command each: make build | test | bench | demo. Docker equivalents: make docker-test etc.
SHELL := /bin/bash
MVN ?= mvn
UV ?= uv
# Stamp file for the synced Python env; the interpreter itself may be a symlink to a system Python.
PYENV := py/.venv/.synced
JAR := target/ledgertable.jar
export LEDGER_S3_ENDPOINT ?= http://127.0.0.1:9000

.PHONY: build test test-java test-py bench demo minio clean docker-build docker-test docker-bench docker-demo

build: $(PYENV)
	$(MVN) -q package -DskipTests

$(PYENV): py/pyproject.toml py/uv.lock
	cd py && $(UV) sync --frozen -q
	@touch $(PYENV)

minio:
	scripts/minio.sh

test: build minio test-java test-py

test-java:
	$(MVN) -q test

# LEDGER_SEEDS=1-100 make test-py runs more randomized seeds.
test-py:
	cd py && .venv/bin/python -m pytest -q

bench: build minio
	java -jar $(JAR) bench --store local --out bench/results
	java -jar $(JAR) bench --store s3 --out bench/results
	scripts/machine.sh > bench/results/machine.txt
	cd py && .venv/bin/python -m ledger.plots --results ../bench/results --plots ../bench/plots

demo: build
	cd py && .venv/bin/python -m ledger.demo --work ../work/demo

clean:
	$(MVN) -q clean
	rm -rf work

docker-build:
	docker compose build

docker-test docker-bench docker-demo: docker-build
	docker compose run --rm ledger make $(@:docker-%=%)
