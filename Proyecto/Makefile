ifeq ($(OS),Windows_NT)
PYTHON := .venv/Scripts/python.exe
PIP := .venv/Scripts/pip.exe
else
PYTHON := .venv/bin/python
PIP := .venv/bin/pip
endif

PARTITIONS ?=

.PHONY: venv install install-dev run-bronze run-silver run-gold-full run-gold-incremental run-gold-backfill publish-gcp test lint ci

venv:
	python -m venv .venv

install: venv
	$(PIP) install -r requirements.txt

install-dev: venv
	$(PIP) install -r requirements.txt -r requirements-dev.txt

run-bronze:
	$(PYTHON) -m src.pipeline.orchestration.run_local_ingestion

run-silver:
	$(PYTHON) -m src.pipeline.transform.build_silver

run-gold-full:
	$(PYTHON) -m src.pipeline.gold.build_gold --mode full

run-gold-incremental:
	$(PYTHON) -m src.pipeline.gold.build_gold --mode incremental

run-gold-backfill:
	$(PYTHON) -m src.pipeline.gold.build_gold --mode backfill --partitions $(PARTITIONS)

publish-gcp:
	$(PYTHON) -m src.pipeline.orchestration.publish_gcp_reference

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m compileall src

ci:
	$(PYTHON) -m compileall src
	$(PYTHON) -m pytest -q
