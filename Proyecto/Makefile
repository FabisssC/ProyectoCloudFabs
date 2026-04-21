ifeq ($(OS),Windows_NT)
PYTHON := .venv/Scripts/python.exe
PIP := .venv/Scripts/pip.exe
else
PYTHON := .venv/bin/python
PIP := .venv/bin/pip
endif

.PHONY: venv install install-dev test lint

venv:
	python -m venv .venv

install: venv
	$(PIP) install -r requirements.txt

install-dev: venv
	$(PIP) install -r requirements.txt -r requirements-dev.txt

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m compileall src
