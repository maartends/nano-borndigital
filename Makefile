# "nano-borndigital"
SHELL:=/bin/bash

.ONESHELL:

.PHONY: make-help
make-help:
	@echo "Usage: $ make <target>"
	@echo " TODO"

.PHONY: init
init:
	@echo "[INIT]: Install pipenv..."
	pip3 install pipenv

.PHONY: install-req
install-req:
	@echo "[INSTALL-REQ]: Install requirements..."
	pipenv install

.PHONY: install-dev
install-dev:
	@echo "[INSTALL-DEV]: Install development requirements..."
	pipenv install --dev

.PHONY: install
install: install-req install-dev
	@echo "[INSTALL]: Install all requirements..."

.PHONY: lint
lint:
	@echo "[LINT]: Run Flake8 linter on code syntax"
	pipenv run flake8 --ignore E221,E251 main.py

.PHONY: tests
tests:
	@echo "[TESTS]: Running unittests..."
	pipenv run pytest -v tests

.PHONY: run
run:
	@echo "[RUN]: Run application..."
	pipenv run python main.py

.PHONY: mv-config
mv-config:
	@echo "[MV-CONFIG]: Rename 'config.yml.example' to 'config.yml'..."
	cp config.yml.example config.yml

.PHONY: all
all: init install tests
	@echo "***********************************************"
	@echo "* Succes: see README.md for more information. *"
	@echo "*         or do 'make help'                   *"
	@echo "***********************************************"

