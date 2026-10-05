# Everyday commands. `make help` lists them.
#
# Terraform targets take ENV (default prod); see infra/envs/README.md.
# Applying always needs a reviewed plan file from `make plan`.

PY      ?= .venv/bin/python
ENV     ?= prod
TF      := terraform -chdir=infra
BACKEND := $(if $(filter prod,$(ENV)),backend.hcl,envs/$(ENV).backend.hcl)
VARFILE := $(if $(filter prod,$(ENV)),,-var-file=envs/$(ENV).tfvars)

.PHONY: help install lint typecheck test coverage golden audit security check \
        fmt validate init plan apply smoke

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

install: ## Create .venv and install the package with dev tools
	python3 -m venv .venv
	$(PY) -m pip install -e ".[dev]"

lint: ## ruff
	$(PY) -m ruff check src tests scripts

typecheck: ## mypy
	$(PY) -m mypy

test: ## All tests (moto; no AWS needed)
	$(PY) -m pytest -q

coverage: ## Tests with a coverage report (fails below the floor)
	$(PY) -m pytest -q --cov --cov-report=term-missing

golden: ## Regenerate tests/golden snapshots after an intended output change
	UPDATE_GOLDEN=1 $(PY) -m pytest -q tests/test_golden.py

audit: ## Known vulnerabilities in installed dependencies
	$(PY) -m pip_audit --skip-editable

security: ## checkov scan of infra/ (needs `pip install checkov`)
	checkov --config-file .checkov.yaml --directory infra

check: lint typecheck test validate ## Everything CI runs that needs no network

fmt: ## terraform fmt
	terraform fmt -recursive infra

validate: ## terraform fmt check + validate
	terraform fmt -check -recursive infra
	$(TF) validate

init: ## Point Terraform at ENV's state (re-run when switching ENV)
	$(TF) init -reconfigure -backend-config=$(BACKEND)

plan: ## Plan for ENV into infra/tfplan (refuses if init points elsewhere)
	@$(PY) scripts/check_tf_backend.py $(ENV) $(BACKEND)
	$(TF) plan -out=tfplan $(VARFILE)

apply: ## Apply the plan file from `make plan` (nothing else)
	$(TF) apply tfplan

smoke: ## End-to-end test of the deployed stack (uploads real files)
	$(PY) scripts/smoke_test.py
