SHELL := /bin/bash
.DEFAULT_GOAL := help

# Spark 3.5 does not support the JDK 25/26 that Fedora ships as system Java, so we
# pin a project-local Temurin 17. `make setup` installs it if it is not already there.
JDK_HOME  ?= $(HOME)/.local/jdks/jdk-17
export JAVA_HOME := $(JDK_HOME)

ENV       ?= local
SCALE     ?= 1.0
SEED      ?= 42
DATE      ?= $(shell date +%F)
UV        := uv

export MEDCHAIN_ENV := $(ENV)

.PHONY: help
help:  ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: quickstart
quickstart:  ## Fetch Azure's Gold output, build and serve the dashboard (no rebuild)
	@python3 quickstart.py

# ---------------------------------------------------------------- environment

.PHONY: jdk
jdk:  ## Install a project-local Temurin JDK 17 (no sudo, system JDK untouched)
	@if [ -x "$(JDK_HOME)/bin/java" ]; then \
		echo "JDK 17 present: $$($(JDK_HOME)/bin/java -version 2>&1 | head -1)"; \
	else \
		echo "Installing Temurin JDK 17 into $(JDK_HOME)..."; \
		mkdir -p $(JDK_HOME); \
		curl -sSL "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse?project=jdk" \
			| tar xz -C $(JDK_HOME) --strip-components=1; \
		$(JDK_HOME)/bin/java -version; \
	fi

.PHONY: setup
setup: jdk  ## Create the venv and install all dependency groups
	$(UV) venv --python 3.11
	$(UV) pip install -e '.[local,generate,web,dev]'
	@echo "Setup complete. JAVA_HOME=$(JAVA_HOME)"

.PHONY: doctor
doctor:  ## Verify the toolchain can actually start Spark + Delta
	@$(UV) run python -c "import sys; print('python', sys.version.split()[0])"
	@$(JDK_HOME)/bin/java -version 2>&1 | head -1
	@$(UV) run python -m medchain.doctor

# ---------------------------------------------------------------- data + runs

.PHONY: gen
gen:  ## Generate synthetic source data (SCALE=1.0 full, 0.01 for tests)
	$(UV) run medchain-gen --scale $(SCALE) --seed $(SEED)

.PHONY: run-bronze run-silver run-gold run-quality run-local
run-bronze:  ## Land source files into the Bronze layer
	$(UV) run medchain-run bronze --date $(DATE)

run-silver:  ## Build all Silver tables
	$(UV) run medchain-run silver --date $(DATE)

run-gold:  ## Build the Gold star schema
	$(UV) run medchain-run gold --date $(DATE)

run-quality:  ## Evaluate the data quality scorecard
	$(UV) run medchain-run quality --date $(DATE)

run-local:  ## Full pipeline: bronze -> silver -> gold -> quality
	$(UV) run medchain-run all --date $(DATE)

# ---------------------------------------------------------------- dashboard
WEB := dashboards/web

# The build copies public/data into dist, so dist is a snapshot taken at build
# time. Fetching new data without rebuilding leaves the served page showing the old
# numbers — which the footer's provenance line will tell you, but the targets below
# are ordered so it does not arise.
#
# web-build deliberately does NOT depend on web-data. It used to, which meant
# `make web` after `make web-data-azure` quietly re-ran a local export over the top
# of the Azure data and served that instead. Choosing the data source is the point
# of these targets, so it is never implied.
.PHONY: web-data web-data-azure web-install web-dev web-build web-preview web web-azure
web-data:  ## Export the local Gold layer to JSON (overwrites public/data)
	$(UV) run medchain-web-export

web-data-azure:  ## Fetch the dashboard data the cluster produced (overwrites public/data)
	./infra/fetch_web_data.sh

web-install:  ## Install frontend dependencies
	cd $(WEB) && npm install

web-dev:  ## Dev server with hot reload, on whatever data is present (localhost:5173)
	cd $(WEB) && npm run dev

web-build:  ## Production build into dist, from whatever data is present
	cd $(WEB) && npm run build

web-preview:  ## Serve the production build (localhost:4173)
	cd $(WEB) && npm run preview

# Sub-makes rather than prerequisites: prerequisite order is not guaranteed under
# `make -j`, and building before the data lands is exactly the bug being avoided.
web:  ## Local Gold -> build -> serve
	@$(MAKE) web-data
	@$(MAKE) web-build
	@$(MAKE) web-preview

web-azure:  ## Azure Gold -> build -> serve
	@$(MAKE) web-data-azure
	@$(MAKE) web-build
	@$(MAKE) web-preview

.PHONY: questions
questions:  ## Print the 7 business questions with their current answers
	$(UV) run python -m medchain.gold.report

# ---------------------------------------------------------------- quality

.PHONY: test test-unit test-spark test-integration lint fmt typecheck check
test-unit:  ## Fast pure-Python tests (no Spark)
	$(UV) run pytest tests/unit -q

test-spark:  ## Spark tests against a local session
	$(UV) run pytest tests/spark -q

test-integration:  ## Full generator -> bronze -> silver -> gold run
	$(UV) run pytest tests/integration -q

test:  ## Unit + Spark tests
	$(UV) run pytest tests/unit tests/spark -q

lint:  ## Ruff lint
	$(UV) run ruff check src tests dashboards

fmt:  ## Ruff format + autofix
	$(UV) run ruff format src tests dashboards
	$(UV) run ruff check --fix src tests dashboards

typecheck:  ## mypy
	$(UV) run mypy src

check: lint typecheck test  ## Everything CI runs

# ---------------------------------------------------------------- azure

.PHONY: provision upload deploy stop teardown cost
provision:  ## Create Azure resources (SPENDS CREDIT - requires az login)
	./infra/provision.sh

upload:  ## Push generated data to the ADLS landing container
	./infra/upload.sh

deploy:  ## Build the wheel and deploy notebooks + ADF pipelines
	./infra/deploy.sh

run-azure:  ## Create/update the Databricks job and trigger a pipeline run
	./infra/run_pipeline.sh

stop:  ## Terminate every running Databricks cluster
	./infra/stop_clusters.sh

cost:  ## Report month-to-date Azure spend
	./infra/cost.sh

teardown:  ## Delete the entire resource group
	./infra/teardown.sh

# ---------------------------------------------------------------- misc

.PHONY: clean
clean:  ## Remove generated data and build artefacts
	rm -rf data/bronze data/silver data/gold data/control data/quarantine \
	       data/checkpoints data/_warehouse build dist *.egg-info \
	       .pytest_cache .ruff_cache .mypy_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

.PHONY: clean-all
clean-all: clean  ## Also remove generated source data
	rm -rf data/landing data/_truth
