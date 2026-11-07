FAULT  ?= none
SETTLE ?= 15 minutes
RANGE  ?= 15m

# make bench
MESSAGES ?= 300000
RUNS     ?= 3
WORK_US  ?= 0

DC := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo docker-compose)
COMPOSE := FAULT_ENV=faults/$(FAULT).env $(DC)

.PHONY: up down reset ps logs truth baseline score psql ui bench

up:            ## Start the pipeline:  make up FAULT=drop
	@test -f faults/$(FAULT).env || (echo "unknown FAULT=$(FAULT); see faults/" && exit 1)
	$(COMPOSE) up -d --build

down:          ## Stop, keep data
	$(COMPOSE) down

reset:         ## Stop and wipe all data (Kafka, Postgres)
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=50

truth:         ## Ground truth: loss, attribution, latency decomposition
	$(COMPOSE) exec -T postgres psql -U weir -d weir -v "settle=$(SETTLE)" -f /sql/truth.sql

baseline:      ## What standard tools show: Kafka lag, Celery queue length
	DC="$(DC)" ./scripts/baseline.sh

score:         ## Truth, standard tools and Weir side by side:  make score RANGE=15m
	@$(MAKE) --no-print-directory truth
	@echo; echo "======== Standard tools ========"
	@$(MAKE) --no-print-directory baseline
	@RANGE="$(RANGE)" ./scripts/score.sh

psql:
	$(COMPOSE) exec postgres psql -U weir -d weir

bench:         ## SDK overhead on a Kafka consumer (stop the demo first):  make bench WORK_US=100
	$(COMPOSE) --profile bench build bench
	$(COMPOSE) --profile bench run --rm bench python /bench/kafka_bench.py \
		--messages $(MESSAGES) --runs $(RUNS) --work-us $(WORK_US)

ui:            ## Open the Weir UI
	open http://localhost:4100
