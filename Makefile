# Repo-wide checks. `make ci` runs exactly what GitHub Actions runs (.github/workflows/ci.yml).
# Any other target is forwarded to demo/Makefile, so `make up FAULT=quick` works from the repo root.
.DEFAULT_GOAL := up

# Pinned: new ruff releases enable new rules by default, which would fail CI with no code change.
RUFF := uvx ruff@0.17.4

%:
	@$(MAKE) --no-print-directory -C demo $@

.PHONY: ci lint test test-sdk test-scraper bench-sdk check-conventions

ci: lint test test-sdk test-scraper bench-sdk check-conventions  ## Everything CI checks

lint:          ## Go: gofmt + vet. Python: ruff lint + format check
	@cd server && test -z "$$(gofmt -l .)" || { gofmt -l .; echo "fix with: gofmt -w server"; exit 1; }
	cd server && go vet ./...
	$(RUFF) check .
	$(RUFF) format --check .

test:          ## Server tests, with the race detector
	cd server && go test -race ./...

test-sdk:      ## Python SDK tests
	uv run --package weir-sdk pytest sdk/python/tests

test-scraper:  ## Scraper tests
	uv run --package weir-scraper pytest scraper/tests

bench-sdk:     ## SDK cost per message, no broker or network. Fails above MAX_US microseconds
	uv run --package weir-sdk python -I sdk/python/bench/micro.py --max-us $(or $(MAX_US),25)

check-conventions:  ## Validate conventions/ with OpenTelemetry Weaver
	docker run --rm --mount type=bind,source=$(CURDIR)/conventions,target=/home/weaver/source,readonly \
		otel/weaver:v0.28.0 registry check --registry=/home/weaver/source
