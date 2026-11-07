# Built from the repo root (see docker-compose.yml) so the uv workspace (root pyproject.toml,
# uv.lock, sdk/python, scraper) is in the build context. PACKAGE picks what to install:
# weir-demo for the pipeline services, weir-scraper for the scraper.
FROM python:3.12-slim
ARG PACKAGE=weir-demo

COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/venv \
    PATH="/venv/bin:$PATH"

WORKDIR /src
# Dependencies first, so editing the services doesn't reinstall them.
COPY pyproject.toml uv.lock ./
COPY demo/pyproject.toml demo/
COPY sdk/python sdk/python
COPY scraper scraper
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --package ${PACKAGE}

WORKDIR /app
COPY demo/services/ .
