-- truth.*  : ground truth, written by the demo itself. Weir never reads these;
--            they exist to score Weir's answers.
-- app.*    : the pipeline's real tables.

CREATE SCHEMA truth;
CREATE SCHEMA app;

-- Every order the producer intended to send. Expected line keys are
-- order_id || '-' || 0..n_lines-1.
CREATE TABLE truth.produced (
    seq        bigint PRIMARY KEY,
    order_id   text        NOT NULL UNIQUE,
    n_lines    int         NOT NULL,
    event_time timestamptz NOT NULL
);

-- Every fault the demo injected, so "what went missing" can be explained.
CREATE TABLE truth.injected (
    id     bigserial PRIMARY KEY,
    key    text        NOT NULL,           -- order_id
    stage  text        NOT NULL,
    fault  text        NOT NULL,           -- drop | duplicate | poison | stuck_behind_poison
    detail text,
    at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON truth.injected (key);

-- Written by the Celery worker, drained by the aggregate cron.
CREATE TABLE app.staged_lines (
    line_key   text          NOT NULL,
    order_id   text          NOT NULL,
    event_time timestamptz   NOT NULL,
    amount     numeric(10,2) NOT NULL,
    batch_id   text          NOT NULL,
    staged_at  timestamptz   NOT NULL DEFAULT now()
);
-- Lets the Weir scraper find the oldest waiting row by reading one index entry (demo/scraper.toml).
CREATE INDEX staged_lines_event_time ON app.staged_lines (event_time);

-- Final sink. Primary key dedupes duplicate deliveries.
CREATE TABLE app.line_items (
    line_key   text PRIMARY KEY,
    order_id   text          NOT NULL,
    event_time timestamptz   NOT NULL,
    amount     numeric(10,2) NOT NULL,
    batch_id   text          NOT NULL,
    staged_at  timestamptz   NOT NULL,
    sink_at    timestamptz   NOT NULL DEFAULT now()
);
CREATE INDEX ON app.line_items (sink_at);
