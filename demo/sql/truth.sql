-- Ground-truth report. Run with:  make truth            (default settle window 15 minutes)
--                                 make truth SETTLE='3 minutes'
-- Orders younger than the settle window are counted as in-flight, not lost.

\pset footer off

CREATE TEMP VIEW expected AS
SELECT p.order_id,
       p.order_id || '-' || g AS line_key,
       p.event_time
FROM truth.produced p
CROSS JOIN LATERAL generate_series(0, p.n_lines - 1) g;

CREATE TEMP VIEW missing AS
SELECT e.*
FROM expected e
LEFT JOIN app.line_items s USING (line_key)
WHERE s.line_key IS NULL
  AND e.event_time < now() - interval :'settle';

\echo
\echo '== Conservation (orders older than the settle window) =='
SELECT (SELECT count(*) FROM expected WHERE event_time < now() - interval :'settle') AS expected_lines,
       (SELECT count(*) FROM missing)                                                AS missing_lines,
       round(100.0 * (SELECT count(*) FROM missing)
             / nullif((SELECT count(*) FROM expected WHERE event_time < now() - interval :'settle'), 0), 3) AS missing_pct,
       (SELECT count(*) FROM expected e LEFT JOIN app.line_items s USING (line_key)
         WHERE s.line_key IS NULL AND e.event_time >= now() - interval :'settle')     AS in_flight_lines;

\echo '== Why it is missing (joined to injected faults) =='
SELECT coalesce(i.stage || ':' || i.fault, 'UNEXPLAINED') AS cause,
       count(*)                   AS lines,
       count(DISTINCT m.order_id) AS orders
FROM missing m
LEFT JOIN (SELECT DISTINCT key, stage, fault FROM truth.injected WHERE fault <> 'duplicate') i
       ON i.key = m.order_id
GROUP BY 1
ORDER BY 2 DESC;

\echo '== Latency decomposition, delivered in the last settle window (seconds) =='
SELECT count(*) AS delivered_lines,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM staged_at - event_time))::numeric, 1) AS to_staging_p50,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM staged_at - event_time))::numeric, 1) AS to_staging_p99,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM sink_at - staged_at))::numeric, 1)   AS schedule_wait_p50,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM sink_at - staged_at))::numeric, 1)   AS schedule_wait_p99,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM sink_at - event_time))::numeric, 1)  AS end_to_end_p50,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM sink_at - event_time))::numeric, 1)  AS end_to_end_p99
FROM app.line_items
WHERE sink_at > now() - interval :'settle';

\echo '== End-to-end age by event minute (last 30 minutes) =='
SELECT date_trunc('minute', event_time) AS event_minute,
       count(*) AS lines,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM staged_at - event_time))::numeric, 1) AS to_staging_p99_s,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM sink_at - event_time))::numeric, 1)  AS end_to_end_p99_s
FROM app.line_items
WHERE event_time > now() - interval '30 minutes'
GROUP BY 1
ORDER BY 1;

\echo '== Injected faults =='
SELECT stage, fault, count(*) AS events, count(DISTINCT key) AS orders, min(at) AS first_at, max(at) AS last_at
FROM truth.injected
GROUP BY 1, 2
ORDER BY 1, 2;
