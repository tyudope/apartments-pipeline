-- 1. what is loaded
SELECT month_key, count(*) AS listings FROM dw.fact_listing GROUP BY 1 ORDER BY 1;
-- 2. rejects and why
SELECT month_key, reject_reason, count(*) FROM dw.rejects GROUP BY 1, 2 ORDER BY 1;
-- 3. idempotency: run a month twice, this does not change
SELECT count(*) FROM dw.fact_listing WHERE month_key = '2024-06';
-- 4. the quality gate
SELECT month_key, step, passed, value FROM dw.run_log ORDER BY ran_at DESC LIMIT 10;
-- 5. flats seen in more than one month (why the grain is id+month)
SELECT months_seen, count(*) AS flats
FROM (SELECT id, count(*) AS months_seen FROM dw.fact_listing GROUP BY id) t
GROUP BY 1 ORDER BY 1;