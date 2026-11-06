"""Quality checks on the gold layer for one month. Each check writes a row to
dw.run_log; any failure raises, which stops the pipeline. Bad data never
reaches the API silently.

    python -m pipeline.checks --month 2023-08
"""
import argparse
import logging

from pipeline import db

log = logging.getLogger(__name__)

MAX_REJECT_RATE = 0.10        # above this the source probably changed, not the market
MAX_MONTH_OVER_MONTH = 0.50   # row count may not swing more than 50% vs previous month


def check_rows_present(conn, month):
    n = db.scalar(conn, "SELECT count(*) FROM dw.fact_listing WHERE month_key = %s", (month,))
    return n > 0, f"rows={n}"


def check_no_null_keys(conn, month):
    n = db.scalar(conn, """
        SELECT count(*) FROM dw.fact_listing
        WHERE month_key = %s AND (id IS NULL OR city_key IS NULL OR price IS NULL OR area_m2 IS NULL)
    """, (month,))
    return n == 0, f"null_key_rows={n}"


def check_unique_key(conn, month):
    n = db.scalar(conn, "SELECT count(*) - count(DISTINCT id) FROM dw.fact_listing WHERE month_key = %s", (month,))
    return n == 0, f"duplicate_ids={n}"


def check_no_orphan_cities(conn, month):
    n = db.scalar(conn, """
        SELECT count(*) FROM dw.fact_listing f
        LEFT JOIN dw.dim_city c ON c.city_key = f.city_key
        WHERE f.month_key = %s AND c.city_key IS NULL
    """, (month,))
    return n == 0, f"orphan_rows={n}"


def check_reject_rate(conn, month):
    good = db.scalar(conn, "SELECT count(*) FROM dw.fact_listing WHERE month_key = %s", (month,))
    bad = db.scalar(conn, "SELECT count(*) FROM dw.rejects WHERE month_key = %s", (month,))
    rate = bad / (good + bad) if (good + bad) else 1.0
    return rate <= MAX_REJECT_RATE, f"reject_rate={rate:.4f}"


def check_volume_vs_previous_month(conn, month):
    n, prev = conn.execute("""
        WITH counts AS (SELECT month_key, count(*) AS n FROM dw.fact_listing GROUP BY month_key)
        SELECT n, LAG(n) OVER (ORDER BY month_key) AS prev_n
        FROM counts WHERE month_key <= %s ORDER BY month_key DESC LIMIT 1
    """, (month,)).fetchone()
    if prev is None:
        return True, f"rows={n} prev=none"
    change = abs(n - prev) / prev
    return change <= MAX_MONTH_OVER_MONTH, f"rows={n} prev={prev} change={change:.2%}"


CHECKS = [
    check_rows_present,
    check_no_null_keys,
    check_unique_key,
    check_no_orphan_cities,
    check_reject_rate,
    check_volume_vs_previous_month,
]


def run_checks(month: str) -> dict:
    results, failed = {}, []
    with db.connect() as conn:
        for fn in CHECKS:
            name = fn.__name__.removeprefix("check_")
            passed, value = fn(conn, month)
            results[name] = (passed, value)
            db.log_run(conn, month, f"check:{name}", passed, value)
            (log.info if passed else log.error)("check %s %s: %s %s", month, name, "PASS" if passed else "FAIL", value)
            if not passed:
                failed.append(name)
    if failed:
        raise RuntimeError(f"quality checks failed for {month}: {failed}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run quality checks on one loaded month.")
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run_checks(args.month)


if __name__ == "__main__":
    main()