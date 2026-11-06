"""Orchestrate one month end to end: extract -> transform -> load -> checks.
Each step logs to dw.run_log. If checks fail, the month is removed from gold
(quarantined) so the API never serves it; raw and silver keep it for diagnosis.

    python -m pipeline.run --month 2023-08
    python -m pipeline.run --start 2023-08 --end 2024-06     # backfill

Scheduling: one cron line, e.g. on the 2nd of each month at 06:00:
    0 6 2 * *  cd /opt/apartments-pipeline && .venv/bin/python -m pipeline.run >> logs/run.log 2>&1
"""
import argparse
import logging
import sys
import time

from pipeline import checks, db, extract, load, transform

log = logging.getLogger("pipeline")


def quarantine(month: str) -> None:
    """Remove a month from gold after failed checks. Rejects and run_log stay."""
    with db.connect() as conn:
        n = conn.execute("DELETE FROM dw.fact_listing WHERE month_key = %s", (month,)).rowcount
        conn.commit()
        db.log_run(conn, month, "quarantine", True, f"removed {n} rows from fact_listing")
    log.warning("month %s quarantined: %d rows removed from gold", month, n)


def run_month(month: str) -> bool:
    started = time.perf_counter()
    try:
        with db.connect() as conn:
            db.ensure_schema(conn)
        for step, fn in [("extract", extract.extract), ("transform", transform.transform)]:
            result = fn(month)
            with db.connect() as conn:
                db.log_run(conn, month, step, True, str(result))
        load.load(month)
        try:
            checks.run_checks(month)
        except RuntimeError:
            quarantine(month)   # write-audit-publish, the cheap version
            raise
    except Exception as exc:
        log.error("month %s FAILED: %s", month, exc)
        with db.connect() as conn:
            db.log_run(conn, month, "run", False, str(exc)[:500])
        return False
    log.info("month %s OK in %.1fs", month, time.perf_counter() - started)
    return True


def month_range(start: str, end: str) -> list[str]:
    y, m = int(start[:4]), int(start[5:7])
    out = []
    while f"{y:04d}-{m:02d}" <= end:
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the pipeline for one month or a range.")
    parser.add_argument("--month", help="YYYY-MM")
    parser.add_argument("--start", help="YYYY-MM, backfill start")
    parser.add_argument("--end", help="YYYY-MM, backfill end (inclusive)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    months = [args.month] if args.month else month_range(args.start, args.end)
    results = {m: run_month(m) for m in months}
    failed = [m for m, ok in results.items() if not ok]
    log.info("done: %d ok, %d failed %s", len(months) - len(failed), len(failed), failed or "")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()