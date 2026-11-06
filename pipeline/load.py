"""Load: silver Parquet -> Postgres star schema (gold).

Idempotent by design:
  - dim_month: INSERT ... ON CONFLICT DO NOTHING
  - fact_listing: COPY into a temp table, then INSERT ... ON CONFLICT (id, month_key)
    DO UPDATE. Running the same month twice changes no row counts; a corrected
    source file updates values in place.
  - rejects: delete the month, insert the month (replace-partition pattern).

    python -m pipeline.load --month 2023-08
"""
import argparse
import io
import logging

import pandas as pd
import psycopg

from pipeline import config, db
from pipeline.transform import SILVER_COLUMNS

log = logging.getLogger(__name__)

# Silver calls it "city"; the fact table calls it "city_key".
FACT_COLUMNS = [c if c != "city" else "city_key" for c in SILVER_COLUMNS]
REJECT_DB_COLUMNS = ["id", "month_key", "city_key", "reject_reason", "area_m2", "rooms", "price", "price_per_m2"]


def upsert_dim_month(conn: psycopg.Connection, month: str) -> None:
    year, mon = int(month[:4]), int(month[5:7])
    conn.execute(
        """
        INSERT INTO dw.dim_month (month_key, year, month, quarter, first_day)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (month_key) DO NOTHING
        """,
        (month, year, mon, (mon - 1) // 3 + 1, f"{month}-01"),
    )


def copy_frame(conn: psycopg.Connection, table: str, frame: pd.DataFrame, columns: list[str]) -> None:
    """Bulk-load a DataFrame with COPY, the fastest way into Postgres. NA -> NULL."""
    buf = io.StringIO()
    frame[columns].to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    cols = ", ".join(columns)
    with conn.cursor().copy(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '')") as cp:
        cp.write(buf.read())


def upsert_fact(conn: psycopg.Connection, listings: pd.DataFrame) -> tuple[int, int]:
    """COPY to a temp table, then upsert. Returns (inserted, updated)."""
    frame = listings.rename(columns={"city": "city_key"})
    month = frame["month_key"].iloc[0]

    conn.execute("CREATE TEMP TABLE stage_listing (LIKE dw.fact_listing INCLUDING DEFAULTS) ON COMMIT DROP")
    copy_frame(conn, "stage_listing", frame, FACT_COLUMNS)

    before = db.scalar(conn, "SELECT count(*) FROM dw.fact_listing WHERE month_key = %s", (month,))
    cols = ", ".join(FACT_COLUMNS)
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in FACT_COLUMNS if c not in ("id", "month_key"))
    conn.execute(
        f"""
        INSERT INTO dw.fact_listing ({cols})
        SELECT {cols} FROM stage_listing
        ON CONFLICT (id, month_key) DO UPDATE SET {set_clause}, loaded_at = now()
        """
    )
    after = db.scalar(conn, "SELECT count(*) FROM dw.fact_listing WHERE month_key = %s", (month,))
    inserted = after - before
    return inserted, len(frame) - inserted


def replace_rejects(conn: psycopg.Connection, rejects: pd.DataFrame, month: str) -> int:
    conn.execute("DELETE FROM dw.rejects WHERE month_key = %s", (month,))
    if len(rejects):
        copy_frame(conn, "dw.rejects", rejects.rename(columns={"city": "city_key"}), REJECT_DB_COLUMNS)
    return len(rejects)


def load(month: str) -> dict:
    listings = pd.read_parquet(config.SILVER_DIR / "listings" / f"month={month}")
    rejects = pd.read_parquet(config.SILVER_DIR / "rejects" / f"month={month}")

    with db.connect() as conn:
        db.ensure_schema(conn)
        with conn.transaction():  # the whole month commits or nothing does
            upsert_dim_month(conn, month)
            inserted, updated = upsert_fact(conn, listings)
            n_rejects = replace_rejects(conn, rejects, month)
        stats = {"month": month, "inserted": inserted, "updated": updated, "rejects": n_rejects}
        db.log_run(conn, month, "load", True, str(stats))

    log.info("load %s: %s", month, stats)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Load one month of silver Parquet into Postgres.")
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    print(load(args.month))


if __name__ == "__main__":
    main()