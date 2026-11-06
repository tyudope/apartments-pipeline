"""One place that knows how to talk to Postgres."""
from contextlib import contextmanager

import psycopg

from pipeline import config

SCHEMA_SQL = config.PROJECT_ROOT / "sql" / "schema.sql"


@contextmanager
def connect():
    with psycopg.connect(config.DATABASE_URL) as conn:
        yield conn


def ensure_schema(conn: psycopg.Connection) -> None:
    """Create tables if missing. Safe to run every time (all IF NOT EXISTS)."""
    conn.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
    conn.commit()


def log_run(conn: psycopg.Connection, month: str, step: str, passed: bool, value: str = "") -> None:
    conn.execute(
        "INSERT INTO dw.run_log (month_key, step, passed, value) VALUES (%s, %s, %s, %s)",
        (month, step, passed, value),
    )
    conn.commit()


def scalar(conn: psycopg.Connection, sql: str, params: tuple = ()):
    """Run a query that returns one value and return it."""
    return conn.execute(sql, params).fetchone()[0]