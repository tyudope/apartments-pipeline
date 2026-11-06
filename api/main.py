"""Read-only API over the gold layer. Each endpoint is one SQL file in
sql/questions/, so the questions live in SQL (where analysts can read them)
and the API is a thin, typed wrapper.

    uvicorn api.main:app --reload
    GET /health            last result of every pipeline step and check
    GET /months            which months are loaded
    GET /prices?city=warszawa
    GET /premiums?city=warszawa
    GET /distance?city=warszawa
"""
import psycopg
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row

from pipeline import config

QUESTIONS = config.PROJECT_ROOT / "sql" / "questions"
SQL = {
    "prices": (QUESTIONS / "01_price_per_m2_by_city_month.sql").read_text(encoding="utf-8"),
    "premiums": (QUESTIONS / "02_feature_premiums.sql").read_text(encoding="utf-8"),
    "distance": (QUESTIONS / "03_price_by_distance_band.sql").read_text(encoding="utf-8"),
}

app = FastAPI(
    title="Apartment Prices in Poland",
    version="0.1.0",
    description="Monthly listing prices per m² by city, served from a Postgres star schema.",
)


def query(sql: str, params: dict | None = None) -> list[dict]:
    try:
        with psycopg.connect(config.DATABASE_URL, row_factory=dict_row) as conn:
            return conn.execute(sql, params or {}).fetchall()
    except psycopg.OperationalError as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc


def validate_city(city: str | None) -> str | None:
    if city is None:
        return None
    known = {r["city_key"] for r in query("SELECT city_key FROM dw.dim_city")}
    if city not in known:
        raise HTTPException(status_code=404, detail=f"unknown city '{city}'; known: {sorted(known)}")
    return city


@app.get("/health")
def health():
    """Healthy only if the latest run of every check passed."""
    rows = query("""
        SELECT DISTINCT ON (step) step, month_key, passed, value, ran_at
        FROM dw.run_log ORDER BY step, ran_at DESC
    """)
    checks_ok = all(r["passed"] for r in rows if r["step"].startswith("check:"))
    return {"status": "ok" if rows and checks_ok else "degraded", "last_runs": rows}


@app.get("/months")
def months():
    return query("SELECT month_key, count(*) AS listings FROM dw.fact_listing GROUP BY month_key ORDER BY month_key")


@app.get("/prices")
def prices(city: str | None = Query(None, description="city_key, e.g. warszawa; omit for all cities")):
    return query(SQL["prices"], {"city": validate_city(city)})


@app.get("/premiums")
def premiums(city: str | None = Query(None)):
    return query(SQL["premiums"], {"city": validate_city(city)})


@app.get("/distance")
def distance(city: str | None = Query(None)):
    return query(SQL["distance"], {"city": validate_city(city)})