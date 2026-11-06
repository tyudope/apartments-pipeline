# Apartment Prices in Poland — monthly batch pipeline

A small, production-shaped data pipeline. Every month it takes the newest file of apartment listings for 15 Polish cities (Kaggle, *Apartment Prices in Poland*), lands it untouched, cleans and types it, loads it into a Postgres star schema, runs quality checks, and serves three questions through a FastAPI:

- What is the median price per m² in each city, month by month, and how is it moving?
- How much more do flats with an elevator, a balcony, or parking cost per m²?
- How does price per m² fall with distance from the city centre?

Stack: Python 3.11, Pandas, Parquet, PostgreSQL 16 (Docker), FastAPI, pytest, ruff, GitHub Actions.

## Architecture

```
source CSV (monthly)
   │  extract.py      copy as-is, checksum, skip if present
   ▼
raw          data/raw/month=YYYY-MM/apartments.csv
   │  transform.py    schema guard · normalize · derive · route bad rows to rejects
   ▼
silver       data/silver/{listings,rejects}/month=YYYY-MM/   Parquet, partition overwritten
   │  load.py         COPY → temp table → INSERT … ON CONFLICT (id, month_key) DO UPDATE
   ▼
gold         Postgres schema dw: dim_city · dim_month · fact_listing · rejects · run_log · v_listing_latest
   │  checks.py       6 checks → run_log; a failed month is quarantined out of gold
   ▼
api/main.py  /prices  /premiums  /distance  /months  /health
```

`run.py` runs the four steps for one month or a range (backfill) and is what cron calls. Every step writes a row to `dw.run_log`.

## Run it

```bash
docker run -d --name apt-db -e POSTGRES_USER=apartments -e POSTGRES_PASSWORD=apartments \
  -e POSTGRES_DB=apartments -p 5433:5432 postgres:16
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
# put apartments_pl_YYYY_MM.csv files in ./source, then:
python -m pipeline.run --start 2023-08 --end 2024-06
uvicorn api.main:app            # http://localhost:8000/docs
pytest -q && ruff check .       # what CI runs on every push
```

Configuration is read from environment variables (see `.env.example`); nothing is hardcoded.

## Data model

Grain of `fact_listing`: **one row per listing per month it was observed**. A flat on the market for three months is three facts — that is information (how long flats sit), not duplication. `v_listing_latest` collapses to one row per flat, in the last month it was seen, for the pricing questions.

| Table | Key | Notes |
| --- | --- | --- |
| `dim_city` | `city_key` | 15 cities; display name with diacritics, voivodeship |
| `dim_month` | `month_key` | year, month, quarter, first day |
| `fact_listing` | `(id, month_key)` | area, rooms, floor, build year, 5 boolean flags, price, price_per_m2, coordinates, distance to centre |
| `rejects` | — | rows refused by transform, with `reject_reason` |
| `run_log` | `run_id` | every step and check: passed/failed, value, timestamp |

Indexed on `(city_key, month_key)`, which is how every question filters.

## The dirt, and what the pipeline does about it

One month of source is about 19,000 rows × 28 columns. The publisher pre-filters area and price ranges, so the real dirt is elsewhere:

| Issue in source | Handling |
| --- | --- |
| `condition` 76% null, `buildingMaterial` 39%, `type` 21%, `floor` 18%, `buildYear` 17% | kept as NULL; nullable `Int64` / `boolean` dtypes so nothing is silently cast to float or 0 |
| flags as `"yes"` / `"no"` strings | → nullable booleans, case- and whitespace-insensitive |
| city as lowercase without diacritics (`krakow`) | kept as the key; `dim_city` carries `Kraków` for display |
| the same listing id in several monthly files | fact grain is (id, month); `v_listing_latest` for one row per flat |
| implausible rows, e.g. 4 rooms in 37 m² | routed to `rejects` with a reason — 0.01–0.02% of rows per month |
| a column added or renamed upstream | `read_raw` fails loudly instead of producing a different table |

Reject rules, first match wins: `missing_key`, `area_out_of_range` (15–250 m²), `price_out_of_range` (100k–10M PLN), `price_per_m2_out_of_range` (2k–40k), `rooms_implausible_for_area` (< 10 m² per room), `floor_above_building`, `build_year_implausible`, `coords_outside_poland`. Thresholds live in `config.py`.

## Quality checks

Run after every load; each writes to `run_log`; any failure quarantines the month out of `fact_listing` (raw, silver, rejects and `run_log` keep it for diagnosis).

1. rows present for the month
2. no NULL in `id`, `city_key`, `price`, `area_m2`
3. `id` unique within the month
4. every `city_key` exists in `dim_city` (no orphans)
5. reject rate ≤ 10% — above that, the source changed, not the market
6. row count within ±50% of the previous month (observed swings: 1–11%)

`GET /health` reports the latest result of every step and check, so the API itself says whether the data behind it passed.

## Design decisions

- **Raw zone, untouched.** Reprocessing is a loop over `data/raw/month=*`; the source never has to be fetched twice.
- **Rejects with reasons instead of silent drops.** A rising reject rate is the alarm that the source changed.
- **Idempotent at every step.** Extract skips by checksum, transform overwrites the partition, load upserts on the natural key. Rerunning a month changes no counts (`inserted: 0, updated: 18901`).
- **Write, audit, then unpublish on failure.** Checks run on the loaded month; a failed month is removed from `fact_listing` before anyone can query it.
- **Postgres for gold.** 195k rows with one composite index answer every question in milliseconds. The star schema would move to BigQuery or Snowflake unchanged when volume or the number of analysts grows — a change of load target, not a redesign.
- **Questions live in SQL files.** The API is a thin typed wrapper; analysts can read and run the same queries.
- **Deliberately left out:** Spark, streaming, SCD type 2, dbt. Each is the right next step at a different scale; none is needed for 20k rows a month.

## What I would do next

1. Raw and silver to S3 or Azure Blob with the same `month=` partitions; `run.py` as a scheduled container job or a four-task Airflow DAG on a managed service; secrets from a vault.
2. Alerting on `run_log` failures.
3. The rent files (`apartments_rent_pl_*.csv`) through the same pipeline with a `listing_kind` column — not a second pipeline.
4. A transaction-price source as a second fact, to compare offer prices with what flats actually sell for.
5. SCD type 2 on `dim_city` only if city attributes ever change.

## Numbers

11 months (2023-08 → 2024-06), 195,549 listings loaded, 19 rejected, full backfill in 6 seconds. Warsaw median 14,712 PLN/m² in August 2023; with elevator 15,377 vs 13,599 without; within 1 km of the centre 18,384.