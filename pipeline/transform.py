"""
Transform: raw CSV -> typed, cleaned 


Rules applied, in ordeR:

1. Schema guard: the raw file must have exactuly the expected columns.
2. Normalize: snake_case names, yes/no -> bool, text lowercased and stripped.
3. Derive: price_per_m2, month_key.
4. Route bad rows to a rejects table WITH A REASON instead of dropping them, 
    A rising reject rate is how we notice the source changed.
5. Write silver/listings/month=YYYY-MM/ and silve/rejects/month=YYYY-MM/, overwriting the partition


python -m pipeline.transform --month 2023-08


"""

import argparse
import logging
from pathlib import Path


import pandas as pd

from pipeline import config

log = logging.getLogger(__name__)


RENAME = {
    "squareMeters" : "area_m2",
    "floorCount": "floor_count",
    "buildYear": "build_year",
    "centreDistance": "centre_distance_km",
    "poiCount": "poi_count",
    "buildingMaterial": "building_material",
    "hasParkingSpace":"has_parking",
    "hasBalcony":"hac_balcony",
    "hasElevator":"has_elevator",
    "hasSecurity": "has_security",
    "hasStorageRoom":"has_storage",
    "type":"building_type",
}

FLAG_COLUMNS = ["has_parking", "has_balcony", "has_elevator", "has_security", "has_storage"]
TEXT_COLUMNS = ["city", "building_type", "ownership", "building_material", "condition"]


# Columns that go to the fact table. The seven POI distances stay in raw only.
# they don't serve the questions we answer.

SILVER_COLUMNS = [
    "id", "month_key", "city", "building_type", "ownership", "building_material",
    "condition", "area_m2", "rooms", "floor", "floor_count", "build_year",
    "latitude", "longitude", "centre_distance_km", "poi_count",
    *FLAG_COLUMNS, "price", "price_per_m2",
]

REJECT_COLUMNS = ["id", "month_key", "city", "reject_reason", "area_m2", "rooms", "price", "price_per_m2"]


def read_raw(path:Path) -> pd.DataFrame:

    """Read the raw CSV and refuse to continue if its columns changed."""
    df = pd.read_csv(path, dtype = {"id":"string", "city":"string"})
    missing = set (config.RAW_COLUMNS) - set(df.columns)
    extra = set(df.columns) - set(config.RAW_COLUMNS)


    if missing or extra:
        raise ValueError(f"raw schema changed: missing = {sorted(missing)}, extra = {sorted(extra)}")

    return df



def normalize(df: pd.DataFrame, month_key: str) -> pd.DataFrame:
    """Rename, tyep, and standardize. No rows are dropped here."""


    df = df.rename(columns = RENAME).copy()

    df["month_key"] = month_key


    for col in TEXT_COLUMNS:
        df[col] = df[col].astype("string").str.strip().str.lower()


    # "yes"/"no" strings -> nullable booleans, Anything else becomes <NA>

    for col in FLAG_COLUMNS:
        df[col] = (
            df[col].astype("string").str.strip().str.lower()
            .map({"yes":True, "no":False})
            .astype("boolean")
        )


    # Integers that arrive as floats because of NaNs -> Nullable in64

    for col in ["rooms", "floor", "floor_count", "build_year", "poi_count"]:
        df[col] = df[col].round().astype("Int64")


    df["price"] = df["price"].astype("int64")
    df["price_per_m2"] = (df["price"] / df["area_m2"]).round(2)

    return df




def reject_reasons(df: pd.DataFrame) -> pd.Series:
    """One reason per row ('' = keep). First failing rule wins."""
    c = config
    listing_year = int(df["month_key"].iloc[0][:4])
    rules = [
        ("missing_key", df["id"].isna() | df["city"].isna() | df["price"].isna() | df["area_m2"].isna()),
        ("area_out_of_range", ~df["area_m2"].between(c.MIN_AREA_M2, c.MAX_AREA_M2)),
        ("price_out_of_range", ~df["price"].between(c.MIN_PRICE_PLN, c.MAX_PRICE_PLN)),
        ("price_per_m2_out_of_range", ~df["price_per_m2"].between(c.MIN_PPM2, c.MAX_PPM2)),
        ("rooms_implausible_for_area", (df["area_m2"] / df["rooms"]) < c.MIN_M2_PER_ROOM),
        ("floor_above_building", df["floor"] > df["floor_count"]),
        ("build_year_implausible", (df["build_year"] < c.MIN_BUILD_YEAR) | (df["build_year"] > listing_year + 1)),
        ("coords_outside_poland", ~df["latitude"].between(*c.POLAND_LAT) | ~df["longitude"].between(*c.POLAND_LON)),
    ]
    reason = pd.Series("", index=df.index, dtype="string")
    for name, mask in rules:
        mask = mask.fillna(False).astype(bool)   # <NA> comparisons never reject
        reason = reason.mask((reason == "") & mask, name)
    return reason



def write_partition(frame:pd.DataFrame, zone:str, month:str) -> None:
    """Overwrite the month partition: deete old parts, write one new part."""

    out_dir = config.SILVER_DIR / zone / f"month={month}"
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("*.parquet"):
        old.unlink()

    frame.to_parquet(out_dir / "part-0.parquet", index = False)



def transform(month: str) -> dict:

    raw_file = config.RAW_DIR / f"month={month}" / "apartments.csv"
    df = normalize(read_raw(raw_file), month)
    df["reject_reason"] = reject_reasons(df)


    listings = df.loc[df["reject_reason"] == "", SILVER_COLUMNS]
    rejects = df.loc[df["reject_reason"] != "", REJECT_COLUMNS]

    #Duplicate ids innside one month are source big; keep the first, log the rest.

    dup = listings["id"].duplicated(keep="first")
    if dup.any():
        log.warning("transform %s: %d duplicate ids with the month, keeping first", month, int(dup.sum()))
        listings = listings[~dup]


    write_partition(listings, "listings", month)
    write_partition(rejects, "rejects", month)

    stats = {
        "month":month,
        "rows_in":int(len(df)),
        "rows_out":int(len(listings)),
        "rejected":int(len(rejects)),
        "reject_reasons": rejects["reject_reason"].value_counts().to_dict(),
        "null_pct": {k: round(float(v), 1) for k,v in (listings.isna().mean() * 100).items() if v > 0},
    }
    log.info("transform %s: in=%d out=%d rejected=%d %s", month, stats["rows_in"], stats["rows_out"], stats["rejected"], stats["reject_reasons"])
    return stats




def main() -> None:
    parser = argparse.ArgumentParser(description="Clean one month of raw listings into silver parquet.")
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    print(transform(args.month))



if __name__ == "__main__":
    main()
