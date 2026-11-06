"""Central configuration: every path and connection string the pipeline uses.

Values come from environment variables with local defaults, so the same code
runs on a different environment without breaking.
"""
import os
from pathlib import Path

# The repo root, found from this file's location. Works no matter which
# directory you run the command from.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Where the monthly CSVs are downloaded to. Not part of the pipeline's data;
# it's the upstream system we pull from.
SOURCE_DIR = Path(os.getenv("SOURCE_DIR", PROJECT_ROOT / "source"))

# The data zones.
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"        # files exactly as received
SILVER_DIR = DATA_DIR / "silver"  # cleaned, typed Parquet

# The warehouse.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://apartments:apartments@localhost:5433/apartments",
)


# Expected column set of a raw file. If the publisher adds or renames a column,
# transform fails loudly instead of silently producing a different table.
RAW_COLUMNS = [
    "id", "city", "type", "squareMeters", "rooms", "floor", "floorCount",
    "buildYear", "latitude", "longitude", "centreDistance", "poiCount",
    "schoolDistance", "clinicDistance", "postOfficeDistance",
    "kindergartenDistance", "restaurantDistance", "collegeDistance",
    "pharmacyDistance", "ownership", "buildingMaterial", "condition",
    "hasParkingSpace", "hasBalcony", "hasElevator", "hasSecurity",
    "hasStorageRoom", "price",
]

# Plausibility bounds. The publisher pre-filters some of these; we enforce
# them anyway because the pipeline must not depend on upstream behaviour.
MIN_AREA_M2, MAX_AREA_M2 = 15.0, 250.0
MIN_PRICE_PLN, MAX_PRICE_PLN = 100_000, 10_000_000
MIN_PPM2, MAX_PPM2 = 2_000, 40_000          # price per m2
MIN_M2_PER_ROOM = 10.0                      # 4 rooms in 35 m2 is not a flat
MIN_BUILD_YEAR = 1850
POLAND_LAT = (49.0, 55.0)
POLAND_LON = (14.0, 24.2)