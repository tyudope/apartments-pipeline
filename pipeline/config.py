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