"""Extract: land one month's source file in the raw zone, untouched.

The source publishes one CSV per month. This step copies it to
data/raw/month=YYYY-MM/apartments.csv and records a checksum, so the pipeline
can always reprocess from exactly what it received.

Idempotent: if the raw file already exists with the same checksum, nothing is
written. A re-published source file (same name, new content) is re-landed.

    python -m pipeline.extract --month 2023-08
"""
import argparse
import hashlib
import logging
import shutil
from pathlib import Path

from pipeline import config

log = logging.getLogger(__name__)


def source_path(month: str) -> Path:
    """'2023-08' -> source/apartments_pl_2023_08.csv"""
    year, mon = month.split("-")
    return config.SOURCE_DIR / f"apartments_pl_{year}_{mon}.csv"


def raw_path(month: str) -> Path:
    """Partition layout: month=YYYY-MM, the convention Spark/Hive/Athena read natively."""
    return config.RAW_DIR / f"month={month}" / "apartments.csv"


def sha256(path: Path) -> str:
    """Checksum in 1 MB chunks so large files don't need to fit in memory."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def extract(month: str) -> dict:
    src = source_path(month)
    if not src.exists():
        raise FileNotFoundError(f"source file for {month} not found: {src}")

    dst = raw_path(month)
    src_hash = sha256(src)

    if dst.exists() and sha256(dst) == src_hash:
        log.info("extract %s: raw file already present with same checksum, skipping", month)
        return {"month": month, "written": False, "bytes": dst.stat().st_size, "sha256": src_hash}

    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)
    log.info("extract %s: wrote %s (%d bytes)", month, dst, dst.stat().st_size)
    return {"month": month, "written": True, "bytes": dst.stat().st_size, "sha256": src_hash}


def main() -> None:
    parser = argparse.ArgumentParser(description="Land one month's source file in the raw zone.")
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    print(extract(args.month))


if __name__ == "__main__":
    main()