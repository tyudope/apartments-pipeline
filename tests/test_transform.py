"""Unit tests for the transform step, against a 10-row fixture: four clean
rows and six deliberately dirty ones. No database needed.
"""
from pathlib import Path

import pandas as pd
import pytest

from pipeline import transform
from pipeline.transform import normalize, read_raw, reject_reasons

FIXTURE = Path(__file__).parent / "fixtures" / "apartments_pl_2099_01.csv"


@pytest.fixture
def frame() -> pd.DataFrame:
    return normalize(read_raw(FIXTURE), "2099-01")


def test_schema_guard_rejects_unexpected_columns(tmp_path):
    df = pd.read_csv(FIXTURE)
    df["surprise"] = 1
    bad = tmp_path / "apartments.csv"
    df.to_csv(bad, index=False)
    with pytest.raises(ValueError, match="raw schema changed"):
        read_raw(bad)


def test_flags_become_booleans_regardless_of_case(frame):
    row = frame.set_index("id").loc["flags_case"]
    assert bool(row["has_elevator"]) is True
    assert bool(row["has_balcony"]) is False
    assert str(frame["has_elevator"].dtype) == "boolean"


def test_price_per_m2_is_derived(frame):
    row = frame.set_index("id").loc["dirty_price_low"]
    assert row["price_per_m2"] == pytest.approx(50000 / row["area_m2"], abs=0.01)


def test_each_dirty_row_gets_the_right_reason(frame):
    by_id = dict(zip(frame["id"], reject_reasons(frame), strict=True))
    assert by_id["dirty_tiny_area"] == "area_out_of_range"
    assert by_id["dirty_price_low"] == "price_out_of_range"
    assert by_id["dirty_rooms"] == "rooms_implausible_for_area"
    assert by_id["dirty_floor"] == "floor_above_building"
    assert by_id["dirty_coords"] == "coords_outside_poland"
    assert by_id["flags_case"] == ""  # odd casing is normalized, not rejected


def test_transform_writes_partition_and_is_rerunnable(tmp_path, monkeypatch):
    raw_dir, silver_dir = tmp_path / "raw", tmp_path / "silver"
    monkeypatch.setattr(transform.config, "RAW_DIR", raw_dir)
    monkeypatch.setattr(transform.config, "SILVER_DIR", silver_dir)
    (raw_dir / "month=2099-01").mkdir(parents=True)
    (raw_dir / "month=2099-01" / "apartments.csv").write_bytes(FIXTURE.read_bytes())

    first = transform.transform("2099-01")
    second = transform.transform("2099-01")
    assert first == second
    assert first["rows_out"] == 5 and first["rejected"] == 5
    assert len(list((silver_dir / "listings" / "month=2099-01").glob("*.parquet"))) == 1