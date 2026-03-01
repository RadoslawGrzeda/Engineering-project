import sys
import os
import io
import csv as csv_module
import datetime
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, call

# load_file_develop (so that shema, main are importable)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# repo root (so that apps.logger_config is importable when main is loaded)
_repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, _repo_root)

# ── Pre-import setup ──────────────────────────────────────────────────────────
# main.py runs module-level code on import (creates process_data instance,
# calls validate_shape + load_to_db). We mock the DB engine and file I/O so
# the import does not require a real database or filesystem.

_mock_engine_global = MagicMock()
_mock_conn_global = MagicMock()
_mock_ctx_global = MagicMock()
_mock_ctx_global.__enter__ = MagicMock(return_value=_mock_conn_global)
_mock_ctx_global.__exit__ = MagicMock(return_value=False)
_mock_engine_global.begin.return_value = _mock_ctx_global

# One valid PosInformation row so validate_shape returns a proper DataFrame
_POS_CSV = (
    "art_key,ean,vat_rate,price_net,price_gross\n"
    "200001,5901234123457,0.23,10.0,12.3\n"
)


class _CsvCtx:
    """Context-manager wrapper around StringIO so csv.DictReader works correctly."""

    def __init__(self, content: str):
        self._f = io.StringIO(content)

    def __enter__(self):
        return self._f

    def __exit__(self, *args):
        pass


def _fake_open(path, *args, **kwargs):
    return _CsvCtx(_POS_CSV)


with patch("sqlalchemy.create_engine", return_value=_mock_engine_global), \
     patch("builtins.open", side_effect=_fake_open):
     import main  # noqa: E402

# The class (used to build instances in tests)
ProcessData = main.process_data

from shema import Sector, Department, Segment, Chief, PosInformation, Product, Contractor  # noqa: E402

# ── Helpers ───────────────────────────────────────────────────────────────────

def write_csv(path, rows: list) -> None:
    """Write a list of dicts as a UTF-8 CSV file."""
    with open(path, "w", newline="", encoding="UTF-8") as f:
        if not rows:
            return
        writer = csv_module.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_engine():
    """Returns (engine_mock, conn_mock) with context-manager wiring."""
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = ctx
    return engine, conn


@pytest.fixture
def proc(tmp_path, mock_engine):
    """ProcessData instance with mocked engine. path points to tmp_path/data.csv."""
    engine, _ = mock_engine
    with patch("main.sa.create_engine", return_value=engine):
        instance = ProcessData(str(tmp_path / "data.csv"))
    return instance


# ── validate_shape ────────────────────────────────────────────────────────────

VALID_SECTOR = {"sector_id": "1", "sector_name": "Tech", "sector_code": "T01"}
INVALID_SECTOR = {"sector_id": "0", "sector_name": "Tech", "sector_code": "T01"}  # id=0 fails gt=0


class TestValidateShape:

    def test_valid_row_appears_in_df(self, proc, tmp_path):
        write_csv(tmp_path / "data.csv", [VALID_SECTOR])
        df, errors = proc.validate_shape(Sector)
        assert len(df) == 1
        assert errors == []

    def test_invalid_row_collected_in_errors(self, proc, tmp_path):
        write_csv(tmp_path / "data.csv", [INVALID_SECTOR])
        df, errors = proc.validate_shape(Sector)
        assert len(df) == 0
        assert len(errors) == 1

    def test_mixed_rows_separated(self, proc, tmp_path):
        write_csv(tmp_path / "data.csv", [VALID_SECTOR, INVALID_SECTOR])
        df, errors = proc.validate_shape(Sector)
        assert len(df) == 1
        assert len(errors) == 1

    def test_multiple_valid_rows_all_in_df(self, proc, tmp_path):
        row2 = {"sector_id": "2", "sector_name": "Finance", "sector_code": "F01"}
        write_csv(tmp_path / "data.csv", [VALID_SECTOR, row2])
        df, errors = proc.validate_shape(Sector)
        assert len(df) == 2
        assert errors == []

    def test_returns_tuple_of_df_and_list(self, proc, tmp_path):
        write_csv(tmp_path / "data.csv", [VALID_SECTOR])
        result = proc.validate_shape(Sector)
        assert isinstance(result, tuple)
        assert isinstance(result[0], pd.DataFrame)
        assert isinstance(result[1], list)

    def test_file_not_found_raises(self, proc):
        proc.path = "/nonexistent/does_not_exist.csv"
        with pytest.raises(FileNotFoundError):
            proc.validate_shape(Sector)

    def test_error_tuple_contains_line_number(self, proc, tmp_path):
        write_csv(tmp_path / "data.csv", [INVALID_SECTOR])
        _, errors = proc.validate_shape(Sector)
        line_no, _row, _err = errors[0]
        assert line_no == 2  # header is line 1, first data row is line 2

    def test_valid_segment_row(self, proc, tmp_path):
        row = {
            "segment_id": "1", "segment_code": "ABC",
            "segment_name": "Tech", "chief_id": "CH001", "sector_id": "1",
        }
        write_csv(tmp_path / "data.csv", [row])
        df, errors = proc.validate_shape(Segment)
        assert len(df) == 1
        assert errors == []

    def test_invalid_chief_id_creates_error(self, proc, tmp_path):
        row = {
            "segment_id": "1", "segment_code": "ABC",
            "segment_name": "Tech", "chief_id": "BADID", "sector_id": "1",
        }
        write_csv(tmp_path / "data.csv", [row])
        _, errors = proc.validate_shape(Segment)
        assert len(errors) == 1

    def test_valid_pos_information_row(self, proc, tmp_path):
        row = {
            "art_key": "200001", "ean": "5901234123457",
            "vat_rate": "0.23", "price_net": "10.0", "price_gross": "12.3",
        }
        write_csv(tmp_path / "data.csv", [row])
        df, errors = proc.validate_shape(PosInformation)
        assert len(df) == 1
        assert errors == []

    def test_pos_information_ean_too_short_creates_error(self, proc, tmp_path):
        row = {
            "art_key": "200001", "ean": "12345",  # not 13 chars
            "vat_rate": "0.23", "price_net": "10.0", "price_gross": "12.3",
        }
        write_csv(tmp_path / "data.csv", [row])
        _, errors = proc.validate_shape(PosInformation)
        assert len(errors) == 1

    def test_department_valid_row(self, proc, tmp_path):
        row = {"department_id": "1", "department_name": "IT", "sector_id": "1"}
        write_csv(tmp_path / "data.csv", [row])
        df, errors = proc.validate_shape(Department)
        assert len(df) == 1
        assert errors == []

    def test_department_id_zero_creates_error(self, proc, tmp_path):
        row = {"department_id": "0", "department_name": "IT", "sector_id": "1"}
        write_csv(tmp_path / "data.csv", [row])
        _, errors = proc.validate_shape(Department)
        assert len(errors) == 1


# ── load_to_db ────────────────────────────────────────────────────────────────

class TestLoadToDb:

    def test_unknown_table_returns_without_error(self, proc):
        proc.load_to_db(pd.DataFrame(), "nonexistent_table")  # must not raise

    def test_dispatches_to_load_sector(self, proc):
        with patch.object(proc, "_load_sector") as mock_handler:
            df = pd.DataFrame()
            proc.load_to_db(df, "sector")
            mock_handler.assert_called_once_with(df)

    def test_dispatches_to_load_department(self, proc):
        with patch.object(proc, "_load_department") as mock_handler:
            df = pd.DataFrame()
            proc.load_to_db(df, "department")
            mock_handler.assert_called_once_with(df)

    def test_dispatches_to_load_segment(self, proc):
        with patch.object(proc, "_load_segment") as mock_handler:
            df = pd.DataFrame()
            proc.load_to_db(df, "segment")
            mock_handler.assert_called_once_with(df)

    def test_dispatches_to_load_chief(self, proc):
        with patch.object(proc, "_load_chief") as mock_handler:
            df = pd.DataFrame()
            proc.load_to_db(df, "chief")
            mock_handler.assert_called_once_with(df)

    def test_dispatches_to_load_pos_information(self, proc):
        with patch.object(proc, "_load_pos_information") as mock_handler:
            df = pd.DataFrame()
            proc.load_to_db(df, "pos_information")
            mock_handler.assert_called_once_with(df)

    def test_dispatches_to_load_product(self, proc):
        with patch.object(proc, "_load_product") as mock_handler:
            df = pd.DataFrame()
            proc.load_to_db(df, "product")
            mock_handler.assert_called_once_with(df)

    def test_dispatches_to_load_contractor(self, proc):
        with patch.object(proc, "_load_contractor") as mock_handler:
            df = pd.DataFrame()
            proc.load_to_db(df, "contractor")
            mock_handler.assert_called_once_with(df)


# ── _load_sector ──────────────────────────────────────────────────────────────

class TestLoadSector:

    def _df(self):
        return pd.DataFrame([
            {"sector_id": 1, "sector_name": "Tech", "sector_code": "T01"},
            {"sector_id": 2, "sector_name": "Finance", "sector_code": "F01"},
        ])

    def test_calls_engine_begin(self, proc, mock_engine):
        engine, _ = mock_engine
        proc._load_sector(self._df())
        engine.begin.assert_called_once()

    def test_executes_sql(self, proc, mock_engine):
        _, conn = mock_engine
        proc._load_sector(self._df())
        conn.execute.assert_called_once()

    def test_passes_all_rows_as_records(self, proc, mock_engine):
        _, conn = mock_engine
        proc._load_sector(self._df())
        _, records = conn.execute.call_args[0]
        assert len(records) == 2


# ── _load_department ──────────────────────────────────────────────────────────

class TestLoadDepartment:

    def test_valid_sector_id_row_inserted(self, proc, mock_engine):
        _, conn = mock_engine
        df = pd.DataFrame([{"department_id": 1, "department_name": "IT", "sector_id": 1}])
        with patch.object(proc, "_get_existing_sectors", return_value={1}):
            proc._load_department(df)
        _, records = conn.execute.call_args[0]
        assert len(records) == 1

    def test_invalid_sector_id_row_skipped(self, proc, mock_engine):
        _, conn = mock_engine
        df = pd.DataFrame([{"department_id": 1, "department_name": "IT", "sector_id": 99}])
        with patch.object(proc, "_get_existing_sectors", return_value={1}):
            proc._load_department(df)
        _, records = conn.execute.call_args[0]
        assert len(records) == 0

    def test_mixed_sector_ids_filtered_correctly(self, proc, mock_engine):
        _, conn = mock_engine
        df = pd.DataFrame([
            {"department_id": 1, "department_name": "IT", "sector_id": 1},    # valid
            {"department_id": 2, "department_name": "HR", "sector_id": 99},   # invalid
        ])
        with patch.object(proc, "_get_existing_sectors", return_value={1}):
            proc._load_department(df)
        _, records = conn.execute.call_args[0]
        assert len(records) == 1


# ── _load_segment ─────────────────────────────────────────────────────────────

class TestLoadSegment:

    def _df(self, sector_id=1):
        return pd.DataFrame([{
            "segment_id": 1, "segment_code": "ABC", "segment_name": "Alpha",
            "chief_id": "CH001", "sector_id": sector_id,
        }])

    def test_valid_sector_id_calls_engine_begin(self, proc, mock_engine):
        engine, _ = mock_engine
        with patch.object(proc, "_get_existing_sectors", return_value={1}):
            proc._load_segment(self._df(sector_id=1))
        engine.begin.assert_called_once()

    def test_invalid_sector_id_inserts_zero_records(self, proc, mock_engine):
        _, conn = mock_engine
        with patch.object(proc, "_get_existing_sectors", return_value={1}):
            proc._load_segment(self._df(sector_id=99))
        # All DML execute calls should receive empty record lists
        for c in conn.execute.call_args_list:
            pos_args = c[0]
            if len(pos_args) == 2 and isinstance(pos_args[1], list):
                assert len(pos_args[1]) == 0


# ── _load_chief ───────────────────────────────────────────────────────────────

class TestLoadChief:

    def _df(self):
        return pd.DataFrame([{
            "chief_id": "CH001",
            "chief_first_name": "Jan",
            "chief_last_name": "Kowalski",
            "chief_email": "jan@example.com",
            "chief_phone": "+48123456789",
        }])

    def test_calls_engine_begin(self, proc, mock_engine):
        engine, _ = mock_engine
        proc._load_chief(self._df())
        engine.begin.assert_called_once()

    def test_executes_sql(self, proc, mock_engine):
        _, conn = mock_engine
        proc._load_chief(self._df())
        conn.execute.assert_called_once()

    def test_source_file_added_to_records(self, proc, mock_engine):
        _, conn = mock_engine
        proc.path = "/fake/chief.csv"
        proc._load_chief(self._df())
        _, records = conn.execute.call_args[0]
        assert records[0]["source_file"] == "/fake/chief.csv"


# ── _load_pos_information ─────────────────────────────────────────────────────

class TestLoadPosInformation:

    def _df(self, art_key=200001):
        return pd.DataFrame([{
            "art_key": art_key, "ean": "5901234123457",
            "vat_rate": 0.23, "price_net": 10.0, "price_gross": 12.3,
        }])

    def test_empty_df_does_not_insert(self, proc, mock_engine):
        """Empty df: art_key lookup runs but valid is empty, so no INSERT happens."""
        _, conn = mock_engine
        empty = pd.DataFrame(columns=["art_key", "ean", "vat_rate", "price_net", "price_gross"])
        with patch.object(proc, "_get_existing_art_keys", return_value=set()):
            proc._load_pos_information(empty)
        conn.execute.assert_not_called()

    def test_art_key_not_in_product_table_returns_early(self, proc, mock_engine):
        engine, _ = mock_engine
        with patch.object(proc, "_get_existing_art_keys", return_value=set()):
            proc._load_pos_information(self._df())
        # Only the (patched) FK lookup ran; no INSERT engine.begin call
        engine.begin.assert_not_called()

    def test_new_price_triggers_insert(self, proc, mock_engine):
        engine, _ = mock_engine
        # art_key exists in product; no current price record → new row
        empty_current = pd.DataFrame(
            columns=["art_key", "ean", "price_net", "price_gross", "vat_rate"]
        )
        with patch.object(proc, "_get_existing_art_keys", return_value={200001}), \
             patch.object(proc, "_get_current_pos_information", return_value=empty_current):
            proc._load_pos_information(self._df())
        engine.begin.assert_called_once()

    def test_unchanged_price_skips_insert(self, proc, mock_engine):
        engine, conn = mock_engine
        # Existing record in pos_information has the same price as in df
        current = pd.DataFrame([{
            "art_key": 200001, "ean": "5901234123457",
            "price_net": 10.0, "price_gross": 12.3, "vat_rate": 0.23,
        }])
        with patch.object(proc, "_get_existing_art_keys", return_value={200001}), \
             patch.object(proc, "_get_current_pos_information", return_value=current):
            proc._load_pos_information(self._df())
        conn.execute.assert_not_called()

    def test_changed_price_triggers_insert(self, proc, mock_engine):
        engine, conn = mock_engine
        # Existing record has a DIFFERENT price_net → should insert
        current = pd.DataFrame([{
            "art_key": 200001, "ean": "5901234123457",
            "price_net": 9.0, "price_gross": 11.07, "vat_rate": 0.23,  # different
        }])
        with patch.object(proc, "_get_existing_art_keys", return_value={200001}), \
             patch.object(proc, "_get_current_pos_information", return_value=current):
            proc._load_pos_information(self._df())
        engine.begin.assert_called_once()


# ── _load_product ─────────────────────────────────────────────────────────────

class TestLoadProduct:

    def _df(self):
        return pd.DataFrame([{
            "art_key": 200001, "art_number": "ART-001", "art_name": "Test Product",
            "segment_id": 1, "departament_id": 1, "contractor_id": 1,
            "brand": "BrandX", "pos_information_id": 200001,
            "article_codification_date": "2026-01-01",
        }])

    def test_empty_df_returns_without_engine_call(self, proc, mock_engine):
        engine, _ = mock_engine
        proc._load_product(pd.DataFrame())
        engine.begin.assert_not_called()

    def test_all_valid_fks_inserts_record(self, proc, mock_engine):
        _, conn = mock_engine
        with patch.object(proc, "_get_existing_contractor_ids", return_value={1}), \
             patch.object(proc, "_get_existing_segment_ids", return_value={1}), \
             patch.object(proc, "_get_existing_department_ids", return_value={1}):
            proc._load_product(self._df())
        conn.execute.assert_called_once()
        _, records = conn.execute.call_args[0]
        assert len(records) == 1

    def test_invalid_fks_skips_insert(self, proc, mock_engine):
        engine, conn = mock_engine
        with patch.object(proc, "_get_existing_contractor_ids", return_value=set()), \
             patch.object(proc, "_get_existing_segment_ids", return_value=set()), \
             patch.object(proc, "_get_existing_department_ids", return_value=set()):
            proc._load_product(self._df())
        conn.execute.assert_not_called()

    def test_mixed_fks_filters_correctly(self, proc, mock_engine):
        _, conn = mock_engine
        df = pd.DataFrame([
            {  # valid: all FKs exist
                "art_key": 200001, "art_number": "ART-001", "art_name": "A",
                "segment_id": 1, "departament_id": 1, "contractor_id": 1,
                "brand": "B", "pos_information_id": 200001, "article_codification_date": "2026-01-01",
            },
            {  # invalid: all FKs missing
                "art_key": 200002, "art_number": "ART-002", "art_name": "B",
                "segment_id": 99, "departament_id": 99, "contractor_id": 99,
                "brand": "B", "pos_information_id": 200002, "article_codification_date": "2026-01-01",
            },
        ])
        with patch.object(proc, "_get_existing_contractor_ids", return_value={1}), \
             patch.object(proc, "_get_existing_segment_ids", return_value={1}), \
             patch.object(proc, "_get_existing_department_ids", return_value={1}):
            proc._load_product(df)
        _, records = conn.execute.call_args[0]
        assert len(records) == 1
