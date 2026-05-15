import sys
import os
from unittest.mock import patch
from yaml import safe_dump

import pytest
import pandas as pd

import main
from main import _validate_col_type, validate_against_contract, load_contract


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_CONTRACT = {
    "table": "test_table",
    "description": "Kontrakt testowy",
    "sla": {"freshness": "when_file_arrives", "owner": "data-team", "max_delay_hours": 24},
    "change_policy": {"review_required": True, "notify": ["data-team"], "breaking_changes": "require_approval"},
    "columns": [
        {"name": "id",         "type": "integer", "nullable": False},
        {"name": "name",       "type": "string",  "nullable": False},
        {"name": "score",      "type": "float",   "nullable": True},
        {"name": "active",     "type": "boolean", "nullable": False},
        {"name": "created_at", "type": "date",    "nullable": True},
    ],
}


@pytest.fixture
def valid_df():
    return pd.DataFrame({
        "id":         [1, 2, 3],
        "name":       ["Alice", "Bob", "Charlie"],
        "score":      [9.5, 8.0, None],
        "active":     ["true", "false", "true"],
        "created_at": ["2024-01-01", None, "2024-03-15"],
    })


# ---------------------------------------------------------------------------
# load_contract
# ---------------------------------------------------------------------------

class TestLoadContract:
    def test_returns_none_gdy_brak_pliku(self, tmp_path, monkeypatch):
        monkeypatch.setattr(main, "CONTRACTS_DIR", str(tmp_path))
        assert load_contract("nieistniejacy") is None

    def test_wczytuje_yaml_poprawnie(self, tmp_path, monkeypatch):
        monkeypatch.setattr(main, "CONTRACTS_DIR", str(tmp_path))
        (tmp_path / "chief.yaml").write_text(safe_dump(SAMPLE_CONTRACT), encoding="utf-8")

        result = load_contract("chief")

        assert result is not None
        assert result["table"] == "test_table"
        assert len(result["columns"]) == 5

    def test_wczytuje_wszystkie_sekcje(self, tmp_path, monkeypatch):
        monkeypatch.setattr(main, "CONTRACTS_DIR", str(tmp_path))
        (tmp_path / "segment.yaml").write_text(safe_dump(SAMPLE_CONTRACT), encoding="utf-8")

        result = load_contract("segment")

        assert "sla" in result
        assert "change_policy" in result
        assert result["sla"]["owner"] == "data-team"
        assert result["change_policy"]["review_required"] is True


# ---------------------------------------------------------------------------
# _validate_col_type
# ---------------------------------------------------------------------------

class TestValidateColType:

    # --- string ---
    def test_string_zawsze_valid(self):
        assert _validate_col_type(pd.Series(["abc", "def"]), "string") is True

    def test_string_z_liczbami_valid(self):
        assert _validate_col_type(pd.Series(["1", "2", "3"]), "string") is True

    # --- integer ---
    def test_integer_poprawne_liczby(self):
        assert _validate_col_type(pd.Series([1, 2, 3]), "integer") is True

    def test_integer_jako_string(self):
        assert _validate_col_type(pd.Series(["1", "2", "3"]), "integer") is True

    def test_integer_z_czescią_dziesietna_fail(self):
        assert _validate_col_type(pd.Series([1.5, 2.5]), "integer") is False

    def test_integer_tekst_fail(self):
        assert _validate_col_type(pd.Series(["abc", "def"]), "integer") is False

    # --- float ---
    def test_float_poprawne(self):
        assert _validate_col_type(pd.Series([1.5, 2.0, 3.14]), "float") is True

    def test_float_liczby_calkowite_valid(self):
        assert _validate_col_type(pd.Series([1, 2, 3]), "float") is True

    def test_float_tekst_fail(self):
        assert _validate_col_type(pd.Series(["abc"]), "float") is False

    # --- boolean ---
    @pytest.mark.parametrize("value", [
        "true", "false", "True", "False",
        "1", "0", "yes", "no", "t", "f", "y", "n",
    ])
    def test_boolean_poprawne_wartosci(self, value):
        assert _validate_col_type(pd.Series([value]), "boolean") is True

    def test_boolean_niepoprawna_wartosc(self):
        assert _validate_col_type(pd.Series(["maybe", "2", "tak"]), "boolean") is False

    # --- date ---
    def test_date_poprawna(self):
        assert _validate_col_type(pd.Series(["2024-01-01", "2024-12-31"]), "date") is True

    def test_date_niepoprawna(self):
        assert _validate_col_type(pd.Series(["nie-data", "32-13-2024"]), "date") is False

    # --- edge cases ---
    def test_sama_null_zwraca_true(self):
        assert _validate_col_type(pd.Series([None, None]), "integer") is True

    def test_pusta_seria_zwraca_true(self):
        assert _validate_col_type(pd.Series([], dtype=object), "integer") is True


# ---------------------------------------------------------------------------
# validate_against_contract
# ---------------------------------------------------------------------------

class TestValidateAgainstContract:

    def test_poprawny_dataframe(self, valid_df):
        result = validate_against_contract(valid_df, SAMPLE_CONTRACT)
        assert result["valid"] is True
        assert result["missing"] == []
        assert result["extra"] == []
        assert result["nullable_errors"] == []
        assert result["type_errors"] == []

    def test_brakujace_kolumny(self):
        df = pd.DataFrame({"id": [1, 2]})
        result = validate_against_contract(df, SAMPLE_CONTRACT)
        assert result["valid"] is False
        assert "name" in result["missing"]
        assert "score" in result["missing"]
        assert "active" in result["missing"]

    def test_dodatkowe_kolumny(self, valid_df):
        valid_df["extra_col"] = "nadmiarowa"
        result = validate_against_contract(valid_df, SAMPLE_CONTRACT)
        assert result["valid"] is False
        assert "extra_col" in result["extra"]

    def test_brakujace_i_dodatkowe_konczy_wczesnie(self):
        # przy niezgodności kolumn nie sprawdzamy typów ani nullable
        df = pd.DataFrame({"zla_kolumna": [1, 2, 3]})
        result = validate_against_contract(df, SAMPLE_CONTRACT)
        assert result["valid"] is False
        assert result["nullable_errors"] == []
        assert result["type_errors"] == []

    def test_naruszenie_nullable(self, valid_df):
        valid_df["name"] = [None, "Bob", "Charlie"]  # name jest non-nullable
        result = validate_against_contract(valid_df, SAMPLE_CONTRACT)
        assert result["valid"] is False
        assert "name" in result["nullable_errors"]

    def test_nullable_true_pozwala_na_nulle(self, valid_df):
        # score i created_at są nullable=True — nulle są ok
        result = validate_against_contract(valid_df, SAMPLE_CONTRACT)
        assert "score" not in result["nullable_errors"]
        assert "created_at" not in result["nullable_errors"]

    def test_blad_typu(self, valid_df):
        valid_df["id"] = ["nie_int", "tez_nie", "nope"]
        result = validate_against_contract(valid_df, SAMPLE_CONTRACT)
        assert result["valid"] is False
        assert "id" in result["type_errors"]

    def test_wiele_bledow_zbieranych_razem(self, valid_df):
        valid_df["name"] = [None, None, None]    # nullable error
        valid_df["id"]   = ["abc", "def", "ghi"]  # type error
        result = validate_against_contract(valid_df, SAMPLE_CONTRACT)
        assert result["valid"] is False
        assert "name" in result["nullable_errors"]
        assert "id" in result["type_errors"]


# ---------------------------------------------------------------------------
# StreamlitApp.init_config
# ---------------------------------------------------------------------------

class TestInitConfig:
    @patch("minio_client.Minio")
    def test_zastepuje_wszystkie_placeholdery(self, _mock_minio, tmp_path, monkeypatch):
        monkeypatch.setenv("API_KEY",     "secret_key")
        monkeypatch.setenv("name",        "user1")
        monkeypatch.setenv("password",    "pass1")
        monkeypatch.setenv("admin_pass",  "admin_pass1")
        monkeypatch.setenv("admin_name",  "admin1")

        config_content = """\
cookie:
  name: test_cookie
  key: COOKIE_KEY_PLACEHOLDER
  expiry_days: 30
credentials:
  usernames:
    USER_NAME_PLACEHOLDER:
      password: USER_PASSWORD_PLACEHOLDER
    ADMIN_NAME_PLACEHOLDER:
      password: ADMIN_PASSWORD_PLACEHOLDER
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        app = main.StreamlitApp()
        result = app.init_config(str(config_file))

        assert result["cookie"]["key"] == "secret_key"
        assert "user1" in result["credentials"]["usernames"]
        assert "admin1" in result["credentials"]["usernames"]

    @patch("minio_client.Minio")
    def test_brak_placeholdera_pozostawia_wartosc(self, _mock_minio, tmp_path, monkeypatch):
        monkeypatch.setenv("API_KEY",    "key")
        monkeypatch.setenv("name",       "u")
        monkeypatch.setenv("password",   "p")
        monkeypatch.setenv("admin_pass", "ap")
        monkeypatch.setenv("admin_name", "an")

        config_content = """\
cookie:
  name: moj_cookie
  key: staly_klucz
  expiry_days: 7
credentials:
  usernames: {}
"""
        (tmp_path / "config.yaml").write_text(config_content)
        app = main.StreamlitApp()
        result = app.init_config(str(tmp_path / "config.yaml"))

        assert result["cookie"]["key"] == "staly_klucz"