import sys 
import os 
import pytest
import datetime
from pydantic import ValidationError
import sqlalchemy as sa
from dotenv import load_dotenv
load_dotenv()

def init_db():
    connection_string=os.getenv("postgress_connection")
    engine=sa.create_engine(connection_string)
    return engine


def test_db_connection():
    engine=init_db()
    assert engine.connect()

def test_tables_exist():
    engine=init_db()
    inspector=sa.inspect(engine)
    tables=inspector.get_table_names()
    assert 'sector' in tables
    assert 'department' in tables
    assert 'segment' in tables
    assert 'chief' in tables
    assert 'pos_information' in tables
    assert 'contractor' in tables
    assert 'product' in tables



# Wymagane kolumny per tabela (zgodne z INSERTami w main.py)
REQUIRED_COLUMNS = {
    "sector": ["sector_id", "sector_name", "sector_code", "source_file"],
    "department": ["department_id", "department_name", "sector_id", "source_file"],
    "segment": ["segment_id", "segment_code", "segment_name", "sector_id", "source_file"],
    "segment_chief": ["segment_id", "chief_id", "is_current", "valid_from", "valid_to", "source_file"],
    "chief": ["chief_id", "chief_first_name", "chief_last_name", "email_address", "phone_number", "source_file"],
    "contractor": [
        "contractor_id", "contractor_name", "contractor_phone_number",
        "contractor_email_address", "contractor_address", "source_file",
        "created_at", "updated_at",
    ],
    "contract": [
        "contractor_id", "contract_number", "signed_date", "status",
        "is_current", "valid_from", "valid_to", "source_file",
    ],
    "pos_information": [
        "art_key", "ean", "vat_rate", "price_net", "price_gross",
        "date_start", "date_end", "is_current", "last_modified_date", "source_file",
    ],
    "product": [
        "art_key", "art_number", "contractor_id", "segment_id", "department_id",
        "brand", "article_codification_date", "last_modified_at", "source_file",
    ],
}


def test_required_columns_exist():
    """Sprawdza, że każda tabela ma wszystkie wymagane kolumny (zgodne z logiką ładowania w main)."""
    engine = init_db()
    inspector = sa.inspect(engine)
    existing_tables = inspector.get_table_names()

    for table_name, required in REQUIRED_COLUMNS.items():
        assert table_name in existing_tables, f"Tabela {table_name!r} nie istnieje w bazie."
        columns = [c["name"] for c in inspector.get_columns(table_name)]
        missing = [col for col in required if col not in columns]
        assert not missing, (
            f"Tabela {table_name!r}: brakuje wymaganych kolumn: {missing}. "
            f"Dostępne: {sorted(columns)}"
        )

