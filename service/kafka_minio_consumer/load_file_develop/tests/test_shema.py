import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import datetime
from pydantic import ValidationError

from shema import (
    Department,
    Sector,
    PosInformation,
    Segment,
    Chief,
    Product,
    Contractor,
    Category,
    DepartmentSectors,
)

# --- Department ---

def test_department_valid_row():
    row = {
        "department_id": 1,
        "department_name": "IT",
        "sector_id": 1,
    }
    obj = Department.model_validate(row)
    assert obj.department_id == 1
    assert obj.department_name == "IT"
    assert obj.sector_id == 1


def test_department_missing_required_rejected():
    row = {"department_id": 1, "sector_id": 1}
    with pytest.raises(ValidationError):
        Department.model_validate(row)


def test_department_id_zero_rejected():
    row = {"department_id": 0, "department_name": "X", "sector_id": 1}
    with pytest.raises(ValidationError):
        Department.model_validate(row)


# --- Sector ---

def test_sector_valid_row():
    row = {
        "sector_id": 1,
        "sector_name": "Tech",
        "sector_code": "T01",
    }
    obj = Sector.model_validate(row)
    assert obj.sector_id == 1
    assert obj.sector_name == "Tech"
    assert obj.sector_code == "T01"


def test_sector_missing_required_rejected():
    row = {"sector_id": 1, "sector_name": "Tech"}
    with pytest.raises(ValidationError):
        Sector.model_validate(row)


# --- PosInformation ---

def test_pos_information_valid_row():
    row = {
        "art_key": 200001,
        "ean": "5901234123457",
        "vat_rate": 0.23,
        "price_net": 10.0,
        "price_gross": 12.3,
    }
    obj = PosInformation.model_validate(row)
    assert obj.art_key == 200001
    assert obj.ean == "5901234123457"
    assert obj.vat_rate == 0.23


def test_pos_information_missing_required_rejected():
    row = {"art_key": 200001, "ean": "5901234123457", "vat_rate": 0.23}
    with pytest.raises(ValidationError):
        PosInformation.model_validate(row)


def test_pos_information_ean_not_13_chars_rejected():
    row = {
        "art_key": 200001,
        "ean": "5901234",
        "vat_rate": 0.23,
        "price_net": 10.0,
        "price_gross": 12.3,
    }
    with pytest.raises(ValidationError):
        PosInformation.model_validate(row)


def test_pos_information_art_key_too_low_rejected():
    row = {
        "art_key": 1000,
        "ean": "5901234123457",
        "vat_rate": 0.23,
        "price_net": 10.0,
        "price_gross": 12.3,
    }
    with pytest.raises(ValidationError):
        PosInformation.model_validate(row)


# --- Segment ---

def test_segment_valid_row():
    row = {
        "segment_id": 1,
        "segment_code": "ABC",
        "segment_name": "Segment A",
        "chief_id": "CH001",
        "sector_id": 1,
    }
    obj = Segment.model_validate(row)
    assert obj.segment_id == 1
    assert obj.segment_code == "ABC"
    assert obj.chief_id == "CH001"


def test_segment_missing_required_rejected():
    row = {"segment_id": 1, "segment_code": "ABC", "segment_name": "X", "sector_id": 1}
    with pytest.raises(ValidationError):
        Segment.model_validate(row)


def test_segment_chief_id_invalid_format_rejected():
    row = {
        "segment_id": 1,
        "segment_code": "ABC",
        "segment_name": "X",
        "chief_id": "INVALID",
        "sector_id": 1,
    }
    with pytest.raises(ValidationError):
        Segment.model_validate(row)


def test_segment_segment_code_invalid_format_rejected():
    row = {
        "segment_id": 1,
        "segment_code": "ab12",
        "segment_name": "X",
        "chief_id": "CH001",
        "sector_id": 1,
    }
    with pytest.raises(ValidationError):
        Segment.model_validate(row)


# --- Chief ---

def test_chief_valid_row():
    row = {
        "chief_id": "CH001",
        "chief_first_name": "Jan",
        "chief_last_name": "Kowalski",
        "chief_email": "jan.kowalski@example.com",
        "chief_phone": "+48123456789",
    }
    obj = Chief.model_validate(row)
    assert obj.chief_id == "CH001"
    assert obj.chief_email == "jan.kowalski@example.com"


def test_chief_missing_required_rejected():
    row = {
        "chief_id": "CH001",
        "chief_first_name": "Jan",
        "chief_last_name": "Kowalski",
        "chief_email": "jan@example.com",
    }
    with pytest.raises(ValidationError):
        Chief.model_validate(row)


def test_chief_chief_id_invalid_format_rejected():
    row = {
        "chief_id": "X99",
        "chief_first_name": "Jan",
        "chief_last_name": "Kowalski",
        "chief_email": "jan@example.com",
        "chief_phone": "+48123456789",
    }
    with pytest.raises(ValidationError):
        Chief.model_validate(row)


def test_chief_chief_email_invalid_format_rejected():
    row = {
        "chief_id": "CH001",
        "chief_first_name": "Jan",
        "chief_last_name": "Kowalski",
        "chief_email": "not-an-email",
        "chief_phone": "+48123456789",
    }
    with pytest.raises(ValidationError):
        Chief.model_validate(row)


# --- Product ---

def test_product_valid_row():
    row = {
        "art_key": 200001,
        "art_number": "ART-100001",
        "art_name": "Test Product",
        "segment_id": 1,
        "departament_id": 1,
        "contractor_id": 1,
        "brand": "Test Brand",
        "pos_information_id": 200001,
        "article_codification_date": "2026-02-05",
    }
    obj = Product.model_validate(row)
    assert obj.art_key == 200001
    assert obj.art_number == "ART-100001"
    assert obj.brand == "Test Brand"


def test_product_missing_required_rejected():
    row = {
        "art_key": 200001,
        "art_number": "ART-100001",
        "art_name": "Test Product",
        "segment_id": 1,
        "departament_id": 1,
        "contractor_id": 1,
        "brand": "Test Brand",
        "pos_information_id": 200001,
    }
    with pytest.raises(ValidationError):
        Product.model_validate(row)


def test_product_art_key_too_low_rejected():
    row = {
        "art_key": 100,
        "art_number": "ART-100",
        "art_name": "X",
        "segment_id": 1,
        "departament_id": 1,
        "contractor_id": 1,
        "brand": "X",
        "pos_information_id": 2000,
        "article_codification_date": "2026-02-05",
    }
    with pytest.raises(ValidationError):
        Product.model_validate(row)


def test_product_empty_art_number_rejected():
    row = {
        "art_key": 200001,
        "art_number": "",
        "art_name": "X",
        "segment_id": 1,
        "departament_id": 1,
        "contractor_id": 1,
        "brand": "X",
        "pos_information_id": 2000,
        "article_codification_date": "2026-02-05",
    }
    with pytest.raises(ValidationError):
        Product.model_validate(row)


# --- Contractor ---

def test_contractor_valid_row():
    row = {
        "contractor_id": 1,
        "contractor_name": "Test Contractor",
        "contractor_phone_number": "+48123456789",
        "contractor_email_address": "test@example.com",
        "contractor_address": "ul. Test 1",
        "contract_number": "CN/2024/001",
    }
    obj = Contractor.model_validate(row)
    assert obj.contractor_id == 1
    assert obj.contractor_name == "Test Contractor"
    assert obj.contractor_email_address == "test@example.com"


def test_contractor_missing_required_rejected():
    row = {
        "contractor_id": 1,
        "contractor_name": "Test",
        "contractor_phone_number": "+48123456789",
        "contractor_address": "Address",
    }
    with pytest.raises(ValidationError):
        Contractor.model_validate(row)


def test_contractor_email_invalid_format_rejected():
    row = {
        "contractor_id": 1,
        "contractor_name": "Test Contractor",
        "contractor_phone_number": "+48123456789",
        "contractor_email_address": "invalid-email",
        "contractor_address": "ul. Test 1",
        "contract_number": "CN001",
    }
    with pytest.raises(ValidationError):
        Contractor.model_validate(row)


# --- Category (alias) ---

def test_category_valid_row():
    row = {
        "category_id": "C01",
        "category": "Electronics",
        "category_code": "ELEC",
    }
    obj = Category.model_validate(row)
    assert obj.category_id == "C01"
    assert obj.category == "Electronics"


def test_category_missing_required_rejected():
    row = {"category_id": "C01", "category": "Electronics"}
    with pytest.raises(ValidationError):
        Category.model_validate(row)


# --- DepartmentSectors ---

def test_department_sectors_valid_row():
    row = {
        "department_id": 1,
        "sector_id": 1,
        "department_name": "IT",
        "sector_code": "T01",
        "is_current": True,
        "date_start": "2024-01-01",
        "date_end": None,
    }
    obj = DepartmentSectors.model_validate(row)
    assert obj.department_id == 1
    assert obj.sector_id == 1
    assert obj.is_current is True


def test_department_sectors_missing_required_rejected():
    row = {
        "department_id": 1,
        "sector_id": 1,
        "department_name": "IT",
        "sector_code": "T01",
    }
    with pytest.raises(ValidationError):
        DepartmentSectors.model_validate(row)