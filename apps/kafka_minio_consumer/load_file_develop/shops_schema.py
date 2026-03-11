import pydantic
from pydantic import BaseModel, Field, field_validator
import re
from datetime import date
from typing import Optional


class Site(BaseModel):
    site_unique_code: str = Field(...)
    site_code: str = Field(...)
    site_name: str = Field(..., min_length=1, max_length=100)

    @field_validator('site_unique_code')
    @classmethod
    def validate_site_unique_code(cls, v):
        if not re.match(r'^PL[0-9]{3}$', v):
            raise ValueError(f"Invalid site unique code format: {v}")
        return v


class SiteInfo(BaseModel):
    site_unique_code: str = Field(...)
    site_status_code: str = Field(...)
    site_opening_date: Optional[date] = Field(None)
    site_closing_date: Optional[date] = Field(None)
    is_current: Optional[bool] = Field(None)
    valid_from: Optional[date] = Field(None)
    valid_to: Optional[date] = Field(None)

    @field_validator('is_current', mode='before')
    @classmethod
    def parse_bool(cls, v):
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            vv = v.strip().lower()
            if vv in ('1', 'true', 't', 'yes', 'y'):
                return True
            if vv in ('0', 'false', 'f', 'no', 'n'):
                return False
        return v

    @field_validator('site_unique_code')
    @classmethod
    def validate_site_unique_code(cls, v):
        if not re.match(r'^PL[0-9]{3}$', v):
            raise ValueError(f"Invalid site unique code format: {v}")
        return v

class SiteFormat(BaseModel):
    site_unique_code: str = Field(...)
    site_format_unique_code: str = Field(...)
    is_current: Optional[bool] = Field(None)
    start_date: Optional[date] = Field(None)
    valid_to: Optional[date] = Field(None)

    @field_validator('site_format_unique_code')
    @classmethod
    def validate_site_format_unique_code(cls, v):
        if v not in ('HIPER', 'SUPER'):
            raise ValueError(f"Invalid site format unique code: {v}")
        return v

    @field_validator('site_unique_code')
    @classmethod
    def validate_site_unique_code(cls, v):
        if not re.match(r'^PL[0-9]{3}$', v):
            raise ValueError(f"Invalid site unique code format: {v}")
        return v

    @field_validator('is_current', mode='before')
    @classmethod
    def parse_bool(cls, v):
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            vv = v.strip().lower()
            if vv in ('1', 'true', 't', 'yes', 'y'):
                return True
            if vv in ('0', 'false', 'f', 'no', 'n'):
                return False
        return v


class SiteContact(BaseModel):
    site_unique_code: str = Field(...)
    contact_type: str = Field(...)
    contact_value: str = Field(...)
    contact_role: str = Field(...)
    valid_from: Optional[date] = Field(None)
    valid_to: Optional[date] = Field(None)
    is_primary: Optional[bool] = Field(None)

    @field_validator('site_unique_code')
    @classmethod
    def validate_site_unique_code(cls, v):
        if not re.match(r'^PL[0-9]{3}$', v):
            raise ValueError(f"Invalid site unique code format: {v}")
        return v

    @field_validator('is_primary', mode='before')
    @classmethod
    def parse_bool(cls, v):
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            vv = v.strip().lower()
            if vv in ('1', 'true', 't', 'yes', 'y'):
                return True
            if vv in ('0', 'false', 'f', 'no', 'n'):
                return False
        return v

class SiteAddress(BaseModel):
    site_unique_code: str = Field(...)
    site_address_zip_code: str = Field(...)
    site_address_city: str = Field(..., min_length=1, max_length=100)
    site_address_complement: str = Field(..., min_length=1, max_length=255)
    city_code: str = Field(..., min_length=2, max_length=10)
    country_code: str = Field(..., min_length=2, max_length=5)
    site_geo_coordinate_x_value: Optional[float] = Field(None)
    site_geo_coordinate_y_value: Optional[float] = Field(None)
    is_current: Optional[bool] = Field(None)

    @field_validator('site_unique_code')
    @classmethod
    def validate_site_unique_code(cls, v):
        if not re.match(r'^PL[0-9]{3}$', v):
            raise ValueError(f"Invalid site unique code format: {v}")
        return v

    @field_validator('site_address_zip_code')
    @classmethod
    def validate_zip_code(cls, v):
        if not re.match(r'^\d{2}-\d{3}$', v):
            raise ValueError(f"Invalid zip code format: {v}")
        return v

    @field_validator('city_code')
    @classmethod
    def validate_city_code(cls, v):
        if not re.match(r'^[A-Z0-9]{3}$', v):
            raise ValueError(f"Invalid city code format: {v}")
        return v

    @field_validator('country_code')
    @classmethod
    def validate_country_code(cls, v):
        if not re.match(r'^[A-Z]{2}$', v):
            raise ValueError(f"Invalid country code format: {v}")
        return v

    @field_validator('is_current', mode='before')
    @classmethod
    def parse_bool(cls, v):
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            vv = v.strip().lower()
            if vv in ('1', 'true', 't', 'yes', 'y'):
                return True
            if vv in ('0', 'false', 'f', 'no', 'n'):
                return False
        return v

    @field_validator('site_geo_coordinate_x_value')
    @classmethod
    def validate_latitude(cls, v):
        if not (49.0 <= v <= 55.0):
            raise ValueError(f"Latitude {v} out of range for Poland (49-55)")
        return v

    @field_validator('site_geo_coordinate_y_value')
    @classmethod
    def validate_longitude(cls, v):
        if not (14.0 <= v <= 24.5):
            raise ValueError(f"Longitude {v} out of range for Poland (14-24.5)")
        return v
