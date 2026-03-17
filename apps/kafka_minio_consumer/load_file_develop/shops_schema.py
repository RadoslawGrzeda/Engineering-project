import pydantic
from pydantic import BaseModel, Field, field_validator,model_validator
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

    @field_validator('site_code',mode='before')
    @classmethod
    def cast_to_string(cls, v):
        return str(v)

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

    @field_validator('contact_type')
    @classmethod
    def validate_contact_type(cls, v):
        if v not in ('EMAIL', 'PHONE', 'FAX'):
            raise ValueError(f"Invalid contact type: {v}")
        return v

    @model_validator(mode='after')
    def validate_contact_value_format(self):
        if self.contact_type == 'EMAIL':
            if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', self.contact_value):
                raise ValueError(f"Invalid email format: {self.contact_value}")
        elif self.contact_type in ('PHONE', 'FAX'):
            if not re.match(r'^\+?\d[\d\s\-]{6,}$', self.contact_value):
                raise ValueError(f"Invalid phone/fax format: {self.contact_value}")
        return self

    @field_validator('contact_role')
    @classmethod
    def validate_contact_role(cls, v):
        if v not in ('MANAGER', 'SUPPORT', 'SALES'):
            raise ValueError(f"Invalid contact role: {v}")
        return v

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
    site_address_street: str = Field(..., min_length=1, max_length=255)
    city_code: str = Field(..., min_length=2, max_length=10)
    country_code: str = Field(..., min_length=2, max_length=5)

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
