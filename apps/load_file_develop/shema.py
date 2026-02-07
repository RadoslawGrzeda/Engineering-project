from pydantic import BaseModel, Field, EmailStr ,field_validator
from pydantic_extra_types.phone_numbers import PhoneNumber
from typing import List, Optional   
import datetime
import re

from email_validator import validate_email, EmailNotValidError

class Department(BaseModel):
    department_id: int = Field(..., gt=0)
    department_name: str = Field(...,min_length=1,max_length=100)
    sector_id: int   = Field(..., gt=0)

class Sector(BaseModel):
    sector_id: int = Field(..., gt=0,)
    sector_name: str = Field(...,min_length=1)
    sector_code: str = Field(...)

class Segment(BaseModel):
    segment_id: int = Field(..., gt=0)
    segment_code: str = Field(...)
    segment_name: str = Field(...,min_length=1,max_length=100)
    # chief_id: str = Field(..., alias='chief_id')
    sector_id: int = Field(..., gt=0)

class Chief(BaseModel):
    chief_id: str = Field(...)
    # segment_id: int = Field(..., gt=0)
    chief_first_name: str = Field(...,min_length=1,max_length=50)
    chief_last_name: str = Field(...,min_length=1,max_length=50)
    chief_email: str = Field(...,min_length=5,max_length=100)
    chief_phone: PhoneNumber = Field(...)
    # is_current: bool = Field(...)
    # date_start: datetime.date = Field(...)
    # date_end: Optional[datetime.date] = Field(None)


    @field_validator('chief_id')
    @classmethod
    def validate_chief_id(cls, v):
        try:
            return v if re.match(r'^CH[0-9]{3}$', v) else ValueError(f"Invalid chief id format: {v}")
        except Exception as e:
            raise ValueError(f"Invalid chief id format: {v}. Error: {str(e)}")

    @field_validator('chief_email')
    @classmethod
    def validate_chief_email(cls, v):
        try:
            regex=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            return v if re.match(regex, v) else ValueError(f"Invalid email format: {v}")
        except EmailNotValidError as e:
            raise ValueError(f"Invalid email address: {v}. Error: {str(e)}")

'''
    @field_validator('date_end',mode='before')
    @classmethod
    def validate_date_end(cls, v):
        if v in (None, ''):
            return None
        return v
    

 
    @field_validator("is_current", mode="before")
    @classmethod
    def parse_bool(cls, v):
        if isinstance(v, str):
            vv = v.strip().lower()
            if vv in ("1", "true", "t", "yes", "y"):
                return True
            if vv in ("0", "false", "f", "no", "n"):
                return False
        return v
'''

class Category(BaseModel):
    category_id: str = Field(..., alias='category_id')
    category: str = Field(...,min_length=1,max_length=50, alias='category')
    category_code: str = Field(..., alias='category_code')

class DepartmentSectors(BaseModel):
    department_id: int = Field(..., gt=0, alias='department_id')
    sector_id: int = Field(..., gt=0, alias='sector_id')
    department_name: str = Field(...,min_length=1,max_length=50, alias='department_name')
    sector_code: str = Field(...,min_length=1,max_length=50, alias='sector_code')
    is_current: bool = Field(..., alias='is_current')
    date_start: datetime.date = Field(..., alias='date_start')
    date_end: Optional[datetime.date] = Field(None, alias='date_end')


# """