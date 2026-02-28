from multiprocessing import Value
from pydantic import BaseModel, Field, EmailStr ,field_validator
# from pydantic_extra_types.phone_numbers import PhoneNumber
from typing import List, Optional   
import datetime
import re

from email_validator import validate_email, EmailNotValidError

class Department(BaseModel):
    department_id : int = Field(..., gt=0)
    department_name : str = Field(...,min_length=1,max_length=100)
    sector_id : int   = Field(..., gt=0)

class Sector(BaseModel):
    sector_id : int = Field(..., gt=0,)
    sector_name : str = Field(...,min_length=1)
    sector_code : str = Field(...)

class PosInformation(BaseModel):
    # pos_information_id : int =Field(..., gt=0)
    art_key : int = Field(...,gt=2000)
    ean : str = Field(...,)
    vat_rate : float = Field(...,gt=0,le=1)
    price_net : float = Field(...,gt=0)
    price_gross : float = Field(...,gt=0)
    
    @field_validator('ean')
    @classmethod
    def validate_ean(cls, v):
        try:
            if len(v) != 13:
                raise ValueError(f"Invalid ean format: {v}")
            return v
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Invalid ean format: {v}. Error: {str(e)}")

class Segment(BaseModel):
    segment_id : int = Field(..., gt=0)
    segment_code : str = Field(...)
    segment_name : str = Field(...,min_length=1,max_length=100)
    chief_id : str = Field(...)
    sector_id : int = Field(..., gt=0)

    @field_validator('chief_id')
    @classmethod
    def validate_chief_id(cls, v):
        try:
            if not re.match(r'^CH[0-9]{3}$', v):
                raise ValueError(f"Invalid segment id format: {v}")
            return v
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Invalid chief id format: {v}. Error: {str(e)}")

    @field_validator('segment_code')
    @classmethod
    def validate_segment_code(cls, v):
        try:
            if not re.match(r'[A-Z]+', v):
                raise ValueError(f"Invalid segment code format: {v}")
            return v
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Invalid segment code format: {v}. Error: {str(e)}")

class Chief(BaseModel):
    chief_id: str = Field(...)
    # segment_id: int = Field(..., gt=0)
    chief_first_name: str = Field(...,min_length=1,max_length=50)
    chief_last_name: str = Field(...,min_length=1,max_length=50)
    chief_email: str = Field(...,min_length=5,max_length=100)
    chief_phone: str = Field(...)
    # is_current: bool = Field(...)
    # date_start: datetime.date = Field(...)
    # date_end: Optional[datetime.date] = Field(None)

    @field_validator('chief_phone')
    @classmethod
    def validate_chief_phone(cls, v):
        try:
            regex = r'^\+?[1-9]\d{1,14}$'
            if not re.match(regex, v):
                raise ValueError(f"Invalid phone number format: {v}")
            return v
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Invalid phone number format: {v}. Error: {str(e)}")

    @field_validator('chief_id')
    @classmethod
    def validate_chief_id(cls, v):
        try:
            if not re.match(r'^CH[0-9]{3}$', v):
                raise ValueError(f"Invalid chief id format: {v}")
            return v
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Invalid chief id format: {v}. Error: {str(e)}")

    @field_validator('chief_email')
    @classmethod
    def validate_chief_email(cls, v):
        try:
            regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(regex, v):
                raise ValueError(f"Invalid email format: {v}")
            return v
        except ValueError:
            raise
        except EmailNotValidError as e:
            raise ValueError(f"Invalid email address: {v}. Error: {str(e)}")

class Product(BaseModel):
    art_key : int = Field(...,gt=2000)
    art_number : str = Field(..., min_length=1)
    art_name : str = Field(..., min_length=1)
    segment_id : int = Field(..., gt=0)
    departament_id : int = Field(..., gt=0)
    contractor_id : int = Field(..., gt=0)
    brand : str = Field(...)
    pos_information_id : int = Field(..., gt=0)
    article_codification_date : datetime.date = Field(...)

    # @field_validator('art_number')
    # @classmethod
    # def validate_art_number(cls, v):
    #     try:
    #         return v if re.match(r'^[0-9]+$', v) else ValueError(f"Invalid art number format: {v}")
    #     except Exception as e:
    #         raise ValueError(f"Invalid art number format: {v}. Error: {str(e)}")

    # @field_validator('art_key')
    # @classmethod
    # def validate_art_key(cls, v):
    #     try:
    #         return v if re.match(r'^[0-9]+$', v) else ValueError(f"Invalid art key format: {v}")
    #     except Exception as e:
    #         raise ValueError(f"Invalid art key format: {v}. Error: {str(e)}")

# contractor_id,contractor_name,contact_phone_number,contact_phone_email,address,contract_number

class Contractor(BaseModel):
    contractor_id : int = Field()
    contractor_name : str = Field(...,min_length=1,max_length=100)
    contractor_phone_number : str = Field(...)
    contractor_email_address : str = Field(...,min_length=5,max_length=100)
    contractor_address : str = Field(...,min_length=1,max_length=100)
    # contract_number : str = Field()

    @field_validator('contractor_phone_number')
    @classmethod
    def validate_contractor_phone_number(cls, v):
        try:
            regex = r'^\+?[1-9]\d{1,14}$'
            if not re.match(regex, v):
                raise ValueError(f"Invalid phone number format: {v}")
            return v
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Invalid phone number format: {v}. Error: {str(e)}")

    @field_validator('contractor_email_address')
    @classmethod
    def validate_contractor_email(cls, v):
        try:
            regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(regex, v):
                raise ValueError(f"Invalid email format: {v}")
            return v
        except ValueError:
            raise
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