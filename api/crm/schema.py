from datetime import datetime, date
from typing import Any

from pydantic import BaseModel, ConfigDict


class DeadLetterBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    error_code: str | None = None
    error_message: str | None = None
    status: str = "NEW"
    person_id: str | None = None
    correlation_id: str | None = None
    source_application: str | None = None
    raw_payload: dict[str, Any]
    retry_count: int = 0
    inserted_at: datetime | None = None


class DeadLetterUpdate(BaseModel):
    raw_payload: dict[str, Any]


# --- AddUser request schemas ---

class AccountIn(BaseModel):
    person_id: str
    first_name: str
    last_name: str
    middle_name: str | None = None
    birth_date: date | None = None
    gender_code: str | None = None
    country_code: str | None = None
    country_name: str | None = None
    civil_status: str | None = None
    passport_number: str | None = None
    registration_date: date
    creation_application: str | None = None
    correlation_id: str | None = None


class LoyaltyIn(BaseModel):
    identifier_id: str
    person_id: str
    loyalty_status: str
    start_date: date
    end_date: date | None = None


class NationalityIn(BaseModel):
    person_id: str
    country_code: str


class AddressChannelIn(BaseModel):
    channel_id: str | None = None
    person_id: str
    channel_type: str | None = None
    value: str | None = None
    flag_main_type: bool = False
    preferred_channel: bool = False
    address_address: str | None = None
    address_zip_code: str | None = None
    address_code: str | None = None
    address_city: str | None = None
    option_channel: str | None = None
    flag_valid: bool = True
    created_date: datetime | None = None
    last_modified_date: datetime | None = None
    is_deleted: bool = False


class ContactChannelIn(BaseModel):
    channel_id: str | None = None
    person_id: str
    channel_type: str
    value: str
    flag_main_type: bool = False
    preferred_channel: bool = False
    option_channel: str | None = None
    flag_valid: bool = True
    created_date: datetime | None = None
    last_modified_date: datetime | None = None
    is_deleted: bool = False


class CommunicationSubscriptionIn(BaseModel):
    communication_id: str | None = None
    person_id: str
    community_code: str
    community_code_value: str | None = None
    date_of_subscription: datetime | None = None
    date_of_unsubscription: datetime | None = None
    reason_of_unsubscription: str | None = None
    last_modified_date: datetime | None = None


class DigitalAccessIn(BaseModel):
    id: str | None = None
    person_id: str
    username: str | None = None
    email_user: str | None = None
    is_active: bool = True
    last_login_date: datetime | None = None
    created_date: datetime | None = None
    portal_user_confirmation_date: datetime | None = None
    preferred_delivery_method: str | None = None


class AccountIndicatorIn(BaseModel):
    person_id: str
    type: str
    is_active: bool = True


class LanguageIn(BaseModel):
    id: str | None = None
    person_id: str
    language_code: str
    language_name: str | None = None
    language_level: str | None = None


class AddUserRequest(BaseModel):
    account: AccountIn
    loyalty: LoyaltyIn | None = None
    nationalities: list[NationalityIn] | None = None
    address_channels: list[AddressChannelIn] | None = None
    contact_channels: list[ContactChannelIn] | None = None
    communication_subscriptions: list[CommunicationSubscriptionIn] | None = None
    digital_access: DigitalAccessIn | None = None
    account_indicators: list[AccountIndicatorIn] | None = None
    languages: list[LanguageIn] | None = None