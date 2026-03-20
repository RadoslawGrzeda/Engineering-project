import json
import uuid
from datetime import datetime, timedelta, date
from random import randint, choice, uniform

from faker import Faker


GENDERS = [
    {"gender_code": "M", "gender_name": "Male"},
    {"gender_code": "F", "gender_name": "Female"},
]

CIVIL_STATUSES = [
    {"civil": "single", "is_current": True},
    {"civil": "married", "is_current": True},
    {"civil": "divorced", "is_current": True},
    {"civil": "widowed", "is_current": True},
]

COUNTRIES = [
    {"country_code": "PL", "country_name": "Poland"},
    {"country_code": "DE", "country_name": "Germany"},
    {"country_code": "CZ", "country_name": "Czech Republic"},
    {"country_code": "SK", "country_name": "Slovakia"},
    {"country_code": "UA", "country_name": "Ucraine"},
    {"country_code": "LT", "country_name": "Lithuania"},
]

LANGUAGES = [
    {"language_code": "pl", "language_name": "Polish"},
    {"language_code": "de", "language_name": "Germany"},
    {"language_code": "en", "language_name": "English"},
    {"language_code": "es", "language_name": "Spanish"},
    {"language_code": "uk", "language_name": "Ukraine"},
]

LANGUAGE_LEVELS = ["A1", "A2", "B1", "B2", "C1", "C2"]

LOYALTY_STATUSES = ["Bronze", "Silver", "Gold", "Platinum"]

CHANNEL_TYPES = ["email", "sms", "phone", "push"]

COMMUNICATION_CODES = [
    {"code": "STORE_PROMO", "value": "Promotion in local store"},
    {"code": "ECOMMERCE", "value": "E-commerce"},
    {"code": "NEWSLETTER", "value": "Newsletter"},
    {"code": "LOYALTY_INFO", "value": "Loyalty information"},
]

ACCOUNT_INDICATOR_TYPES = [
    # {"type": "VIP", "value": "true"},
    {"type": "EMPLOYEE", "value": "false"},
    {"type": "SENIOR", "value": "true"},
    {"type": "KDR", "value": "true"},
]


class CrmGenerator:
    def __init__(self):
        self.fake = Faker("pl_PL")


    def _generate_account(self):
        first_name = self.fake.first_name()
        gender = "F" if first_name.lower().endswith("a") else "M"
        person_id = str(uuid.uuid4())[:8]
        registration_date = self.fake.date_between(start_date="-5y", end_date="today")

        return {
            "person_id": person_id,
            # "person_ty": choice(["fizyczna", "prawna"]) if randint(0, 100) > 95 else "fizyczna",
            "first_name": first_name,
            "last_name": self.fake.last_name_female() if gender == "F" else self.fake.last_name(),
            "middle_name": (self.fake.first_name_female() if gender == "F" else self.fake.first_name_male()) if randint(0, 100) > 95 else None,
            "birth_date": self.fake.date_of_birth(minimum_age=15, maximum_age=90).isoformat(),
            "gender_code": gender,
            "civil_status": choice(CIVIL_STATUSES)["civil"] if randint(0, 100) > 95 else None,
            "passport_number": self.fake.bothify("??#######").upper() if randint(0, 100) > 95 else None,
            "registration_date": registration_date.isoformat(),
            "creation_application": choice(["STORE_POS", "WEBSITE", "MOBILE_APPLICATION"]),
        }


    def _generate_loyalty(self, person_id: str, registration_date: str):
        start_date = datetime.fromisoformat(registration_date)
        end_date = start_date + timedelta(days=365)

        return {
            "identifier_id": str(uuid.uuid4()),
            "person_id": person_id,
            "loyalty_status": 'Silver',
            "start_date": start_date.date().isoformat(),
            "end_date": end_date.date().isoformat(),
        }


    def _generate_address_channel(self, person_id: str):
        city = self.fake.city()
        street = self.fake.street_address()
        zipcode = self.fake.zipcode()
        country = choice(COUNTRIES)

        return {
            "channel_id": str(uuid.uuid4()),
            "person_id": person_id,
            "channel_type": "address",
            "value": f"{street}, {zipcode} {city}, Polska",
            "flag_main_type": randint(0, 100) < 80,
            "preferred_channel": randint(0, 100) < 60,
            "address_address": street,
            "address_zip_code": zipcode,
            'address_code':'PL',
            "address_city": city,
            # "address_x_position": round(uniform(49.0, 54.8), 6),
            # "address_y_position": round(uniform(14.1, 24.1), 6),
            "option_channel": choice(["work", "other"]) if randint(0, 100) > 85 else "home",
            "flag_valid": randint(0, 100) < 90,
            # "source": choice(["STORE_POS", "ECOMMERCE", "MOBILE_APP", "MANUAL"]),
            "created_date": self.fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "last_modified_date": self.fake.date_between(start_date="-1y", end_date="today").isoformat(),
            "is_deleted": False,
        }


    def _generate_contact_channels(self, person_id: str):
        channels = []

        channels.append({
            "channel_id": str(uuid.uuid4()),
            "person_id": person_id,
            "channel_type": "email",
            "value": self.fake.email(),
            "flag_main_type": randint(0, 100) < 80,
            "preferred_channel": randint(0, 100) < 70,
            "option_channel": choice(["personal", "work"]),
            "flag_valid": randint(0, 100) < 90,
            # "source": choice(["STORE_POS", "ECOMMERCE", "MOBILE_APP"]),
            "created_date": self.fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "last_modified_date": self.fake.date_between(start_date="-1y", end_date="today").isoformat(),
            "is_deleted": False,
        })

        channels.append({
            "channel_id": str(uuid.uuid4()),
            "person_id": person_id,
            "channel_type": "phone",
            "value": self.fake.phone_number(),
            "flag_main_type": randint(0, 100) < 80,
            "preferred_channel": randint(0, 100) < 40,
            "option_channel": choice(["mobile", "landline"]),
            "flag_valid": randint(0, 100) < 85,
            # "source": choice(["STORE_POS", "ECOMMERCE", "MOBILE_APP"]),
            "created_date": self.fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "last_modified_date": self.fake.date_between(start_date="-1y", end_date="today").isoformat(),
            "is_deleted": False,
        })

        if randint(0, 100) > 70:
            channels.append({
                "channel_id": str(uuid.uuid4()),
                "person_id": person_id,
                "channel_type": choice(["email", "phone"]),
                "value": self.fake.email() if randint(0, 1) else self.fake.phone_number(),
                "flag_main_type": False,
                "preferred_channel": False,
                "option_channel": "work",
                "flag_valid": randint(0, 100) < 70,
                # "source": choice(["STORE_POS", "ECOMMERCE", "MOBILE_APP"]),
                "created_date": self.fake.date_between(start_date="-3y", end_date="today").isoformat(),
                "last_modified_date": self.fake.date_between(start_date="-1y", end_date="today").isoformat(),
                "is_deleted": False,
            })

        return channels

    def _generate_communication_subscriptions(self, person_id: str):
        subscriptions = []
        for comm in COMMUNICATION_CODES:
            if randint(0, 100) < 60:
                sub_date = self.fake.date_between(start_date="-3y", end_date="today")
                is_unsubscribed = randint(0, 100) < 15
                subscriptions.append({
                    "communication_id": str(uuid.uuid4()),
                    "person_id": person_id,
                    "community_code": comm["code"],
                    "community_code_value": comm["value"],
                    "date_of_subscription": sub_date.isoformat(),
                    "date_of_unsubscription": (sub_date + timedelta(days=randint(30, 365))).isoformat() if is_unsubscribed else None,
                    "reason_of_unsubscription": choice(["spam", "not_interested", "too_frequent"]) if is_unsubscribed else None,
                    "last_modified_date": self.fake.date_between(start_date="-1y", end_date="today").isoformat(),
                })
        return subscriptions



    def _generate_digital_access(self, person_id: str, email: str):
        is_active = randint(0, 100) < 80

        return {
            "id": str(uuid.uuid4()),
            "person_id": person_id,
            "username": self.fake.user_name() if is_active else None,
            "email_user": email,
            "is_active": is_active,
            "last_login_date": self.fake.date_between(start_date="-30d", end_date="today").isoformat() if is_active else None,
            "created_date": self.fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "portal_user_confirmation_date": self.fake.date_between(start_date="-5y", end_date="today").isoformat() if is_active else None,
            'preferred_delivery_method':choice(['Courier','Parcel locker','Personal collection']) if is_active and randint(0,100)>70 else None,
        }

    def _get_age(self,date_of_birth:datetime) -> int:
        today = date.today()
        date_of_birth = datetime.strptime(date_of_birth, "%Y-%m-%d").date()
        age=today.year-date_of_birth.year - ((today.month, today.day) < (today.month, today.day))
        return age

    def _generate_account_indicators(self, person_id: str,date_of_birth: datetime,civil_status) -> dict:
        # indicators = []
        # for ind in ACCOUNT_INDICATOR_TYPES:
        #     if randint(0, 100) < 40:
        #         indicators.append({
        age=self._get_age(date_of_birth)

        # if age >=60:
        is_senior = randint(0, 100) < 50 if age>=60 else None

        is_kdf=randint(0, 100) < 50 if civil_status == "married" else None

        if is_senior or is_kdf:
            return {
                "id": str(uuid.uuid4()),
                "person_id": person_id,
                "type_account_indicator": 'KDR' if is_kdf else 'SENIOR',
                "value_account_indicator": 'ACTIVE',
                "last_modified_date": self.fake.date_between(start_date="-1y", end_date="today").isoformat(),
                "is_deleted": False,
                "created_date": self.fake.date_between(start_date="-5y", end_date="today").isoformat(),
            }
        if randint(0, 100) >80:
            return {
                "id": str(uuid.uuid4()),
                "person_id": person_id,
                "type_account_indicator": 'EMPLOYEE',
                "value_account_indicator": 'ACTIVE' if randint(0, 100) < 80 else 'NON_ACTIVE',
                "last_modified_date": self.fake.date_between(start_date="-1y", end_date="today").isoformat(),
                "is_deleted": False,
                "created_date": self.fake.date_between(start_date="-5y", end_date="today").isoformat(),
            }
        return None

        # return = person
        #             "id": str(uuid.uuid4()),
        #             "person_id": person_id,
        #             "type_account_indicator": ind["type"],
        #             "value_account_indicator": ind["value"],
        #             "last_modified_date": self.fake.date_between(start_date="-1y", end_date="today").isoformat(),
        #             "is_deleted": False,
        #             "created_date": self.fake.date_between(start_date="-5y", end_date="today").isoformat(),
        #         })
        # return indicators


    def _generate_languages(self, person_id: str):
        langs = [LANGUAGES[0]]
        if randint(0, 100) < 50:
            extra = choice(LANGUAGES[1:])
            langs.append(extra)

        return [
            {
                "id": str(uuid.uuid4()),
                "person_id": person_id,
                "language_code": lang["language_code"],
                "language_name": lang["language_name"],
                "language_level": "C2" if lang["language_code"] == "pl" else choice(LANGUAGE_LEVELS),
            }
            for lang in langs
        ]

    def generate_customer(self):
        account = self._generate_account()
        person_id = account["person_id"]
        registration_date = account["registration_date"]
        date_of_birth = account["birth_date"]
        civil_status = account["civil_status"]

        contact_channels = self._generate_contact_channels(person_id)
        email = next((ch["value"] for ch in contact_channels if ch["channel_type"] == "email"), None)

        return {
            "account": account,
            "loyalty": self._generate_loyalty(person_id, registration_date),
            "address_channels": [self._generate_address_channel(person_id)],
            "contact_channels": contact_channels,
            "communication_subscriptions": self._generate_communication_subscriptions(person_id),
            "digital_access": self._generate_digital_access(person_id, email),
            "account_indicators": self._generate_account_indicators(person_id,date_of_birth,civil_status),
            "languages": self._generate_languages(person_id),
        }


if __name__ == "__main__":
    crm = CrmGenerator()
    customer = crm.generate_customer()
    print(json.dumps(customer, indent=2, ensure_ascii=False))