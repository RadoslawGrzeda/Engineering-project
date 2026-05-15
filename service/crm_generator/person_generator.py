import json
import os
import uuid
from datetime import datetime, timedelta, date
from pathlib import Path
from random import randint, choice
from faker import Faker

from service.logger_config import correlation_id

SCHEMA_VERSION = "1.0"
SOURCE_SYSTEM = "CRM"

EMAIL_DOMAINS = [
    "gmail.com", "outlook.com", "yahoo.com", "wp.pl", "onet.pl",
    "interia.pl", "o2.pl", "icloud.com", "protonmail.com",
]

IDENTIFIER_COUNTER_FILE = Path(__file__).with_name("identifier.csv")
IDENTIFIER_FLUSH_INTERVAL = 1

FAKER_BY_COUNTRY = {
    "PL": Faker("pl_PL"),
    "DE": Faker("de_DE"),
    "CZ": Faker("cs_CZ"),
    "SK": Faker("sk_SK"),
    "UA": Faker("uk_UA"),
    "LT": Faker("lt_LT"),
}

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
    {"country_code": "UA", "country_name": "Ukraine"},
    {"country_code": "LT", "country_name": "Lithuania"},
]


LANGUAGES = [
    {"language_code": "pl", "language_name": "Polish"},
    {"language_code": "de", "language_name": "German"},
    {"language_code": "en", "language_name": "English"},
    {"language_code": "es", "language_name": "Spanish"},
    {"language_code": "uk", "language_name": "Ukrainian"},
    {"language_code": "cs", "language_name": "Czech"},
    {"language_code": "sk", "language_name": "Slovak"},
    {"language_code": "lt", "language_name": "Lithuanian"},
]

COUNTRY_LANGUAGE_MAP = {
    "PL": {"language_code": "pl", "language_name": "Polish"},
    "DE": {"language_code": "de", "language_name": "German"},
    "CZ": {"language_code": "cs", "language_name": "Czech"},
    "SK": {"language_code": "sk", "language_name": "Slovak"},
    "UA": {"language_code": "uk", "language_name": "Ukrainian"},
    "LT": {"language_code": "lt", "language_name": "Lithuanian"},
}

LANGUAGE_LEVELS = ["A1", "A2", "B1", "B2", "C1", "C2"]

LOYALTY_STATUSES = ["Bronze", "Silver", "Gold", "Platinum"]

ADDRESS_TYPES = ["home", "work", "other"]

COMMUNICATION_CODES = [
    {"code": "STORE_PROMO"},
    {"code": "ECOMMERCE"},
    {"code": "NEWSLETTER"},
    {"code": "LOYALTY_INFO"},
]


ACCOUNT_INDICATOR_TYPES = [
    {"type": "EMPLOYEE"},
    {"type": "SENIOR"},
    {"type": "KDR"},
]


def _fmt_dt(value) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


class PersonGenerator:
    def __init__(self):
        self.fake = Faker("pl_PL")
        self._identifier_path = IDENTIFIER_COUNTER_FILE
        self._identifier_flush_interval = IDENTIFIER_FLUSH_INTERVAL
        self._identifier_generated_since_flush = 0
        self._identifier_counter = self._load_identifier_counter()


    def _load_identifier_counter(self) -> int:
        raw_value = self._identifier_path.read_text(encoding="utf-8").strip()
        if not raw_value:
            return 0
        return int(raw_value)


    def _write_identifier_counter(self, value: int):
        with self._identifier_path.open("r+", encoding="utf-8") as counter_file:
            counter_file.seek(0)
            counter_file.write(str(value))
            counter_file.truncate()
            counter_file.flush()
            os.fsync(counter_file.fileno())


    def _generate_identifier_id(self) -> str:
        self._identifier_counter += 1
        self._identifier_generated_since_flush += 1

        if self._identifier_generated_since_flush >= self._identifier_flush_interval:
            self._flush_counter()

        return str(self._identifier_counter)


    def _flush_counter(self):
        self._write_identifier_counter(self._identifier_counter)
        self._identifier_generated_since_flush = 0


    def _calculate_age(self, birthdate):
        today = date.today()
        age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
        return age

    def _now_str(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _generate_account(self):
        country = choice(COUNTRIES) if randint(0, 100) > 75 else COUNTRIES[0]
        fake = FAKER_BY_COUNTRY[country["country_code"]]

        gender = choice(["M", "F"])
        if gender == "F":
            first_name = fake.first_name_female()
            last_name = fake.last_name_female()
            middle_name = fake.first_name_female() if randint(0, 100) > 85 else None
        else:
            first_name = fake.first_name_male()
            last_name = fake.last_name_male()
            middle_name = fake.first_name_male() if randint(0, 100) > 85 else None

        person_id = str(uuid.uuid4())[:13].replace("-", "")
        registration_dt = fake.date_time_between(start_date="-5y", end_date="now")
        return {
            "person_id": person_id,
            "first_name": first_name,
            "last_name": last_name,
            "middle_name": middle_name,
            "birth_date": fake.date_of_birth(minimum_age=15, maximum_age=90).isoformat(),
            "gender_code": gender,
            "country_code": country["country_code"],
            "country_name": country["country_name"],
            "civil_status_code": choice(CIVIL_STATUSES)["civil"] if randint(0, 100) > 95 else None,
            "passport_number": fake.bothify("??#######").upper() if randint(0, 100) > 95 else None,
            "registration_date": _fmt_dt(registration_dt),
            "creation_application": choice(["STORE_POS", "WEBSITE", "MOBILE_APPLICATION"]),
        }


    def _generate_loyalty(self, person_id: str, registration_date: str):
        created_dt = datetime.combine(
            self.fake.date_between(start_date="-3y", end_date="today"),
            datetime.min.time(),
        )
        updated_dt = datetime.combine(
            self.fake.date_between(start_date="-1y", end_date="today"),
            datetime.min.time(),
        )
        status = choice(["Bronze"] * 4 + ["Silver"] * 3 + ["Gold"] * 2 + ["Platinum"])

        return {
            "identifier_id": self._generate_identifier_id(),
            "person_id": person_id,
            "status_code": status,
            # "created_at": _fmt_dt(created_dt),
            # "updated_at": _fmt_dt(updated_dt),
        }


    def _generate_single_address(self, person_id: str, address_type: str, is_current: bool):
        city = self.fake.city()
        street = self.fake.street_address()
        zipcode = self.fake.zipcode()

        created_dt = datetime.combine(
            self.fake.date_between(start_date="-5y", end_date="today"),
            datetime.min.time(),
        )
        updated_dt = datetime.combine(
            self.fake.date_between(start_date="-1y", end_date="today"),
            datetime.min.time(),
        )

        return {
            "person_id": person_id,
            "address_type": address_type,
            "option_channel": is_current,
            "address_street": street,
            "address_zip_code": zipcode,
            "address_city": city,
            "country_code": "PL",
            "geo_coordinates_x_value": None,
            "geo_coordinates_y_value": None,
            # "created_at": _fmt_dt(created_dt),
            # "updated_at": _fmt_dt(updated_dt),
        }

    def _generate_address_channels(self, person_id: str):
        roll = randint(0, 100)

        if roll < 5:
            return []

        if roll < 30:
            return [self._generate_single_address(person_id, "home", is_current=True)]

        if roll < 70:
            return [
                self._generate_single_address(person_id, "home", is_current=True),
                self._generate_single_address(person_id, "work", is_current=True),
            ]

        return [
            self._generate_single_address(person_id, "home", is_current=True),
            self._generate_single_address(person_id, "work", is_current=True),
            self._generate_single_address(person_id, "other", is_current=False),
        ]




    def _generate_phone(self) -> str:
        prefix = choice(['50', '51', '53', '57', '60', '66', '69', '72', '73', '78', '79', '88'])
        number = self.fake.numerify(prefix + '#######')
        return f'+48 {number[:3]} {number[3:6]} {number[6:]}'

    def _generate_contact_channels(self, person_id: str):
        channels = []

        created_dt_email = datetime.combine(
            self.fake.date_between(start_date="-5y", end_date="today"),
            datetime.min.time(),
        )
        updated_dt_email = datetime.combine(
            self.fake.date_between(start_date="-1y", end_date="today"),
            datetime.min.time(),
        )

        channels.append({
            "person_id": person_id,
            "contact_type": "email",
            "value": self.fake.email(),
            "flag_main_type": randint(0, 100) < 80,
            "preferred_channel": randint(0, 100) < 70,
            "option_channel": True,
            "flag_valid": randint(0, 100) < 90,
            # "created_at": _fmt_dt(created_dt_email),
            # "updated_at": _fmt_dt(updated_dt_email),
        })

        created_dt_phone = datetime.combine(
            self.fake.date_between(start_date="-5y", end_date="today"),
            datetime.min.time(),
        )
        updated_dt_phone = datetime.combine(
            self.fake.date_between(start_date="-1y", end_date="today"),
            datetime.min.time(),
        )

        channels.append({
            "person_id": person_id,
            "contact_type": "phone",
            "value": self._generate_phone(),
            "flag_main_type": randint(0, 100) < 80,
            "preferred_channel": randint(0, 100) < 40,
            "option_channel": True,
            "flag_valid": randint(0, 100) < 85,
            # "created_at": _fmt_dt(created_dt_phone),
            # "updated_at": _fmt_dt(updated_dt_phone),
        })

        return channels

    def _generate_communication_subscriptions(self, person_id: str):
        subscriptions = []
        for comm in COMMUNICATION_CODES:
            if randint(0, 100) < 60:
                sub_date = self.fake.date_between(start_date="-3y", end_date="today")
                is_unsubscribed = randint(0, 100) < 15
                updated_dt = datetime.combine(
                    self.fake.date_between(start_date="-1y", end_date="today"),
                    datetime.min.time(),
                )
                subscriptions.append({
                    "person_id": person_id,
                    "communication_code": comm["code"],
                    "value": "AIV_02" if is_unsubscribed else "AIV_01",
                    "date_of_subscription": _fmt_dt(datetime.combine(sub_date, datetime.min.time())),
                    "date_of_unsubscription": _fmt_dt(datetime.combine(sub_date + timedelta(days=randint(30, 365)), datetime.min.time())) if is_unsubscribed else None,
                    "reason_of_unsubscription": choice(["spam", "not_interested", "too_frequent"]) if is_unsubscribed else None,
                    "updated_at": _fmt_dt(updated_dt),
                })
        return subscriptions



    def _generate_digital_access(self, person_id: str, email: str):
        is_active = randint(0, 100) < 90

        created_dt = datetime.combine(
            self.fake.date_between(start_date="-5y", end_date="today"),
            datetime.min.time(),
        )
        updated_dt = datetime.combine(
            self.fake.date_between(start_date="-1y", end_date="today"),
            datetime.min.time(),
        )

        return {
            "person_id": person_id,
            "username": self.fake.user_name(),
            "email_user": email,
            "is_active": is_active,
            "last_login_at": self.fake.date_time_between(start_date="-30d", end_date="now").strftime("%Y-%m-%d %H:%M:%S") if is_active else None,
            "portal_user_confirmation_at": self.fake.date_time_between(start_date="-5y", end_date="now").strftime("%Y-%m-%d %H:%M:%S") if is_active else None,
            # "created_at": _fmt_dt(created_dt),
            # "updated_at": _fmt_dt(updated_dt),
        }

    def _get_age(self, date_of_birth: str) -> int:
        today = date.today()
        dob = datetime.strptime(date_of_birth, "%Y-%m-%d").date()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

    def _generate_account_indicators(self, person_id: str, date_of_birth: str, civil_status) -> dict:
        age = self._get_age(date_of_birth)

        is_senior = randint(0, 100) < 50 if age >= 60 else None
        is_kdf = randint(0, 100) < 50 if civil_status == "married" else None

        if is_senior or is_kdf:
            indicator_type = 'KDR' if is_kdf else 'SENIOR'
            is_active = True
        elif randint(0, 100) > 80:
            indicator_type = 'EMPLOYEE'
            is_active = randint(0, 100) < 80
        else:
            return None

        created_dt = datetime.combine(
            self.fake.date_between(start_date="-5y", end_date="today"),
            datetime.min.time(),
        )
        updated_dt = datetime.combine(
            self.fake.date_between(start_date="-1y", end_date="today"),
            datetime.min.time(),
        )
        return {
            "person_id": person_id,
            "type": indicator_type,
            "is_active": is_active,
            # "created_at": _fmt_dt(created_dt),
            # "updated_at": _fmt_dt(updated_dt),
        }

    def _generate_nationalities(self, person_id: str, primary_country_code: str):
        nationalities = [{"person_id": person_id, "country_code": primary_country_code}]
        if randint(0, 100) < 15:
            extra = choice([c for c in COUNTRIES if c["country_code"] != primary_country_code])
            nationalities.append({"person_id": person_id, "country_code": extra["country_code"]})
        return nationalities

    def _generate_languages(self, person_id: str, nationalities: list):
        country_codes = [n["country_code"] for n in nationalities]

        native_lang = COUNTRY_LANGUAGE_MAP[country_codes[0]]
        seen_codes = {native_lang["language_code"]}
        langs = [{
            "person_id": person_id,
            "language_code": native_lang["language_code"],
            "language_level": "C2",
        }]

        if native_lang["language_code"] != "pl":
            seen_codes.add("pl")
            langs.append({
                "person_id": person_id,
                "language_code": "pl",
                "language_level": choice(["A1", "A2", "B1", "B2", "C1"]),
            })

        for cc in country_codes[1:]:
            lang = COUNTRY_LANGUAGE_MAP[cc]
            if lang["language_code"] not in seen_codes:
                seen_codes.add(lang["language_code"])
                langs.append({
                    "person_id": person_id,
                    "language_code": lang["language_code"],
                    "language_level": choice(["B2", "C1", "C2"]),
                })

        if randint(0, 100) < 50:
            extra_pool = [l for l in LANGUAGES if l["language_code"] not in seen_codes]
            if extra_pool:
                extra = choice(extra_pool)
                seen_codes.add(extra["language_code"])
                langs.append({
                    "person_id": person_id,
                    "language_code": extra["language_code"],
                    "language_level": choice(LANGUAGE_LEVELS),
                })

        return langs

    def generate_customer(self):
        correlation_id.set(str(uuid.uuid4())[:8])
        account = self._generate_account()
        account['correlation_id'] = correlation_id.get()
        person_id = account["person_id"]
        registration_date = account["registration_date"]
        date_of_birth = account["birth_date"]
        civil_status = account["civil_status_code"]
        country_code = account["country_code"]

        contact_channels = self._generate_contact_channels(person_id)
        email = next((ch["value"] for ch in contact_channels if ch["contact_type"] == "email"), None)

        nationalities = self._generate_nationalities(person_id, country_code)

        return {
            "account": account,
            "loyalty": self._generate_loyalty(person_id, registration_date),
            "nationalities": nationalities,
            "address_channels": self._generate_address_channels(person_id),
            "contact_channels": contact_channels,
            "communication_subscriptions": self._generate_communication_subscriptions(person_id),
            "digital_access": self._generate_digital_access(person_id, email),
            "account_indicators": self._generate_account_indicators(person_id, date_of_birth, civil_status),
            "languages": self._generate_languages(person_id, nationalities),
        }


if __name__ == "__main__":
    crm = PersonGenerator()
    customer = crm.generate_customer()
    print(json.dumps(customer, indent=2, ensure_ascii=False))
