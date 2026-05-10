import json
import uuid
from datetime import datetime, timedelta, date
from pathlib import Path
from random import randint, choice, uniform
from faker import Faker

from apps.logger_config import correlation_id

IDENTIFIER_COUNTER_FILE = Path(__file__).with_name("identifier.csv")
IDENTIFIER_FLUSH_INTERVAL = 100

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
        self._identifier_path.write_text(str(value), encoding="utf-8")


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
        registration_date = fake.date_between(start_date="-5y", end_date="today")
        return {
            "person_id": person_id,
            "first_name": first_name,
            "last_name": last_name,
            "middle_name": middle_name,
            "birth_date": fake.date_of_birth(minimum_age=15, maximum_age=90).isoformat(),
            "gender_code": gender,
            "country_code": country["country_code"],
            "country_name": country["country_name"],
            "civil_status": choice(CIVIL_STATUSES)["civil"] if randint(0, 100) > 95 else None,
            "passport_number": fake.bothify("??#######").upper() if randint(0, 100) > 95 else None,
            "registration_date": registration_date.isoformat(),
            "creation_application": choice(["STORE_POS", "WEBSITE", "MOBILE_APPLICATION"]),
        }


    def _generate_loyalty(self, person_id: str, registration_date: str):
        start_date = datetime.fromisoformat(registration_date)

        status = choice(["Bronze"] * 4 + ["Silver"] * 3 + ["Gold"] * 2 + ["Platinum"])

        if randint(0, 100) < 80:
            end_date = None
        else:
            days_active = randint(30, (date.today() - start_date.date()).days or 30)
            end_date = (start_date + timedelta(days=days_active)).date().isoformat()

        return {
            "identifier_id": self._generate_identifier_id(),
            "person_id": person_id,
            "loyalty_status": status,
            "start_date": start_date.date().isoformat(),
            "end_date": end_date,
        }


    def _generate_single_address(self, person_id: str, option_channel: str, is_current: bool):
        city = self.fake.city()
        street = self.fake.street_address()
        zipcode = self.fake.zipcode()

        return {
            "channel_id": str(uuid.uuid4()),
            "person_id": person_id,
            "channel_type": "address",
            "value": f"{street}, {zipcode} {city}",
            "flag_main_type": option_channel == "home",
            "preferred_channel": option_channel == "home",
            "address_address": street,
            "address_zip_code": zipcode,
            "address_code": "PL",
            "address_city": city,
            "option_channel": option_channel,
            "flag_valid": is_current and randint(0, 100) < 90,
            "created_date": datetime.combine(self.fake.date_between(start_date="-5y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
            "last_modified_date": datetime.combine(self.fake.date_between(start_date="-1y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
            "is_deleted": not is_current,
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

        addresses = [
            self._generate_single_address(person_id, "home", is_current=True),
            self._generate_single_address(person_id, "work", is_current=True),
            self._generate_single_address(person_id, choice(["home", "other"]), is_current=False),
        ]
        return addresses





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
            "created_date": datetime.combine(self.fake.date_between(start_date="-5y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
            "last_modified_date": datetime.combine(self.fake.date_between(start_date="-1y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
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
            "created_date": datetime.combine(self.fake.date_between(start_date="-5y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
            "last_modified_date": datetime.combine(self.fake.date_between(start_date="-1y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
            "is_deleted": False,
        })

        if randint(0, 100) > 70:
            channel_type = choice(["email", "phone"])
            channels.append({
                "channel_id": str(uuid.uuid4()),
                "person_id": person_id,
                "channel_type": channel_type,
                "value": self.fake.email() if channel_type == "email" else self.fake.phone_number(),
                "flag_main_type": False,
                "preferred_channel": False,
                "option_channel": "work",
                "flag_valid": randint(0, 100) < 70,
                # "source": choice(["STORE_POS", "ECOMMERCE", "MOBILE_APP"]),
                "created_date": datetime.combine(self.fake.date_between(start_date="-3y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
                "last_modified_date": datetime.combine(self.fake.date_between(start_date="-1y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
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
                    "date_of_subscription": datetime.combine(sub_date, datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
                    "date_of_unsubscription": datetime.combine(sub_date + timedelta(days=randint(30, 365)), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S") if is_unsubscribed else None,
                    "reason_of_unsubscription": choice(["spam", "not_interested", "too_frequent"]) if is_unsubscribed else None,
                    "last_modified_date": datetime.combine(self.fake.date_between(start_date="-1y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
                })
        return subscriptions



    def _generate_digital_access(self, person_id: str, email: str):
        is_active = randint(0, 100) < 90

        return {
            "id": str(uuid.uuid4()),
            "person_id": person_id,
            "username": self.fake.user_name(),
            "email_user": email,
            "is_active": is_active,
            "last_login_date": datetime.combine(self.fake.date_between(start_date="-30d", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S") if is_active else None,
            "created_date": datetime.combine(self.fake.date_between(start_date="-5y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
            "portal_user_confirmation_date": datetime.combine(self.fake.date_between(start_date="-5y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S") if is_active else None,
            'preferred_delivery_method':choice(['Courier','Parcel locker','Personal collection']) if is_active and randint(0,100)>70 else None,
        }

    def _get_age(self, date_of_birth: str) -> int:
        today = date.today()
        dob = datetime.strptime(date_of_birth, "%Y-%m-%d").date()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

    def _generate_account_indicators(self, person_id: str,date_of_birth: datetime,civil_status) -> dict:
        age=self._get_age(date_of_birth)

        is_senior = randint(0, 100) < 50 if age>=60 else None

        is_kdf=randint(0, 100) < 50 if civil_status == "married" else None

        if is_senior or is_kdf:
            return {
                "id": str(uuid.uuid4()),
                "person_id": person_id,
                "type_account_indicator": 'KDR' if is_kdf else 'SENIOR',
                "value_account_indicator": 'ACTIVE',
                "last_modified_date": datetime.combine(self.fake.date_between(start_date="-1y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
                "is_deleted": False,
                "created_date": datetime.combine(self.fake.date_between(start_date="-5y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
            }
        if randint(0, 100) >80:
            return {
                "id": str(uuid.uuid4()),
                "person_id": person_id,
                "type_account_indicator": 'EMPLOYEE',
                "value_account_indicator": 'ACTIVE' if randint(0, 100) < 80 else 'NON_ACTIVE',
                "last_modified_date": datetime.combine(self.fake.date_between(start_date="-1y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
                "is_deleted": False,
                "created_date": datetime.combine(self.fake.date_between(start_date="-5y", end_date="today"), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
            }
        return None

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
            "id": str(uuid.uuid4()),
            "person_id": person_id,
            "language_code": native_lang["language_code"],
            "language_name": native_lang["language_name"],
            "language_level": "C2",
        }]

        if native_lang["language_code"] != "pl":
            seen_codes.add("pl")
            langs.append({
                "id": str(uuid.uuid4()),
                "person_id": person_id,
                "language_code": "pl",
                "language_name": "Polish",
                "language_level": choice(["A1", "A2", "B1", "B2", "C1"]),
            })

        for cc in country_codes[1:]:
            lang = COUNTRY_LANGUAGE_MAP[cc]
            if lang["language_code"] not in seen_codes:
                seen_codes.add(lang["language_code"])
                langs.append({
                    "id": str(uuid.uuid4()),
                    "person_id": person_id,
                    "language_code": lang["language_code"],
                    "language_name": lang["language_name"],
                    "language_level": choice(["B2", "C1", "C2"]),
                })

        if randint(0, 100) < 50:
            extra_pool = [l for l in LANGUAGES if l["language_code"] not in seen_codes]
            if extra_pool:
                extra = choice(extra_pool)
                seen_codes.add(extra["language_code"])
                langs.append({
                    "id": str(uuid.uuid4()),
                    "person_id": person_id,
                    "language_code": extra["language_code"],
                    "language_name": extra["language_name"],
                    "language_level": choice(LANGUAGE_LEVELS),
                })

        return langs

    def generate_customer(self):
        correlation_id.set(str(uuid.uuid4())[:8])
        account = self._generate_account()
        account['correlation_id']=correlation_id.get()
        person_id = account["person_id"]
        registration_date = account["registration_date"]
        date_of_birth = account["birth_date"]
        civil_status = account["civil_status"]
        country_code = account["country_code"]

        contact_channels = self._generate_contact_channels(person_id)
        email = next((ch["value"] for ch in contact_channels if ch["channel_type"] == "email"), None)

        nationalities = self._generate_nationalities(person_id, country_code)

        return {
            # "correlation_id": correlation_id.get(),
            "account": account,
            "loyalty": self._generate_loyalty(person_id, registration_date),
            "nationalities": nationalities,
            "address_channels": self._generate_address_channels(person_id),
            "contact_channels": contact_channels,
            "communication_subscriptions": self._generate_communication_subscriptions(person_id),
            "digital_access": self._generate_digital_access(person_id, email),
            "account_indicators": self._generate_account_indicators(person_id,date_of_birth,civil_status),
            "languages": self._generate_languages(person_id, nationalities),
        }


if __name__ == "__main__":
    crm = PersonGenerator()
    customer = crm.generate_customer()
    print(json.dumps(customer, indent=2, ensure_ascii=False))