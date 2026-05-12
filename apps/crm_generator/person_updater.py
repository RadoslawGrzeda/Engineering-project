import json
import os
import uuid
from datetime import datetime, timedelta, timezone, date
from random import randint, choice

import psycopg2
import psycopg2.extras
import psycopg2.pool
from dotenv import load_dotenv
from faker import Faker

from apps.logger_config import correlation_id
from apps.crm_generator.person_generator import (
    SCHEMA_VERSION,
    SOURCE_SYSTEM,
    LOYALTY_STATUSES,
    CIVIL_STATUSES,
    COMMUNICATION_CODES,
    EMAIL_DOMAINS,
    LANGUAGES,
    LANGUAGE_LEVELS,
)

load_dotenv()

UPDATE_ACTIONS = [
    "contact", "address", "loyalty", "subscription", "civil",
    "contact_add", "address_add", "subscription_add",
    "indicator_add", "language_add", "resubscribe",
]

ADDRESS_TYPES = ["home", "work", "other"]

SELECT_RANDOM_PERSON = """
    SELECT person_id FROM client.customer
    WHERE person_id IS NOT NULL
    ORDER BY random() LIMIT 1
"""
SELECT_CUSTOMER = """
    SELECT c.*, dc.country_name
    FROM client.customer c
    LEFT JOIN client.nationality n ON n.person_id = c.person_id
    LEFT JOIN client.dict_country dc ON dc.country_code = n.country_code
    WHERE c.person_id = %s
    LIMIT 1
"""
SELECT_LOYALTY = """
    SELECT * FROM client.loyalty_status
    WHERE person_id = %s ORDER BY created_at DESC LIMIT 1
"""
SELECT_LANGUAGES = "SELECT * FROM client.language WHERE person_id = %s"
SELECT_NATIONALITIES = "SELECT * FROM client.nationality WHERE person_id = %s"
SELECT_INDICATORS = "SELECT * FROM client.customer_indicator WHERE person_id = %s LIMIT 1"
SELECT_ADDRESSES = "SELECT * FROM client.address WHERE person_id = %s"
SELECT_CONTACTS = "SELECT * FROM client.contact WHERE person_id = %s"
SELECT_SUBSCRIPTIONS = "SELECT * FROM client.communication_subscription WHERE person_id = %s"
SELECT_DIGITAL = "SELECT * FROM client.digital_access WHERE person_id = %s LIMIT 1"


def _fmt_dt(value):
    if value is None:
        return None
    if isinstance(value, (datetime,)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _fmt_date(value):
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


class PersonUpdater:

    def __init__(self):
        conn_str = os.getenv("POSTGRES_CONNECTION")
        if not conn_str:
            raise RuntimeError("POSTGRES_CONNECTION env var is required")
        self.pool = psycopg2.pool.SimpleConnectionPool(1, 3, conn_str)
        self.fake = Faker("pl_PL")

    def _get_conn(self):
        conn = self.pool.getconn()
        conn.autocommit = True
        return conn

    def _put_conn(self, conn):
        self.pool.putconn(conn)

    def _select_random_person_id(self, cur):
        cur.execute(SELECT_RANDOM_PERSON)
        row = cur.fetchone()
        return row["person_id"] if row else None

    def _fetch_customer(self, person_id: str):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(SELECT_CUSTOMER, (person_id,))
                customer = cur.fetchone()
                if not customer:
                    return None

                cur.execute(SELECT_LOYALTY, (person_id,))
                loyalty = cur.fetchone()

                cur.execute(SELECT_LANGUAGES, (person_id,))
                languages = cur.fetchall()

                cur.execute(SELECT_NATIONALITIES, (person_id,))
                nationalities = cur.fetchall()

                cur.execute(SELECT_INDICATORS, (person_id,))
                indicator = cur.fetchone()

                cur.execute(SELECT_ADDRESSES, (person_id,))
                addresses = cur.fetchall()

                cur.execute(SELECT_CONTACTS, (person_id,))
                contacts = cur.fetchall()

                cur.execute(SELECT_SUBSCRIPTIONS, (person_id,))
                subs = cur.fetchall()

                cur.execute(SELECT_DIGITAL, (person_id,))
                digital = cur.fetchone()
        finally:
            self._put_conn(conn)

        return self._build_payload(
            customer, loyalty, languages, nationalities, indicator,
            addresses, contacts, subs, digital,
        )

    def _build_payload(self, customer, loyalty, languages, nationalities, indicator,
                       addresses, contacts, subs, digital):
        person_id = customer["person_id"]

        account = {
            "person_id": person_id,
            "first_name": customer["first_name"],
            "last_name": customer["last_name"],
            "middle_name": customer.get("middle_name"),
            "birth_date": _fmt_date(customer.get("birth_date")),
            "gender_code": customer.get("gender_code"),
            "country_code": (nationalities[0]["country_code"] if nationalities else "PL"),
            "country_name": customer.get("country_name") or "Poland",
            "civil_status_code": customer.get("civil_status_code"),
            "passport_number": customer.get("passport_number"),
            "registration_date": _fmt_dt(customer.get("registration_date")),
            "creation_application": customer.get("creation_application"),
        }

        loyalty_payload = None
        if loyalty:
            loyalty_payload = {
                "identifier_id": loyalty["identifier_id"],
                "person_id": person_id,
                "status_code": loyalty["status_code"],
                "created_at": _fmt_dt(loyalty.get("created_at")),
                "updated_at": _fmt_dt(loyalty.get("updated_at")),
            }

        address_channels = []
        for a in addresses:
            address_channels.append({
                "person_id": person_id,
                "address_type": a.get("address_type"),
                "option_channel": a.get("option_channel"),
                "address_street": a.get("address_street"),
                "address_zip_code": a.get("address_zip_code"),
                "address_city": a.get("address_city"),
                "country_code": a.get("country_code"),
                "geo_coordinates_x_value": a.get("geo_coordinates_x_value"),
                "geo_coordinates_y_value": a.get("geo_coordinates_y_value"),
                "created_at": _fmt_dt(a.get("created_at")),
                "updated_at": _fmt_dt(a.get("updated_at")),
            })

        contact_channels = []
        for c in contacts:
            contact_channels.append({
                "person_id": person_id,
                "contact_type": c.get("contact_type"),
                "value": c.get("value"),
                "flag_main_type": c.get("flag_main_type"),
                "preferred_channel": c.get("preferred_channel"),
                "option_channel": c.get("option_channel"),
                "flag_valid": c.get("flag_valid"),
                "created_at": _fmt_dt(c.get("created_at")),
                "updated_at": _fmt_dt(c.get("updated_at")),
            })

        subscriptions = []
        for s in subs:
            subscriptions.append({
                "person_id": person_id,
                "communication_code": s.get("communication_code"),
                "value": s.get("value"),
                "date_of_subscription": _fmt_dt(s.get("date_of_subscription")),
                "date_of_unsubscription": _fmt_dt(s.get("date_of_unsubscription")),
                "reason_of_unsubscription": s.get("reason_of_unsubscription"),
                "updated_at": _fmt_dt(s.get("updated_at")),
            })

        digital_access = None
        if digital:
            digital_access = {
                "person_id": person_id,
                "username": digital.get("username"),
                "email_user": digital.get("email_user"),
                "is_active": digital.get("is_active"),
                "last_login_at": _fmt_dt(digital.get("last_login_at")),
                "portal_user_confirmation_at": _fmt_dt(digital.get("portal_user_confirmation_at")),
                "created_at": _fmt_dt(digital.get("created_at")),
                "updated_at": _fmt_dt(digital.get("updated_at")),
            }

        indicator_payload = None
        if indicator:
            indicator_payload = {
                "person_id": person_id,
                "type": indicator.get("type"),
                "is_active": indicator.get("is_active"),
                "updated_at": _fmt_dt(indicator.get("updated_at")),
                "created_at": _fmt_dt(indicator.get("created_at")),
            }

        languages_payload = [{
            "person_id": person_id,
            "language_code": l.get("language_code"),
            "language_level": l.get("language_level"),
        } for l in languages]

        nationalities_payload = [{
            "person_id": person_id,
            "country_code": n.get("country_code"),
        } for n in nationalities]

        return {
            "account": account,
            "loyalty": loyalty_payload,
            "nationalities": nationalities_payload,
            "address_channels": address_channels,
            "contact_channels": contact_channels,
            "communication_subscriptions": subscriptions,
            "digital_access": digital_access,
            "account_indicators": indicator_payload,
            "languages": languages_payload,
        }

    def _update_contact(self, payload) -> bool:
        phones = [c for c in payload["contact_channels"] if c["contact_type"] == "phone"]
        if not phones:
            return False
        target = choice(phones)
        target["value"] = f"+48 {randint(500, 899):03d} {randint(0, 999):03d} {randint(0, 999):03d}"
        target["updated_at"] = self._now_str()
        return True

    def _update_address(self, payload) -> bool:
        if not payload["address_channels"]:
            return False
        target = choice(payload["address_channels"])
        target["address_city"] = self.fake.city()
        target["address_street"] = self.fake.street_address()
        target["address_zip_code"] = self.fake.zipcode()
        target["updated_at"] = self._now_str()
        return True

    def _update_loyalty(self, payload) -> bool:
        if not payload["loyalty"]:
            return False
        current = payload["loyalty"]["status_code"]
        options = [s for s in LOYALTY_STATUSES if s != current]
        payload["loyalty"]["status_code"] = choice(options)
        payload["loyalty"]["updated_at"] = self._now_str()
        return True

    def _update_subscription(self, payload) -> bool:
        active_subs = [s for s in payload["communication_subscriptions"]
                       if not s.get("date_of_unsubscription")]
        if not active_subs:
            return False
        target = choice(active_subs)
        target["value"] = "AIV_02"
        target["date_of_unsubscription"] = self._now_str()
        target["reason_of_unsubscription"] = choice(["spam", "not_interested", "too_frequent"])
        target["updated_at"] = self._now_str()
        return True

    def _update_civil(self, payload) -> bool:
        current = payload["account"].get("civil_status_code")
        options = [s["civil"] for s in CIVIL_STATUSES if s["civil"] != current]
        payload["account"]["civil_status_code"] = choice(options)
        return True

    def _update_contact_add(self, payload) -> bool:
        contact_type = choice(["email", "phone"])
        if contact_type == "phone":
            new_value = f"+48 {randint(500, 899):03d} {randint(0, 999):03d} {randint(0, 999):03d}"
        else:
            first = payload["account"]["first_name"].lower()
            last = payload["account"]["last_name"].lower()
            new_value = f"{first}.{last}{randint(10, 999)}@{choice(EMAIL_DOMAINS)}"
        existing_values = {c["value"] for c in payload["contact_channels"]}
        if new_value in existing_values:
            return False
        now = self._now_str()
        payload["contact_channels"].append({
            "person_id": payload["account"]["person_id"],
            "contact_type": contact_type,
            "value": new_value,
            "flag_main_type": False,
            "preferred_channel": False,
            "option_channel": True,
            "flag_valid": True,
            "created_at": now,
            "updated_at": now,
        })
        return True

    def _update_address_add(self, payload) -> bool:
        existing_types = {a.get("address_type") for a in payload["address_channels"]}
        available = [t for t in ADDRESS_TYPES if t not in existing_types]
        if not available:
            return False
        new_type = choice(available)
        now = self._now_str()
        payload["address_channels"].append({
            "person_id": payload["account"]["person_id"],
            "address_type": new_type,
            "option_channel": True,
            "address_street": self.fake.street_address(),
            "address_zip_code": self.fake.zipcode(),
            "address_city": self.fake.city(),
            "country_code": "PL",
            "geo_coordinates_x_value": None,
            "geo_coordinates_y_value": None,
            "created_at": now,
            "updated_at": now,
        })
        return True

    def _update_subscription_add(self, payload) -> bool:
        existing = {s.get("communication_code") for s in payload["communication_subscriptions"]}
        available = [c["code"] for c in COMMUNICATION_CODES if c["code"] not in existing]
        if not available:
            return False
        now = self._now_str()
        payload["communication_subscriptions"].append({
            "person_id": payload["account"]["person_id"],
            "communication_code": choice(available),
            "value": "AIV_01",
            "date_of_subscription": now,
            "date_of_unsubscription": None,
            "reason_of_unsubscription": None,
            "updated_at": now,
        })
        return True

    def _update_indicator_add(self, payload) -> bool:
        if payload.get("account_indicators"):
            return False
        birth_str = payload["account"].get("birth_date")
        if not birth_str:
            return False
        birth = datetime.strptime(birth_str, "%Y-%m-%d").date()
        today = date.today()
        age = today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))
        if age >= 60:
            indicator_type = "SENIOR"
        elif age > 30:
            indicator_type = choice(["KDR", "EMPLOYEE"])
        else:
            indicator_type = "EMPLOYEE"
        now = self._now_str()
        payload["account_indicators"] = {
            "person_id": payload["account"]["person_id"],
            "type": indicator_type,
            "is_active": True,
            "updated_at": now,
            "created_at": now,
        }
        return True

    def _update_language_add(self, payload) -> bool:
        existing = {l.get("language_code") for l in payload["languages"]}
        available = [l["language_code"] for l in LANGUAGES if l["language_code"] not in existing]
        if not available:
            return False
        payload["languages"].append({
            "person_id": payload["account"]["person_id"],
            "language_code": choice(available),
            "language_level": choice(LANGUAGE_LEVELS),
        })
        return True

    def _update_resubscribe(self, payload) -> bool:
        inactive = [s for s in payload["communication_subscriptions"]
                    if s.get("date_of_unsubscription")]
        if not inactive:
            return False
        target = choice(inactive)
        target["value"] = "AIV_01"
        target["date_of_unsubscription"] = None
        target["reason_of_unsubscription"] = None
        target["updated_at"] = self._now_str()
        return True

    def _apply_update(self, payload) -> str:
        handlers = {
            "contact": self._update_contact,
            "address": self._update_address,
            "loyalty": self._update_loyalty,
            "subscription": self._update_subscription,
            "civil": self._update_civil,
            "contact_add": self._update_contact_add,
            "address_add": self._update_address_add,
            "subscription_add": self._update_subscription_add,
            "indicator_add": self._update_indicator_add,
            "language_add": self._update_language_add,
            "resubscribe": self._update_resubscribe,
        }
        actions = UPDATE_ACTIONS.copy()
        while actions:
            action = choice(actions)
            actions.remove(action)
            if handlers[action](payload):
                return action
        return "civil"

    def _now_str(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def generate_update(self):
        event_id = str(uuid.uuid4())[:8]
        correlation_id.set(event_id)

        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                person_id = self._select_random_person_id(cur)
        finally:
            self._put_conn(conn)

        if not person_id:
            return None

        payload = self._fetch_customer(person_id)
        if not payload:
            return None

        action = self._apply_update(payload)

        payload["account"]["correlation_id"] = event_id

        return {
            "event_id": event_id,
            "event_type": "CUSTOMER_UPDATED",
            "event_timestamp": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "schema_version": SCHEMA_VERSION,
            "source_system": SOURCE_SYSTEM,
            "update_action": action,
            "payload": payload,
        }


if __name__ == "__main__":
    updater = PersonUpdater()
    event = updater.generate_update()
    print(json.dumps(event, indent=2, ensure_ascii=False, default=str))
