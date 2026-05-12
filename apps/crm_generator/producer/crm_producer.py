import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from apps.crm_generator.person_generator import PersonGenerator, SCHEMA_VERSION, SOURCE_SYSTEM
from dotenv import load_dotenv
from kafka import KafkaProducer
from pythonjsonlogger import json as jsonlogger

load_dotenv(Path(__file__).parent.parent / ".env")


def _event_timestamp() -> str:
    """ISO-8601 UTC w mikrosekundach z sufiksem 'Z'.
    Format akceptowany wprost przez Postgres TIMESTAMP[TZ] oraz java.time.OffsetDateTime/Instant.parse."""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


class CrmProducer:
    """
    Class responsible for simulating a real CRM mechanism
    and generate event with registered user
    """

    def __init__(self):
        self.generator=PersonGenerator()
        self.producer = KafkaProducer(
                                        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                                        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                                    )
        self.topic=os.getenv('KAFKA_TOPIC')


    def generate_person(self):
        person = self.generator.generate_customer()
        event_id = str(uuid.uuid4())[:8]
        if isinstance(person, dict) and isinstance(person.get("account"), dict):
            person["account"]["correlation_id"] = event_id
        event = {
            "event_id": event_id,
            "event_type": "CUSTOMER_CREATED",
            "event_timestamp": _event_timestamp(),
            "schema_version": SCHEMA_VERSION,
            "source_system": SOURCE_SYSTEM,
            "update_action": None,
            "payload": person,
        }
        self.producer.send(self.topic, value=event).get(timeout=10)
        return event

if __name__ == "__main__":
    crm = CrmProducer()
    try:
        while True:
            print(crm.generate_person())
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nShutdown complete.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        crm.generator._flush_counter()
        crm.producer.flush()

