import json
import os
import time

from dotenv import load_dotenv
from kafka import KafkaProducer

from apps.crm_generator.person_updater import PersonUpdater

load_dotenv()


class CrmUpdater:

    def __init__(self):
        self.updater = PersonUpdater()
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        self.topic = os.getenv("KAFKA_TOPIC")

    def send_update(self):
        event = self.updater.generate_update()
        if event is None:
            return None
        self.producer.send(self.topic, value=event)
        return event


if __name__ == "__main__":
    updater = CrmUpdater()
    interval = float(os.getenv("CRM_UPDATE_INTERVAL_SECONDS", "5"))
    try:
        while True:
            event = updater.send_update()
            if event is None:
                print("No customers in DB yet — skipping update")
            else:
                print(f"[{event['event_id']}] {event['event_type']} action={event['update_action']} "
                      f"person_id={event['payload']['account']['person_id']}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nShutdown complete.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        updater.producer.flush()
