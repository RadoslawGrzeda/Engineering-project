import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

from service.crm_generator import scheduler
from service.crm_generator.person_updater import PersonUpdater
# from service.crm_generator.producer.crm_producer import _strip_audit_timestamps

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
        # _strip_audit_timestamps(event.get("payload"))
        self.producer.send(self.topic, value=event)
        return event


if __name__ == "__main__":
    updater = CrmUpdater()
    try:
        while True:
            if not scheduler.is_active():
                print(
                    f"Outside active hours "
                    f"({scheduler.ACTIVE_HOUR_START}:00–{scheduler.ACTIVE_HOUR_END}:00) "
                    f"— sleeping until the window opens"
                )
                scheduler.wait_until_active()
                continue

            batch_size = scheduler.next_batch_size()
            for _ in range(batch_size): 
                event = updater.send_update()
                if event is None:
                    print("No customers in DB yet — skipping update")
                else:
                    print(f"[{event['event_id']}] {event['event_type']} action={event['update_action']} "
                          f"person_id={event['payload']['account']['person_id']}")

            scheduler.sleep_random()
    except KeyboardInterrupt:
        print("\nShutdown complete.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        updater.producer.flush()
