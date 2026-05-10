import time

from apps.crm_generator.person_generator import PersonGenerator
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import json
load_dotenv()

class CrmProducer:
    """
    Class responsible for simulating a real CRM mechanism
    and generate event with registered user
    """

    def __init__(self):
        self.generator=PersonGenerator()
        self.producer = KafkaProducer(
                                        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                    )
        self.topic=os.getenv('KAFKA_TOPIC')


    def generate_person(self):
        person = self.generator.generate_customer()
        self.producer.send(self.topic, value=person)
        return person

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

