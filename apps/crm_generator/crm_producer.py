import time

from person_generator import PersonGenerator
# from crm_generator import CrmGenerator
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import json
load_dotenv()

class CrmProducer:
    """
    Class responsible from simulating real CRM mechanism
    and generate event with registered user
    """

    def __init__(self):
        self.generator=PersonGenerator()
        self.producer = KafkaProducer(
                                        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                                        # key_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                    )
        self.topic=os.getenv('KAFKA_TOPIC')


    def generate_person(self):
        person = self.generator.generate_customer()
        self.producer.send(self.topic, value=person)
        return person

if __name__ == "__main__":
    crm = CrmProducer()
    while True:
        print(crm.generate_person())
        time.sleep(5)
