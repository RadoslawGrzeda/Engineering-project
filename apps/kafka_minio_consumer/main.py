from click import group
from kafka import KafkaConsumer
import json
import json
import requests
from urllib.parse import unquote
import os 
from dotenv import load_dotenv
from apps.logger_config import get_logger

class KafkaMinioConsumer:
    def __init__(self):
        load_dotenv()
        self.logger = get_logger(__name__)
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.group_id = os.getenv('KAFKA_GROUP_ID')
        self.topic=os.getenv('KAFKA_TOPIC')
        self.airflow_api_url = os.getenv('AIRFLOW_API_URL')
        self.API_USER = os.getenv('API_USER')
        self.API_PASSWORD = os.getenv('API_PASSWORD')
        try:
            self.consumer = KafkaConsumer(
                                    bootstrap_servers=[self.bootstrap_servers],
                                    group_id=self.group_id,
                                    auto_offset_reset='earliest',
                                    enable_auto_commit=True,
                                    auto_commit_interval_ms=5000,
                                    session_timeout_ms=30000,
                                    heartbeat_interval_ms=10000,
                                    max_poll_records=100,
                                    max_poll_interval_ms=300000,
                                    max_partition_fetch_bytes=1048576,                        
                                    )
        except Exception as e:
            self.logger.error("Error creating Kafka consumer: %s", e, extra={'bootstrap_servers': self.bootstrap_servers, 'group_id': self.group_id, 'topic': self.topic})
            raise
        self.logger.info("Kafka consumer created successfully and subscribed to topic '%s'", self.topic, extra={'bootstrap_servers': self.bootstrap_servers, 'group_id': self.group_id, 'topic': self.topic} )

    def _send_post_request(self, bucket_name, file_key, schema):
        try:
            response = requests.post(
                self.airflow_api_url,
                json={
                    'conf': {
                        'bucket_name': bucket_name,
                        'file_key': file_key,
                        'file_schema' : schema}},
                auth=(self.API_USER, self.API_PASSWORD)
            )
            response.raise_for_status()
            self.logger.info("POST request sent successfully to API for file '%s' in bucket '%s'", file_key.split('/')[-1], bucket_name, extra={'file_key': file_key, 'bucket_name': bucket_name, 'schema': schema})
        except requests.exceptions.RequestException as e:
            self.logger.error("Error sending POST request to API: %s", e, extra={'file_key': file_key, 'bucket_name': bucket_name, 'schema': schema})
            raise

    def consume_messages(self):
        while True:
            message = self.consumer.poll(timeout_ms=1.0)
            if message:
                try:
                    for tp, messages in message.items():
                        for message in messages:
                            event = json.loads(message.value.decode('utf-8'))
                            bucket_name = event['Records'][0]['s3']['bucket']['name']
                            raw_name = event['Records'][0]['s3']['object']['key']
                            file_key = unquote(raw_name)
                            file_name=file_key.split('/')[1]
                            schema=file_key.split("/")[0]
                            self.logger.info("Received event for file '%s' in bucket '%s'", file_name, bucket_name, extra={'file_key': file_key, 'bucket_name': bucket_name, 'schema': schema})
                            self._send_post_request(bucket_name, file_key, schema)
                except Exception as e:
                    self.logger.error("Error processing message: %s", e, extra={'file_key': file_key, 'bucket_name': bucket_name, 'schema': schema})
                    continue

if __name__ == "__main__":
    consumer = KafkaMinioConsumer()
    consumer.consume_messages()


