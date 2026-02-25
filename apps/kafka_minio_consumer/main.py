from kafka import KafkaConsumer
import json
# from apps.logger_config import get_logger
import pandas as pd
import json
# logger = get_logger(__name__)
import requests
from urllib.parse import unquote

consumer = KafkaConsumer(
                        bootstrap_servers=['localhost:9092'],
                        group_id='minio-consumer-group',
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        auto_commit_interval_ms=5000,
                        session_timeout_ms=30000,
                        heartbeat_interval_ms=10000,
                        max_poll_records=100,
                        max_poll_interval_ms=300000,
                        max_partition_fetch_bytes=1048576,                        
                        )
# if consumer is None:
#     logger.error("Failed to create Kafka consumer.")
#     exit(1)

consumer.subscribe(['minio-events'])  

while True:
    message = consumer.poll(timeout_ms=1.0)
    if message:
        try:
            for tp, messages in message.items():
                for message in messages:
                    
                    event= json.loads(message.value.decode('utf-8'))
                    bucket_name = event['Records'][0]['s3']['bucket']['name']
                    
                    raw_name = event['Records'][0]['s3']['object']['key']

                    file_key = unquote(raw_name)
                    file_name=file_key.split('/')[1]
                    schema=file_key.split("/")[0]

                    print(f"Received message: bucket_name={bucket_name}, file_key={file_key}, schema_name={schema}")
                    # print(event)
            # print(f"Received message: {json.dumpmessage}")
            # print(type(message))
            # event = json.dumps(message)
            # event = json.loads(message.value.decode('utf-8'))
            # print(message)
            # print(event)
            # event=message.to_json() 
            # event=json.loads(message.value.decode('utf-8'))
            # bucket_name = event.value['Records'][0]['s3']['bucket']['name']
            # file_name = event.value['Records'][0]['s3']['object']['key']
            # requests.post(
            #         'http://localhost:8000/process_file',
            #           json={
            #             'conf': {
            #                 'bucket_name': bucket_name,
            #                 'file_name': file_name}},
            #         auth=('admin', 'admin')
            #                 )
        except Exception as e:
            print(f"Error processing message: {e}")
            continue

# data=json.load(open('/Users/radoslaw/Desktop/Engineering-project/apps/kafka_minio_consumer/test.json')) 
#bucket_name
#  print(data['Records'][0]['s3']['bucket']['name'])

#file_name
# print(data['Records'][0]['s3']['object']['key'])

# data = pd.read_json('/Users/radoslaw/Desktop/Engineering-project/apps/kafka_minio_consumer/test.json')




