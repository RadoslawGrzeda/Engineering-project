from kafka import KafkaConsumer
import json


consumer = KafkaConsumer(
                        bootstrap_servers=['localhost:9092'],
                        group_id='consumer-group',
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        auto_commit_interval_ms=5000,
                        session_timeout_ms=30000,
                        heartbeat_interval_ms=10000,
                        max_poll_records=100,
                        max_poll_interval_ms=300000,
                        max_partition_fetch_bytes=1048576,                        
                        )

consumer.subscribe(['minio-events'])  

while True:
    message = consumer.poll(timeout_ms=1000)
    if message:
        for topic_partition, messages in message.items():
            for message in messages:
                print(message.value.decode('utf-8'))
