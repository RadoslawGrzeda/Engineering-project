from minio.commonconfig import CopySource
from pandas import errors
from kafka import KafkaConsumer
import json
import requests
from urllib.parse import unquote
import os
from dotenv import load_dotenv
from minio import Minio
from apps.logger_config import get_logger, correlation_id
from apps.kafka_minio_consumer.load_file_develop.file_processor import  DataLoader
import io 
import pandas as pd
from sqlalchemy import create_engine, text
import uuid

MAX_RETRIES = 3

class KafkaMinioConsumer:
    def __init__(self):
        load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
        self.logger = get_logger('apps.kafka_minio_consumer.kafka_consumer.py', service="kafka-minio-consumer")
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        self.group_id = os.getenv('KAFKA_GROUP_ID')
        self.topic=os.getenv('KAFKA_TOPIC')
        self.db_engine = create_engine(os.getenv('postgress_connection'))
        # self.db_engine = create_engine(os.getenv('POSTGRES_CONNECTION'))
        # self.airflow_api_url = os.getenv('AIRFLOW_API_URL')
        # self.API_USER = os.getenv('API_USER')
        # self.API_PASSWORD = os.getenv('API_PASSWORD')
        try:
            self.consumer = KafkaConsumer(
                                    bootstrap_servers=[self.bootstrap_servers],
                                    group_id=self.group_id,
                                    auto_offset_reset='latest',
                                    enable_auto_commit=False,
                                    auto_commit_interval_ms=5000,
                                    session_timeout_ms=30000,
                                    heartbeat_interval_ms=10000,
                                    max_poll_records=100,
                                    max_poll_interval_ms=300000,
                                    max_partition_fetch_bytes=1048576,                        
                                    )
        except Exception as e:
            self.logger.error("Error creating Kafka consumer: %s", e, extra={'class':'KafkaMinioConsumer', 'method': "__init__", 'bootstrap_servers': self.bootstrap_servers, 'group_id': self.group_id, 'topic': self.topic})
            raise
        self.consumer.subscribe([self.topic])
        self.logger.info("Kafka consumer created successfully and subscribed to topic '%s'", self.topic, extra={'class': 'KafkaMinioConsumer', 'method': "__init__", 'bootstrap_servers': self.bootstrap_servers, 'group_id': self.group_id, 'topic': self.topic} )

        try:
            self.minio=Minio (
                os.getenv('MINIO_ADDRESS'),
                access_key=os.getenv('MINIO_USER'),
                secret_key=os.getenv('MINIO_PASSWORD'),
                secure=False
            )
        except Exception as e:
            self.logger.error("Error creating Minio client: %s", e, 
            extra={
                'class': self.__class__.__name__,
                'method': "__init__",
                'minio_address': os.getenv('MINIO_ADDRESS')})
            raise
        self.logger.info("Minio client created successfully", extra={'class': 'KafkaMinioConsumer', 'method': "__init__", 'minio_address': os.getenv('MINIO_ADDRESS')})
    # def _send_post_request(self, bucket_name, file_key, schema):
    #     try:
    #         response = requests.post(
    #             self.airflow_api_url,
    #             json={
    #                 'conf': {
    #                     'bucket_name': bucket_name,
    #                     'file_key': file_key,
    #                     'file_schema' : schema}},
    #             auth=(self.API_USER, self.API_PASSWORD)
    #         )
    #         response.raise_for_status()
    #         self.logger.info("POST request sent successfully to API for file '%s' in bucket '%s'", file_key.split('/')[-1], bucket_name, extra={'file_key': file_key, 'bucket_name': bucket_name, 'schema': schema})
    #     except requests.exceptions.RequestException as e:
    #         self.logger.error("Error sending POST request to API: %s", e, extra={'file_key': file_key, 'bucket_name': bucket_name, 'schema': schema})
    def  _load_file_from_minio(self, bucket_name:str, file_key:str) -> pd.DataFrame:
        '''
        This method is responsible for load file from minio and return it for next processing steps
        '''
        response = None
        try:
            response = self.minio.get_object(bucket_name, file_key)
            buffer=io.BytesIO(response.read())
            df=pd.read_csv(buffer)
            self.logger.info("File '%s' loaded successfully from bucket '%s'",
                            file_key.split('/')[-1],
                            bucket_name,
                            extra={
                                'class': 'KafkaMinioConsumer',
                                'method': "_load_file_from_minio",
                                'file_key': file_key,
                                'bucket_name': bucket_name
                            })
            return df
        except Exception as e:
            self.logger.error("Error loading file from Minio: %s", e, extra={
                'class': 'KafkaMinioConsumer',
                'method': "_load_file_from_minio",
                'file_key': file_key,
                'bucket_name': bucket_name
            })
            raise
        finally:
            if response:
                response.close()
                response.release_conn()

    def _archive_file_in_minio(self, bucket_destination:str, bucket_source: str, file_key: str) -> None :
        '''
        This method is responsible for archiving file from minio after successfully processing the file
        '''
        try:
            self.minio.copy_object(bucket_destination,file_key,CopySource(bucket_source,file_key))
            self.minio.remove_object(bucket_source,file_key)

        except Exception as e:
            self.logger.error("Error archiving file: %s", e, extra={
                'class': 'KafkaMinioConsumer',
                'method': "_archive_file_in_minio",
                'bucket_destination': bucket_destination,
                'bucket_source': bucket_source,
                'file_key': file_key,

            },exc_info=True)

    def _change_file_status_in_db(self, destination_table,file_name, status,error_message=None,inserted_rows=None,rejected_rows=None,engine=None, correlation_id=None):
            sql = text('''
                UPDATE etl_load_log_product
                SET status = :status,
                    error_message = :error_message,
                    inserted_rows_count = :inserted_rows_count,
                    rejected_rows_count = :rejected_rows_count,
                    correlation_id = :correlation_id,
                    processed_at = NOW()
                WHERE file_name = :file_name
                and destination_table = :destination_table
            ''')
            with self.db_engine.connect() as connection:
                try:

                    connection.execute(sql, {
                        'destination_table': destination_table,
                        'file_name': file_name,
                        'status': status,
                        'error_message': error_message if error_message is not None else None,
                        'inserted_rows_count': inserted_rows if inserted_rows is not None else 0,
                        'rejected_rows_count': rejected_rows if rejected_rows is not None else 0,
                        'correlation_id': correlation_id
                    })
                    connection.commit()
                except Exception as e:
                    self.logger.error("Error updating file status in database: %s", e, extra={
                        'class': 'KafkaMinioConsumer',
                        'method': "_change_file_status_in_db",
                        'file_name': file_name,
                        'destination_table': destination_table,
                        'status': status
                    })
                    raise
    
    def _validate_and_load_to_db(self, df, schema, file_key, cid=None):
        '''
        '''
        try:
            process_data=DataLoader(df,file_key, correlation_id=cid,engine=self.db_engine)
            self._change_file_status_in_db(destination_table=schema,file_name=file_key.split('/')[-1], status='pending',engine=self.db_engine, correlation_id=cid)
            data, errors = process_data.validate_shape(schema)
            if errors:
                # self.logger.error("Schema validation errors: %s", errors, extra={'schema': schema, 'file_key': file_key})
                process_data.load_to_dead_letter(errors, schema)
            # self.logger.info("Schema validation passed", extra={'schema': schema, 'file_key': file_key})
            try:
                len_of_load=process_data.load_to_db(data, schema)
                if len_of_load == 0:
                    self._change_file_status_in_db(destination_table=schema,file_name=file_key.split('/')[-1], status='success', error_message=str(errors), engine=self.db_engine, correlation_id=cid)
                elif errors:
                    self._change_file_status_in_db(destination_table=schema,file_name=file_key.split('/')[-1], status='error', error_message=str(errors), engine=self.db_engine, correlation_id=cid)
                elif len_of_load < len(df):
                    self._change_file_status_in_db(destination_table=schema,file_name=file_key.split('/')[-1], status='partial_success', inserted_rows=len_of_load, error_message=str(errors), rejected_rows=len(errors), engine=self.db_engine, correlation_id=cid)
                else:
                    self._change_file_status_in_db(destination_table=schema,file_name=file_key.split('/')[-1], status='success', inserted_rows=len_of_load, engine=self.db_engine, correlation_id=cid)
            except Exception as e:
                self._change_file_status_in_db(destination_table=schema,file_name=file_key.split('/')[-1], status='error', error_message=str(e), engine=self.db_engine, correlation_id=cid)
                # self.logger.error("Error during loading to database: %s", e, extra={'schema': schema, 'file_key': file_key})
                raise
        except Exception as e:
            self.logger.error("Error processing data: %s", e, extra={'class': 'KafkaMinioConsumer', 'method': "_validate_and_load_to_db", 'schema': schema, 'file_key': file_key}, exc_info=True)
            raise

            
    def _message_key(self, tp, msg):
        """Unique key for a Kafka message: (topic, partition, offset)."""
        return (tp.topic, tp.partition, msg.offset)

    def consume_messages(self):
        retry_counts = {}

        while True:
            message = self.consumer.poll(timeout_ms=1000)
            if not message:
                continue

            for tp, messages in message.items():
                for msg in messages:
                    msg_key = self._message_key(tp, msg)
                    file_key = bucket_name = schema = None

                    try:
                        event = json.loads(msg.value.decode('utf-8'))
                        record = event['Records'][0]

                        user_meta = record['s3']['object'].get('userMetadata', {})
                        cid = user_meta.get('X-Amz-Meta-Correlation_id', str(uuid.uuid4())[:8])
                        correlation_id.set(cid)

                        bucket_name = record['s3']['bucket']['name']
                        raw_name = record['s3']['object']['key']
                        file_key = unquote(raw_name)
                        file_name = file_key.split('/')[-1]
                        schema = file_key.split("/")[0]

                        self.logger.info("Received event for file '%s' in bucket '%s'", file_name, bucket_name,
                                        extra={'class': 'KafkaMinioConsumer', 'method': 'consume_messages',
                                        'file_key': file_key, 'bucket_name': bucket_name, 'schema': schema}
                                        )

                        df = self._load_file_from_minio(bucket_name, file_key)
                        self._validate_and_load_to_db(df, schema, file_key, cid=cid)
                        self.consumer.commit()

                        retry_counts.pop(msg_key, None)
                        self._archive_file_in_minio('archive',bucket_name, file_key)

                    except Exception as e:
                        retries = retry_counts.get(msg_key, 0) + 1
                        retry_counts[msg_key] = retries

                        if retries >= MAX_RETRIES:
                            self.logger.error(
                                "Poison message — skipping after %d retries: %s",
                                MAX_RETRIES, e,
                                extra={
                                    'class': 'KafkaMinioConsumer',
                                    'method': 'consume_messages',
                                    'file_key': file_key,
                                    'bucket_name': bucket_name,
                                    'schema': schema,
                                    'topic': tp.topic,
                                    'partition': tp.partition,
                                    'offset': msg.offset,
                                }, exc_info=True)
                            retry_counts.pop(msg_key, None)
                            self.consumer.commit()
                        else:
                            self.logger.warning(
                                "Error processing message (retry %d/%d): %s",
                                retries, MAX_RETRIES, e,
                                extra={
                                    'class': 'KafkaMinioConsumer',
                                    'method': 'consume_messages',
                                    'file_key': file_key,
                                    'bucket_name': bucket_name,
                                    'schema': schema,
                                    'topic': tp.topic,
                                    'partition': tp.partition,
                                    'offset': msg.offset,
                                }, exc_info=True)

if __name__ == "__main__":
    consumer = KafkaMinioConsumer()
    consumer.consume_messages()


