# from apps.kafka_minio_consumer.main import file_key
# from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from datetime import datetime


@dag(
    dag_id='process_minio_file',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['minio', 'etl']
)
def process_minio_file():
    @task
    def load_file_from_bucket(**context):
        from minio import Minio
        import os, tempfile

        conf = context["dag_run"].conf
        bucket_name = conf['bucket_name']
        file_key = conf['file_key']
        file_schema = conf['file_schema']

        minio_client=Minio (
            os.getenv('MINIO_ADDRESS'),
            access_key=os.getenv('MINIO_USER'),
            secret_key=os.getenv('MINIO_PASSWORD'),
            secure=False
        )
        local_path=os.path.join(tempfile.gettempdir(),file_key.split('/')[-1])
        minio_client.fget_object(bucket_name, file_key, local_path)
        print(f"File downloaded to {local_path}")
        return {'path':local_path, 'file_schema': file_schema}
    
    @task
    def validate_schema_and_load_file(file_info):
        from apps.load_file_develop.main import ProcessData
        try:
            process_data=ProcessData(file_info['path'])
            data, errors = process_data.validate_shape(file_info['file_schema'])
        except Exception as e:
            print(f"Error during schema validation: {e}")
            raise   
        if errors:
            print(f"Schema validation errors: {errors}")
            raise ValueError("Schema validation failed")
        print("Schema validation passed")
            # return process_data,file_info['file_schema'], data
        try:
            process_data.load_to_db(data, file_info['file_schema'])
        except Exception as e:
            print(f"Error during loading to database: {e}")
            raise
        print("Data loaded successfully")
    
    file_info = load_file_from_bucket()
    validate_schema_and_load_file(file_info)

process_minio_file()