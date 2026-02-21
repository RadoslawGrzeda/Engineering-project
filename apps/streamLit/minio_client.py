from minio import Minio
from dotenv import load_dotenv
import os
load_dotenv()
import logging

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger=logging.getLogger(__name__)

class MinioClient:
    def __init__(self):
        self.client=Minio(
            os.getenv('MINIO_ADDRESS'),
            access_key=os.getenv('MINIO_USER'),
            secret_key=os.getenv('MINIO_PASSWORD'),
            secure=False
        )
    
    def upload_file(self,client_name:str,bucket_name:str,file_name:str,file_data:bytes,length:int,content_type:str):
        try:
            self.client.put_object(bucket_name,file_name,file_data,length,content_type)
            logger.info(f"File {file_name} uploaded to bucket {bucket_name}")
            return f'Successfully uploaded file {file_name} to bucket {bucket_name}'
        except Exception as e:
            logger.error(f"Error uploading file {file_name} to bucket {bucket_name}: {e}")
            return f'Error uploading file {file_name} to bucket {bucket_name} : {e}'
    def make_bucket(self,client_name:str,bucket_name:str):
        try:
            self.client.make_bucket(bucket_name)
            logger.info(f"Bucket {bucket_name} created")
            return True
        except Exception as e:
            logger.error(f"Error creating bucket {bucket_name}: {e}")
            return False
    def bucket_exists(self,bucket_name:str):
        return self.client.bucket_exists(bucket_name)
# client.bucket_exists('sklep')

