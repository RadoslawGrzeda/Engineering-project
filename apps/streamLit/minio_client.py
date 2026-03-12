from minio import Minio
from dotenv import load_dotenv
import os
import sys
load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))
from apps.logger_config import get_logger

logger = get_logger('apps.streamlit.minio_client.py', service="streamlit")

class MinioClient:
    def __init__(self):
        self.client=Minio(
            os.getenv('MINIO_ADDRESS'),
            access_key=os.getenv('MINIO_USER'),
            secret_key=os.getenv('MINIO_PASSWORD'),
            secure=False
        )
    
    def upload_file(self, client_name : str, bucket_name : str, file_name:str, file_data:bytes, length:int, content_type:str, correlation_id:str):
        try:
            self.client.put_object(bucket_name,file_name,file_data,length,content_type,metadata={'correlation_id':correlation_id})
            logger.info("File uploaded to MinIO", extra={
                "file_name": file_name,
                "bucket": bucket_name,
                "class": "MinioClient",
                "method": "upload_file",
            })
        except Exception as e:
            logger.error("Error uploading file to MinIO", extra={
                "file_name": file_name,
                "bucket": bucket_name,
                "class": "MinioClient",
                "method": "upload_file",
                "error_type": type(e).__name__,
                "error": str(e),
            }, exc_info=True)
            raise
    def make_bucket(self,client_name:str,bucket_name:str):
        try:
            self.client.make_bucket(bucket_name)
            logger.info("Bucket created", extra={
                "bucket": bucket_name,
                "class": "MinioClient",
                "method": "make_bucket",
            })
        except Exception as e:
            logger.error("Error creating bucket", extra={
                "bucket": bucket_name,
                "class": "MinioClient",
                "method": "make_bucket",
                "error_type": type(e).__name__,
                "error": str(e),
            }, exc_info=True)
            raise
    def bucket_exists(self,bucket_name:str):
        return self.client.bucket_exists(bucket_name)
# client.bucket_exists('sklep')

