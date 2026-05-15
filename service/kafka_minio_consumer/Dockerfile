FROM python:3.11-slim

WORKDIR /app

COPY kafka_minio_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY __init__.py service/__init__.py
COPY logger_config.py service/logger_config.py
COPY geo_coordinates/ service/geo_coordinates/
COPY kafka_minio_consumer/ service/kafka_minio_consumer/

CMD ["python", "-m", "service.kafka_minio_consumer.kafka_consumer"]
