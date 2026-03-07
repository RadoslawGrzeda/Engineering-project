FROM python:3.11-slim

WORKDIR /app

COPY kafka_minio_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Odwzoruj strukturę pakietu apps.* wymaganą przez importy
COPY __init__.py apps/__init__.py
COPY logger_config.py apps/logger_config.py
COPY kafka_minio_consumer/ apps/kafka_minio_consumer/

CMD ["python", "-m", "apps.kafka_minio_consumer.kafka_consumer"]
