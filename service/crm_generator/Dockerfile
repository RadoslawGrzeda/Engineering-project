FROM python:3.11-slim

WORKDIR /app

COPY crm_generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY __init__.py service/__init__.py
COPY logger_config.py service/logger_config.py
COPY crm_generator/ service/crm_generator/
