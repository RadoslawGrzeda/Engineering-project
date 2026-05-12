FROM python:3.11-slim

WORKDIR /app

COPY crm_generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY __init__.py apps/__init__.py
COPY logger_config.py apps/logger_config.py
COPY crm_generator/ apps/crm_generator/
