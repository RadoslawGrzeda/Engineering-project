FROM python:3.11-slim

WORKDIR /app

COPY geocoding_crm_address/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY __init__.py service/__init__.py
COPY logger_config.py service/logger_config.py
COPY geocoding_crm_address/ service/geocoding_crm_address/
