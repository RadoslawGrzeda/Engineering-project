FROM python:3.11-slim

WORKDIR /app

COPY geocoding_crm_address/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY __init__.py apps/__init__.py
COPY geocoding_crm_address/ apps/geocoding_crm_address/
