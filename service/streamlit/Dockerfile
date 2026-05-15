FROM python:3.11-slim

WORKDIR /app

COPY streamlit/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streamlit/ .
COPY __init__.py service/__init__.py
COPY logger_config.py service/logger_config.py

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app.py", "--server.address=0.0.0.0", "--server.port=8501"]
