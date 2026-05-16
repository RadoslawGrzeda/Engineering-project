FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-clickhouse==1.8.6

WORKDIR /dbt

COPY dbt_project.yml .
COPY profiles.yml    .
COPY models/         models/
COPY macros/         macros/

ENTRYPOINT ["dbt"]
