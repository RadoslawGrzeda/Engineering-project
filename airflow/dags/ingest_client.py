# airflow dag
from ingest_factory import IngestFactory

dag = IngestFactory('bronze_ingest_client','client_bronze_tables.yaml',tags=['bronze','client']).build_dag()

