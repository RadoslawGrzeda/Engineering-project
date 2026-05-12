# airflow dag
from ingest_factory import IngestFactory

dag = IngestFactory(dag_id='bronze_client_ingest',config_file='client_bronze_tables.yaml',schedule=None,tags=['bronze','client']).build_dag()

