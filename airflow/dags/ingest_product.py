# airflow dag
from ingest_client import IngestFactory

dag=IngestFactory(dag_id = 'bronze_ingest_product',config_file='product_bronze_tables.yaml',
                tags=['bronze','client']).build_dag()
