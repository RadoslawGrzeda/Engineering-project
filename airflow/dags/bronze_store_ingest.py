# airflow dag
from ingest_factory import IngestFactory

dag=IngestFactory(dag_id = 'bronze_store_ingest', config_file = 'site_bronze_tables.yaml',
                tags = ['bronze','store'], schedule=None, dbt_dag_id='dbt_transform_store').build_dag()