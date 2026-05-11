# airflow dag
from ingest_factory import IngestFactory

dag=IngestFactory(dag_id = 'bronze_product_ingest',config_file='product_bronze_tables.yaml',
                tags=['bronze','product'],schedule=None, dbt_dag_id='dbt_transform_product').build_dag()
