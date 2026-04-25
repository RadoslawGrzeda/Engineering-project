# airflow dag
import ingest_client

dag=ingest_client.IngestFactory(dag_id = 'bronze_ingest_site', config_file = 'site_bronze_tables.yaml',
                tags = ['bronze','site']).build_dag()