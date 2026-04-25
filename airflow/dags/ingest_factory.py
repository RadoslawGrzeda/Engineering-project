# # from airflow import DAG, task
# import os
#
# import airflow
# from airflow.decorators import dag, task
# import clickhouse_connect
# import psycopg2
# from dotenv import load_dotenv
# load_dotenv()
#
#
# @dag(
#     dag_id='bronze_ingest_client.customer',
#     schedule_interval=None,
#     catchup=False,
#     tags=['ingest_client'],
# )
# def bronze_ingest_client_customer():
#     @task
#     def ensure_target_table_exists():
#         ch = clickhouse_connect.get_client(
#             host=os.getenv("CLICKHOUSE_HOST"),
#             port=int(os.getenv("CLICKHOUSE_HTTP_PORT", 8123)),
#             username=os.getenv("CLICKHOUSE_USER"),
#             password=os.getenv("CLICKHOUSE_PASSWORD"),
#         )
#         ch.command(f"CREATE DATABASE IF NOT EXISTS bronze")
#         ch.command(f"""
#               CREATE TABLE IF NOT EXISTS bronze.customer
#               (
#                   person_id            String,
#                   first_name           String,
#                   middle_name          Nullable(String),
#                   last_name            String,
#                   birth_date           Nullable(Date),
#                   passport_number      Nullable(String),
#                   gender_code          Nullable(String),
#                   civil_status_code    Nullable(String),
#                   registration_date    DateTime,
#                   creation_application Nullable(String),
#                   is_deleted           Bool,
#                   inserted_at          DateTime,
#                   last_ingested_at     Nullable(DateTime),
#                   correlation_id       Nullable(String),
#                   _ingested_at         DateTime DEFAULT now(),
#                   _logical_date        Date,
#                   _source_table        LowCardinality(String) DEFAULT 'customer'
#               )
#               ENGINE = ReplacingMergeTree(_ingested_at)
#               PARTITION BY _logical_date
#               ORDER BY (person_id, inserted_at)
#           """)
#     @task
#     def get_watermark(**kwargs):
#         return {
#             "from_date": kwargs['data_interval_start'].to_iso8601_string(),
#             "to_date": kwargs['data_interval_end'].to_iso8601_string(),
#             'logical_date':kwargs['data_interval_start'].to_date_string()
#         }
#     @task
#     def drop_partition(window: dict):
#         ch = clickhouse_connect.get_client(
#             host=os.getenv("CLICKHOUSE_HOST"),
#             port=int(os.getenv("CLICKHOUSE_HTTP_PORT", 8123)),
#             username=os.getenv("CLICKHOUSE_USER"),
#             password=os.getenv("CLICKHOUSE_PASSWORD"),
#         )
#         ch.command(f'ALTER TABLE bronze.customer DROP PARTITION %(we)s',parameters={"we": window["logical_date"]})
#
#     @task
#     def load(window: dict) -> int:
#         ch = clickhouse_connect.get_client(
#             host=os.getenv("CLICKHOUSE_HOST"),
#             port=int(os.getenv("CLICKHOUSE_HTTP_PORT", 8123)),
#             username=os.getenv("CLICKHOUSE_USER"),
#             password=os.getenv("CLICKHOUSE_PASSWORD"),
#         )
#         pg_host = os.getenv("POSTGRES_ODS_HOST")
#         pg_port = os.getenv("POSTGRES_ODS_PORT")
#         pg_db = os.getenv("POSTGRES_ODS_DB")
#         pg_user = os.getenv("POSTGRES_ODS_USER")
#         pg_pass = os.getenv("POSTGRES_ODS_PASSWORD")
#
#         ch.command(f"""
#                   INSERT INTO bronze.customer
#                   (person_id, first_name, middle_name, last_name, birth_date,
#                    passport_number, gender_code, civil_status_code, registration_date,
#                    creation_application, is_deleted, inserted_at, last_ingested_at,
#                    correlation_id, _logical_date)
#                   SELECT
#                       person_id, first_name, middle_name, last_name, birth_date,
#                       passport_number, gender_code, civil_status_code, registration_date,
#                       creation_application, is_deleted, inserted_at, last_ingested_at,
#                       correlation_id,
#                       toDate(%(ld)s) AS _logical_date
#                   FROM postgresql(
#                       %(addr)s, %(db)s, 'customer', %(user)s, %(pass)s, 'client'
#                   )
#                   WHERE inserted_at >= parseDateTimeBestEffort(%(ws)s)
#                     AND inserted_at <  parseDateTimeBestEffort(%(we)s)
#               """,parameters={
#             "addr": f"{pg_host}:{pg_port}",
#             "db": pg_db,
#             "user": pg_user,
#             "pass": pg_pass,
#             "ld": window["logical_date"],
#             "ws": window["from_date"],
#             "we": window["to_date"],
#         })
#
#         rows = ch.query(
#             f"SELECT count() FROM bronze.customer "
#             f"WHERE _logical_date = %(ld)s",
#             parameters={"ld": window["logical_date"]},
#         ).result_rows[0][0]
#         return rows
#
#     @task
#     def quality_check(rows: int):
#         if rows == 0:
#             raise ValueError("0 wierszy załadowanych — sprawdź ODS / watermark")
#
#     w = get_watermark()
#     ensure_target_table_exists() >> drop_partition(w) >> quality_check(load(w))
#
#
# bronze_ingest_client_customer()
import os
from pathlib import Path

from airflow.decorators import dag,task
import pendulum
import yaml
import clickhouse_connect


from dotenv import load_dotenv
load_dotenv()
# BONZE_DB='bronze'
# CONFIG_PATH=Path(__file__).parent.parent / 'config' / 'client_bronze_tables.yaml'

class IngestFactory:
    def __init__(self, dag_id : str, config_file: str, schedule: str = "0 2 * * *",
                start_date: pendulum.DateTime | None = None, tags: list[str] | None = None) -> None:
        self.BRONZE_DB='bronze'
        self.CONFIG_DIR = Path(__file__).parent.parent / "config"
        self.dag_id  = dag_id
        self.config_path = self.CONFIG_DIR / config_file
        self.schedule = schedule
        self.start_date = start_date or pendulum.datetime(2026,4,1, tz='Europe/Warsaw')
        self.tags = tags or ["Bronze"]

    def _ch(self):
        return clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            port=int(os.getenv("CLICKHOUSE_HTTP_PORT", 8123)),
            username=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
        )
    def _load_config(self) -> dict:
        with self.config_path.open() as f:
            return yaml.safe_load(f)

    @staticmethod
    def _merge_default(tbl : dict, defaults: dict) -> dict:
        src_schema, src_table = tbl['source'].split('.')
        return {
            'src_schema': src_schema,
            'src_table': src_table,
            'engine': tbl.get('engine', defaults.get('engine','MergeTree')),
            'order_by': tbl.get('order_by', defaults.get('order_by','tuple()')),
            'where': tbl.get('where'),
            'enabled': tbl.get('enabled', True),
            'excluded_columns': tbl.get('excluded_columns', []),
            'target': tbl.get('target', f"{src_schema}__{src_table}")
        }
    # ------- Steps
    def ensure_bronze_database_exists(self):
        self._ch().command(f"CREATE DATABASE IF NOT EXISTS {self.BRONZE_DB}")

    def get_watermark(self, **kwargs) -> dict:
        return {
            'ingested_from': kwargs['data_interval_start'].to_iso8601_string(),
            'ingested_to': kwargs['data_interval_end'].to_iso8601_string(),
            'logical_date': kwargs['data_interval_start'].to_date_string()
        }
    def build_plan(self) -> list[dict]:
        cfg = self._load_config()
        defaults = cfg.get('defaults',{})
        plan =[IngestFactory._merge_default(t,defaults) for t in cfg['tables']]
        p = [p for p in plan if p['enabled']]
        return p

    def refresh_table(self, t : dict) -> dict:
        ch = self._ch()
        select_cols = (f" * EXCEPT({','.join(t['excluded_columns'])})" if t['excluded_columns'] else "*")
        where_clause = f"WHERE {t['where']}" if t['where'] else ""
        ddl = f"""
            CREATE OR REPLACE TABLE {BONZE_DB}.{t['target']}
            ENGINE = {t['engine']} ORDER BY {t['order_by']} 
            AS
            SELECT 
                {select_cols},
                now() as _ingested_at,
                %(src)s as _source_table
            FROM postgresql(
                %(addr)s, %(db)s, %(tbl)s, %(user)s, %(pass)s, %(schema)s
            )
            {where_clause} 
        """
        ch.command(ddl,parameters={
            'src': f"{t['src_schema']}.{t['src_table']}",
            'addr': f"{os.getenv('POSTGRES_ODS_HOST')}:{os.getenv('POSTGRES_ODS_PORT')}",
            'db': f"{os.getenv('POSTGRES_ODS_DB')}",
            'tbl': t['src_table'],
            'user': f"{os.getenv('POSTGRES_ODS_USER')}",
            'pass': f"{os.getenv('POSTGRES_ODS_PASSWORD')}",
            'schema': t['src_schema']
        })
        rows=ch.query(f"SELECT count() from {self.BONZE_DB}.{t['target']}").result_rows[0][0]
        return {'source': f"{t['src_schema']}.{t['src_table']}",
                'target':t['target'],'rows':rows}

    def summary(self, results : list[dict]) -> None:
        print("Ingestion summary:")
        total = sum(r['rows'] for r in results)
        empty = [r['source'] for r in results if r['rows'] == 0 ]
        print(f"Zaladowanie lacznie {len(results)} tabel i {total} wierszy")
        if empty:
            print(f"Uwaga! Puste tabele: {', '.join(empty)}")

    # dag factory
    def build_dag(self):
        @dag(
            dag_id = self.dag_id,
            default_args =  {
                            'owner': 'date_engineer',
                            'retries': 2,
                            'retry_delay': pendulum.duration(minutes=5),
                            },
            schedule = self.schedule,
            start_date=self.start_date,
            catchup=False,
            max_active_runs=1,
            tags=self.tags,
        )
        def _dag():
            @task
            def ensure_db():
                self.ensure_bronze_database_exists()

            @task
            def plan() -> list[dict]:
                return self.build_plan()

            @task
            def refresh_table(t: dict) -> dict:
                return self.refresh_table(t)

            @task
            def summarize(result: list[dict]):
                self.summary(result)

            db = ensure_db()
            tables= plan()
            result = refresh_table.expand(t=tables)
            db >> tables
            summarize(result)
        return _dag()
