import os
from pathlib import Path

from airflow.decorators import dag,task
import pendulum
import yaml
import clickhouse_connect


from dotenv import load_dotenv
load_dotenv()

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
            CREATE OR REPLACE TABLE {self.BRONZE_DB}.{t['target']}
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
        rows=ch.query(f"SELECT count() from {self.BRONZE_DB}.{t['target']}").result_rows[0][0]
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

