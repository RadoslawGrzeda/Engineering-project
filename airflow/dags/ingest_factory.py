import os
from pathlib import Path

from airflow.decorators import dag,task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
import yaml
import clickhouse_connect
import psycopg2
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

_WARSAW = ZoneInfo('Europe/Warsaw')

from dotenv import load_dotenv
load_dotenv()

class IngestFactory:
    def __init__(self, dag_id : str, config_file: str, schedule: str = "0 2 * * *",
                start_date: pendulum.DateTime | None = None, tags: list[str] | None = None,
                dbt_dag_id: str | None = None) -> None:
        self.BRONZE_DB='bronze'
        self.CONFIG_DIR = Path(__file__).parent.parent / "config"
        self.dag_id  = dag_id
        self.config_path = self.CONFIG_DIR / config_file
        self.schedule = schedule
        self.start_date = start_date or pendulum.datetime(2026,4,1, tz='Europe/Warsaw')
        self.tags = tags or ["Bronze"]
        self.dbt_dag_id = dbt_dag_id

    def _ch(self):
        return clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            port=int(os.getenv("CLICKHOUSE_HTTP_PORT")),
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

    def ensure_table_exists(self, t: dict, ch=None) -> None:
        ch = ch or self._ch()
        _replace = "REPLACE(toDateTime64(assumeNotNull(updated_at), 6) AS updated_at)"
        if t['excluded_columns']:
            select_cols = f"* EXCEPT({','.join(t['excluded_columns'])}) {_replace}"
        else:
            select_cols = f"* {_replace}"
        engine_expr = t['engine'] if '(' in t['engine'] else t['engine'] + '()'
        pg_params = {
            'addr': f"{os.getenv('POSTGRES_ODS_HOST')}:{os.getenv('POSTGRES_ODS_PORT')}",
            'db': os.getenv('POSTGRES_ODS_DB'),
            'tbl': t['src_table'],
            'user': os.getenv('POSTGRES_ODS_USER'),
            'pass': os.getenv('POSTGRES_ODS_PASSWORD'),
            'schema': t['src_schema'],
        }
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.BRONZE_DB}.{t['target']}
            ENGINE = {engine_expr}
            ORDER BY {t['order_by']}
            AS SELECT
                {select_cols},
                now() AS _ingested_at,
                '' AS _source_table
            FROM postgresql(
                %(addr)s, %(db)s, %(tbl)s, %(user)s, %(pass)s, %(schema)s
            )
            WHERE 1 = 0
        """
        ch.command(ddl, parameters=pg_params)

    def get_watermark(self) -> str:
        conn=psycopg2.connect(os.getenv('POSTGRES_CONNECTION'))
        with conn.cursor() as cur:
            cur.execute("""
            SELECT last_loaded FROM meta.bronze_watermark
            WHERE dag_id = %s""", (self.dag_id,))
            row=cur.fetchone()
        conn.close()
        return row[0].strftime('%Y-%m-%d %H:%M:%S') if row else '1970-01-01 00:00:00'

    def save_watermark(self, loaded_at: str) -> None:
        conn=psycopg2.connect(os.getenv('POSTGRES_CONNECTION'))
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO meta.bronze_watermark (dag_id, last_loaded) VALUES (%s, %s)
            ON CONFLICT (dag_id) DO UPDATE SET last_loaded = EXCLUDED.last_loaded""", (self.dag_id, loaded_at))
        conn.commit()
        conn.close()

    def build_plan(self) -> list[dict]:
        cfg = self._load_config()
        defaults = cfg.get('defaults',{})
        plan =[IngestFactory._merge_default(t,defaults) for t in cfg['tables']]
        p = [p for p in plan if p['enabled']]
        return p

    def refresh_table(self, t : dict,watermark: str) -> dict:
        ch = self._ch()
        self.ensure_table_exists(t, ch)
        _replace = "REPLACE(toDateTime64(assumeNotNull(updated_at), 6) AS updated_at)"
        if t['excluded_columns']:
            select_cols = f"* EXCEPT({','.join(t['excluded_columns'])}) {_replace}"
        else:
            select_cols = f"* {_replace}"
        extra_where = f"AND {t['where']}" if t['where'] else ''
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ddl = f"""
            INSERT INTO {self.BRONZE_DB}.{t['target']}
            SELECT
                {select_cols},
                now() as _ingested_at,
                %(src)s as _source_table
            FROM postgresql(
                %(addr)s, %(db)s, %(tbl)s, %(user)s, %(pass)s, %(schema)s
            )
            WHERE updated_at > '{watermark}'
            and updated_at <= '{now}'
            {extra_where}
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
                'target':t['target'],'rows':rows,'now':now }

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
            def refresh_table(t: dict,watermark: str) -> dict:
                return self.refresh_table(t, watermark)

            @task
            def save_watermark(results: list[dict]):
                self.save_watermark(min(r['now'] for r in results))
                self.summary(results)

            @task
            def get_wm() -> str:
                return self.get_watermark()

            @task
            def make_kwargs(tables: list[dict], wm: str) -> list[dict]:
                return [{'t': t, 'watermark': wm} for t in tables]

            trigger_dbt = TriggerDagRunOperator(
                task_id='trigger_dbt',
                trigger_dag_id=self.dbt_dag_id,
                wait_for_completion=False,
                conf={"correlation_id": "{{ dag_run.conf.get('correlation_id', '') }}"},
            ) if self.dbt_dag_id else None

            db = ensure_db()
            tables = plan()
            wm = get_wm()
            kwargs = make_kwargs(tables, wm)
            result = refresh_table.expand_kwargs(kwargs)
            db >> tables >> wm >> kwargs
            if trigger_dbt:
                save_watermark(result) >> trigger_dbt
        return _dag()

