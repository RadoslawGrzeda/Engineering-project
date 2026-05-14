import os

import pendulum
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DBT_IMAGE = os.getenv("DBT_IMAGE", "engineering-dbt:1.8")
DBT_NETWORK = os.getenv("DBT_NETWORK", "traefik_network")
DBT_PROJECT_HOST_PATH = os.getenv("DBT_PROJECT_HOST_PATH")
FULL_REFRESH = os.getenv("DBT_FULL_REFRESH", "false").lower() == "true"

def _dbt_op(task_id: str, select: str) -> DockerOperator:
    full_refresh_flag = " --full-refresh" if FULL_REFRESH else ""
    return DockerOperator(
        task_id = task_id,
        image = DBT_IMAGE,
        command = f"run --select {select}{full_refresh_flag} --threads 2 --profiles-dir /dbt",
        network_mode = DBT_NETWORK,
        mounts = [
            Mount(source = f"{DBT_PROJECT_HOST_PATH}/models", target = "/dbt/models", type = "bind"),
            Mount(source = f"{DBT_PROJECT_HOST_PATH}/macros", target = "/dbt/macros", type = "bind"),
        ],
        environment = {
            "DBT_CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": os.getenv("CLICKHOUSE_HTTP_PORT", "8123"),
            "DBT_CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
            "DBT_CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        },
        force_pull = False,
        auto_remove = "success",
        mount_tmp_dir = False,   
    )

@dag(
    dag_id = "dbt_transform_client",
    default_args = {
        "owner": "data_engineer",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    schedule = None,
    start_date = pendulum.datetime(2026, 4, 1, tz = "Europe/Warsaw"),
    catchup = False,
    max_active_runs = 1,
    tags = ["silver", "gold", "dbt", "client"],
)
def dbt_transform_client():
    silver = _dbt_op("silver", "path:models/silver/client")
    gold = _dbt_op("gold", "path:models/gold/client")
    silver >> gold

dag = dbt_transform_client()