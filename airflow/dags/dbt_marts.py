import os

import pendulum
from airflow.decorators import dag, task_group
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DBT_IMAGE = os.getenv("DBT_IMAGE", "engineering-dbt:1.8")
DBT_NETWORK = os.getenv("DBT_NETWORK", "traefik_network")
DBT_PROJECT_HOST_PATH = os.getenv("DBT_PROJECT_HOST_PATH")


def _dbt_op(task_id: str, select: str) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        command=f"run --select {select} --threads 2 --full-refresh --profiles-dir /dbt",
        network_mode=DBT_NETWORK,
        mounts=[
            Mount(source=f"{DBT_PROJECT_HOST_PATH}/models", target="/dbt/models", type="bind"),
            Mount(source=f"{DBT_PROJECT_HOST_PATH}/macros", target="/dbt/macros", type="bind"),
        ],
        environment={
            "DBT_CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": os.getenv("CLICKHOUSE_HTTP_PORT", "8123"),
            "DBT_CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
            "DBT_CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        },
        force_pull=False,
        auto_remove="success",
        mount_tmp_dir=False,
    )


@dag(
    dag_id="dbt_marts",
    default_args={
        "owner": "data_engineer",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=10),
    },
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 1, tz="Europe/Warsaw"),
    catchup=False,
    max_active_runs=1,
    tags=["marts", "dbt", "shop", "product", "client"],
)
def dbt_marts():

    @task_group(group_id="shop")
    def shop_marts():
        _dbt_op("dim_store",         "path:models/marts/store/dim_store.sql")
        _dbt_op("dim_store_history", "path:models/marts/store/dim_store_history.sql")

    @task_group(group_id="product")
    def product_marts():
        _dbt_op("dim_product",              "path:models/marts/product/dim_product.sql")
        _dbt_op("dim_product_history",      "path:models/marts/product/dim_product_history.sql")
        _dbt_op("dim_chief",                "path:models/marts/product/dim_chief.sql")
        _dbt_op("dim_chief_history",        "path:models/marts/product/dim_chief_history.sql")
        _dbt_op("dim_pos_information",      "path:models/marts/product/dim_pos_information.sql")
        _dbt_op("dim_pos_information_history", "path:models/marts/product/dim_pos_information_history.sql")
        _dbt_op("dim_segment_chief",        "path:models/marts/product/dim_segment_chief.sql")
        _dbt_op("dim_segment_chief_history","path:models/marts/product/dim_segment_chief_history.sql")

    @task_group(group_id='client')
    def client_marts():
        _dbt_op("dim_customer", "path:models/marts/client/dim_customer.sql")
        _dbt_op("dim_customer_history", "path:models/marts/client/dim_customer_history.sql")
        _dbt_op("dim_address", "path:models/marts/client/dim_address.sql")
        _dbt_op("dim_address_history", "path:models/marts/client/dim_address_history.sql")
        _dbt_op("dim_contact", "path:models/marts/client/dim_contact.sql")
        _dbt_op("dim_contact_history", "path:models/marts/client/dim_contact_history.sql")
        _dbt_op("dim_digital_access", "path:models/marts/client/dim_digital_access.sql")
        _dbt_op("dim_digital_access_history", "path:models/marts/client/dim_digital_access_history.sql")
        _dbt_op("fct_indicator_history", "path:models/marts/client/fct_indicator_history.sql")
        _dbt_op("fct_indicator", "path:models/marts/client/fct_indicator.sql")
        _dbt_op("fct_loyalty_history", "path:models/marts/client/fct_loyalty_history.sql")
        _dbt_op("fct_loyalty", "path:models/marts/client/fct_loyalty.sql")
        _dbt_op("fct_subscription_history", "path:models/marts/client/fct_subscription_history.sql")
        _dbt_op("fct_subscription", "path:models/marts/client/fct_subscription.sql")

    [shop_marts(), product_marts(), client_marts()]


dag = dbt_marts()
