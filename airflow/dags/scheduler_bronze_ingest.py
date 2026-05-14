import pendulum
from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def _trigger_dag(task_id: str, dag_id: str, wait_for_completion: bool = True) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        wait_for_completion=wait_for_completion,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        conf={"correlation_id": "{{ dag_run.conf.get('correlation_id', dag_run.run_id) }}"},
    )


@dag(
    dag_id="scheduler_bronze_ingest",
    default_args={
        "owner": "data_engineer",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 4, 1, tz="Europe/Warsaw"),
    catchup=False,
    max_active_runs=1,
    tags=["scheduler", "bronze", "dbt"],
)
def scheduler_bronze_ingest():
    bronze_product = _trigger_dag("bronze_product_ingest", "bronze_product_ingest")
    bronze_store = _trigger_dag("bronze_store_ingest", "bronze_store_ingest")
    bronze_client = _trigger_dag("bronze_client_ingest", "bronze_client_ingest")

    # dbt_product = _trigger_dag("dbt_transform_product", "dbt_transform_product")
    # dbt_store = _trigger_dag("dbt_transform_store", "dbt_transform_store")
    dbt_marts = _trigger_dag("dbt_marts", "dbt_marts")

    [bronze_product, bronze_store, bronze_client] >>  dbt_marts


dag = scheduler_bronze_ingest()
