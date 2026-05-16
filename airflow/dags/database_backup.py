import os
import textwrap

import pendulum
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

NETWORK = os.getenv("DBT_NETWORK")
HOST_BACKUP_PATH = os.getenv("BACKUP_HOST_PATH")
BUCKET = os.getenv("BACKUP_BUCKET")
RETENTION_DAYS = os.getenv("BACKUP_RETENTION_DAYS") or "30"
# Use in DockerOperator `environment` (templated) so filenames match across tasks.
BACKUP_STAMP_TEMPLATE = "{{ ts_nodash }}"

MINIO_MC_IMAGE = os.getenv("MINIO_MC_IMAGE")
POSTGRES_IMAGE = os.getenv("POSTGRES_BACKUP_IMAGE")
CLICKHOUSE_IMAGE = os.getenv("CLICKHOUSE_BACKUP_IMAGE")

LOCAL_MOUNT = [Mount(source=HOST_BACKUP_PATH, target="/backups", type="bind")]


def _task(task_id, image, command, env, mounts=None):
    return DockerOperator(
        task_id=task_id,
        image=image,
        entrypoint=["/bin/sh", "-c"],
        command=[command],
        network_mode=NETWORK,
        environment={k: v for k, v in env.items() if v is not None},
        mounts=mounts or [],
        force_pull=False,
        auto_remove="success",
        mount_tmp_dir=False,
    )


@dag(
    dag_id="database_backup",
    default_args={
        "owner": "data_engineer",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=10),
    },
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 4, 1, tz="Europe/Warsaw"),
    catchup=False,
    max_active_runs=1,
    tags=["backup", "postgres", "clickhouse"],
)
def database_backup():
    postgres_dump = _task(
        task_id="postgres_dump",
        image=POSTGRES_IMAGE,
        command=textwrap.dedent(
            """
            set -eu
            mkdir -p /backups/postgres
            find /backups/postgres -maxdepth 1 -name "*.dump" -mtime +%s -delete
            pg_dump \\
              -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \\
              -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc \\
              -f "/backups/postgres/${POSTGRES_DB}_${BACKUP_STAMP}.dump"
            """
            % (int(RETENTION_DAYS),)
        ).strip(),
        env={
            "PGPASSWORD": os.getenv("POSTGRES_ODS_PASSWORD"),
            "POSTGRES_HOST": os.getenv("POSTGRES_ODS_HOST"),
            "POSTGRES_PORT": os.getenv("POSTGRES_ODS_PORT"),
            "POSTGRES_USER": os.getenv("POSTGRES_ODS_USER"),
            "POSTGRES_DB": os.getenv("POSTGRES_ODS_DB"),
            "BACKUP_STAMP": BACKUP_STAMP_TEMPLATE,
        },
        mounts=LOCAL_MOUNT,
    )

    postgres_upload = _task(
        task_id="postgres_upload",
        image=MINIO_MC_IMAGE,
        command=textwrap.dedent(
            f"""
            set -eu
            mc alias set s3 "$MINIO_ENDPOINT" "$MINIO_USER" "$MINIO_PASSWORD"
            mc mb --ignore-existing "s3/{BUCKET}"
            mc cp "/backups/postgres/${{POSTGRES_DB}}_${{BACKUP_STAMP}}.dump" "s3/{BUCKET}/postgres/"
            mc ls "s3/{BUCKET}/postgres/" || true
            mc rm --recursive --force --older-than {RETENTION_DAYS}d "s3/{BUCKET}/postgres/" || true
            """
        ).strip(),
        env={
            "POSTGRES_DB": os.getenv("POSTGRES_ODS_DB"),
            "MINIO_ENDPOINT": os.getenv("MINIO_BACKUP_ENDPOINT"),
            "MINIO_USER": os.getenv("MINIO_BACKUP_USER"),
            "MINIO_PASSWORD": os.getenv("MINIO_BACKUP_PASSWORD"),
            "BACKUP_STAMP": BACKUP_STAMP_TEMPLATE,
        },
        mounts=LOCAL_MOUNT,
    )

    clickhouse_backup = _task(
        task_id="clickhouse_backup",
        image=CLICKHOUSE_IMAGE,
        # Avoid "}}" after shell ")" — Airflow Jinja-templates `command` and treats "}}" as
        # closing "{{", which broke the old "${VAR:-$(...|grep...)}" one-liner.
        command=textwrap.dedent(
            f"""
            set -eu
            if [ -n "${{CLICKHOUSE_BACKUP_DATABASES:-}}" ]; then
              dbs="${{CLICKHOUSE_BACKUP_DATABASES}}"
            else
              dbs=$(clickhouse-client \\
                --host "$CH_HOST" --port "$CH_PORT" \\
                --user "$CH_USER" --password "$CH_PASSWORD" \\
                --query "SHOW DATABASES" \\
                | grep -Ev '^system$|^INFORMATION_SCHEMA$|^information_schema$')
            fi
            for db in $dbs; do
              clickhouse-client \\
                --host "$CH_HOST" --port "$CH_PORT" \\
                --user "$CH_USER" --password "$CH_PASSWORD" \\
                --query "BACKUP DATABASE \\`$db\\` TO S3('${{MINIO_ENDPOINT}}/{BUCKET}/clickhouse/$db/$BACKUP_STAMP', '${{MINIO_USER}}', '${{MINIO_PASSWORD}}')"
            done
            """
        ).strip(),
        env={
            "CH_HOST": os.getenv("CLICKHOUSE_HOST"),
            "CH_PORT": os.getenv("CLICKHOUSE_PORT"),
            "CH_USER": os.getenv("CLICKHOUSE_USER"),
            "CH_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
            "CLICKHOUSE_BACKUP_DATABASES": os.getenv("CLICKHOUSE_BACKUP_DATABASES"),
            "MINIO_ENDPOINT": os.getenv("MINIO_BACKUP_ENDPOINT"),
            "MINIO_USER": os.getenv("MINIO_BACKUP_USER"),
            "MINIO_PASSWORD": os.getenv("MINIO_BACKUP_PASSWORD"),
            "BACKUP_STAMP": BACKUP_STAMP_TEMPLATE,
        },
        mounts=[],
    )

    postgres_dump >> postgres_upload >> clickhouse_backup


dag = database_backup()
