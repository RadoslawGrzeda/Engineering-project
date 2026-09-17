# Deployment runbook (laptop → VPS)

This document covers the practical side of a deployment: secrets, DNS, start-up order, SSO configuration and the smoke test. For what the platform is and how it works, see the [README](../README.md).

## 1. Prerequisites

- Docker + Docker Compose v2
- JDK 17 + Maven (only needed to build the Flink jobs)
- A domain you control, with DNS managed — the examples below use `<domain>`
- Inbound firewall open on **22 (SSH), 80 and 443 only**:

```bash
ufw default deny incoming && ufw allow 22 && ufw allow 80 && ufw allow 443 && ufw enable
```

Everything else — Kafka, MinIO, ClickHouse, PostgreSQL, the Flink UI, Elasticsearch, Kibana, Airflow — is reached through Traefik over HTTPS or stays internal to `traefik_network`. Do not publish those ports; bind them to `127.0.0.1` where a host port is needed at all.

## 2. Secrets

No real `.env` file is committed. Every service ships a template:

```bash
# from the repository root: create each .env from its template, then edit the CHANGE_ME values
find . -name '.env.example' -not -path './.venv*/*' | while read f; do
  cp -n "$f" "${f%.example}"
done
```

Some secrets are shared between services and **must match** across files:

| Secret | Files that have to agree |
|---|---|
| ODS PostgreSQL password | `ods/.env`, `flink/.env`, `airflow/.env`, `service/streamlit/.env`, `service/*/.env` |
| MinIO root user / password | `minIO/.env`, `backup/.env`, `airflow/.env`, `flink/.env`, `service/streamlit/.env` |
| ClickHouse `flink_transaction_sink` password | `flink/.env` ↔ `clickhouse/config/users.xml` |
| ClickHouse `dbt_runner` password | `airflow/.env` ↔ `clickhouse/config/users.xml` |
| ClickHouse `default` password | `clickhouse/config/users.xml` (locked down — replace the placeholder) |

`clickhouse/config/users.xml` is git-ignored and holds literal passwords; edit it directly and keep it in sync with the `.env` files.

Two paths in `airflow/.env` are host-specific and have to be absolute on the target machine: `DBT_PROJECT_HOST_PATH` (the `dbt/` directory) and `BACKUP_HOST_PATH` (where dumps are written before upload).

## 3. DNS

Add an `A` record pointing at the VPS for every host used in the Traefik labels (replace the domain across the compose files and `.env` files):

```
auth.<domain>              # Keycloak
auth-proxy.<domain>        # ForwardAuth callback
traefik.<domain>           # Traefik dashboard (SSO-protected)
streamlit.<domain>
minio-api.<domain>         minio-ui.<domain>
minio-backup-api.<domain>  minio-backup-ui.<domain>
redpanda.<domain>
clickhouse.<domain>
kibana.<domain>
```

## 4. Start-up order

The shared network is `external: true` in every compose file, so create it once:

```bash
docker network create traefik_network
```

Then bring the layers up in this order (network and dependency constraints):

```bash
cd traefik                   && docker compose up -d   # 1. reverse proxy + ForwardAuth
#                                                       2. keycloak — see §5 (kept outside the repo)
cd ../kafka                  && docker compose up -d   # 3. broker quorum (KRaft)
cd ../minIO                  && docker compose up -d   # 4. needs Kafka for event notifications
cd ../ods                    && docker compose up -d   # 5. operational PostgreSQL
cd ../clickhouse             && docker compose up -d
cd ../flink                  && docker compose up -d
cd ../airflow                && docker compose up -d
cd ../elk                    && docker compose up -d
cd ../cloudbeaver            && docker compose up -d
cd ../backup                 && docker compose up -d
cd ../service/streamlit      && docker compose up -d
# consumers and generators last
cd ../kafka_minio_consumer   && docker compose up -d
cd ../geocoding_crm_address  && docker compose up -d
cd ../transaction_generator  && docker compose up -d
cd ../crm_generator/producer && docker compose up -d
cd ../updater                && docker compose up -d
```

After MinIO is up, register the Kafka notification target (`mc admin config set … notify_kafka`) and add the bucket event rule, so that a new object publishes to the `minio-events` topic.

Then build and submit the Flink jobs and build the dbt image as described in the [README](../README.md#running-the-platform).

## 5. Keycloak SSO (ForwardAuth)

The Keycloak stack itself (its compose file and the realm export) is not part of this repository — the realm holds real user accounts, so it lives outside version control. On the Traefik side, a `traefik-forward-auth` container defines the reusable `keycloak-auth@docker` middleware that protects the Traefik dashboard, ClickHouse, Redpanda Console and the MinIO consoles. It stays inert until it is configured:

1. Set strong `KC_DB_PASSWORD` and `KC_ADMIN_PASSWORD` in `keycloak/.env`.
2. Start Keycloak; it imports the `engineering` realm on first run.
3. In the Keycloak admin console: realm `engineering` → Clients → `traefik-forwardauth` → Credentials → copy the client secret.
4. In `traefik/.env`: set `KC_CLIENT_SECRET` to that value, `FORWARD_AUTH_SECRET=$(openssl rand -hex 32)`, and `DOMAIN_NAME` / `KC_HOSTNAME` to your domain.
5. `cd traefik && docker compose up -d` to reload ForwardAuth.

Until steps 3–5 are done, SSO-protected routes return an authentication error — that is expected, not a regression.

A second instance (`keycloak-auth-analyst`) shares the same realm and is attached to CloudBeaver only, which is what gives the analyst role access to the SQL client and nothing else.

## 6. Backups

- **MinIO** — `backup/` runs a second MinIO instance with `mc mirror --watch` and a 30-day lifecycle rule.
- **PostgreSQL + ClickHouse** — the Airflow DAG `database_backup` (`0 4 * * *`) runs `pg_dump -Fc` and a native ClickHouse `BACKUP`, uploading both to the backup MinIO bucket `database-backups/` with `BACKUP_RETENTION_DAYS` retention.
- **Off-site caveat** — the backup instance is co-located on the same host, so losing the host still loses the backups. For a real off-site copy, run the `backup/` stack (or an extra `mc mirror`) on a separate machine or external S3 and point `MINIO_BACKUP_ENDPOINT` there.

## 7. Smoke test

```bash
docker ps --format '{{.Names}}\t{{.Status}}' | sort        # everything Up / healthy
curl -fsS https://streamlit.<domain> >/dev/null && echo "streamlit OK"
curl -fsS https://redpanda.<domain>  >/dev/null && echo "redpanda OK"   # 302 → Keycloak means SSO works
docker logs flink-jobmanager --tail 20                      # both jobs RUNNING
```

## 8. Application tests

```bash
cd service/streamlit         && pytest tests/ -v
cd service/kafka_minio_consumer/load_file_develop && pytest tests/ -v
```
