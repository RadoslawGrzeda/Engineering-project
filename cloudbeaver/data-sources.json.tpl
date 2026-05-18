{
    "folders": {},
    "connections": {
        "local-postgres-ods": {
            "provider": "postgresql",
            "driver": "postgres-jdbc",
            "name": "ODS – PostgreSQL",
            "save-password": true,
            "read-only": false,
            "configuration": {
                "host": "postgres_ods",
                "port": "5432",
                "database": "${POSTGRES_DB}",
                "user": "${POSTGRES_USER}",
                "password": "${POSTGRES_PASSWORD}",
                "configurationType": "MANUAL"
            }
        },
        "local-clickhouse": {
            "provider": "generic",
            "driver": "clickhouse_v2",
            "name": "ClickHouse – Analytics",
            "save-password": true,
            "read-only": false,
            "configuration": {
                "host": "clickhouse",
                "port": "8123",
                "database": "${CLICKHOUSE_DB}",
                "user": "${CLICKHOUSE_USER}",
                "password": "${CLICKHOUSE_PASSWORD}",
                "configurationType": "MANUAL"
            }
        }
    }
}
