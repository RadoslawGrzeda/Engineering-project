package config;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;

public class FlinkPostgresConfig {

    private static final String POSTGRES_URL = System.getenv("POSTGRES_URL");
    private static final String POSTGRES_USER = System.getenv("POSTGRES_USER");
    private static final String POSTGRES_PASSWORD = System.getenv("POSTGRES_PASSWORD");

    public static JdbcExecutionOptions execOption() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(500)
                .withMaxRetries(5)
                .build();
    }

    public static JdbcConnectionOptions connOption() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(POSTGRES_URL)
                .withDriverName("org.postgresql.Driver")
                .withUsername(POSTGRES_USER)
                .withPassword(POSTGRES_PASSWORD)
                .build();
    }
}
