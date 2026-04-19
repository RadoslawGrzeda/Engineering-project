package config;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;

public class FlinkClickHouseConfig {
    private static final String CLICKHOUSE_URL = System.getenv("CLICKHOUSE_URL");
    private static final String CLICKHOUSE_USER = System.getenv("CLICKHOUSE_USER");
    private static final String CLICKHOUSE_PASSWORD = System.getenv("CLICKHOUSE_PASSWORD");

    public static JdbcExecutionOptions execOption() {
        return JdbcExecutionOptions.builder()
               .withBatchSize(1000)
               .withBatchIntervalMs(200)
               .withMaxRetries(5)
               .build();
   }

    public static JdbcConnectionOptions connOption(){
            return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(CLICKHOUSE_URL)
            .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
            .withUsername(CLICKHOUSE_USER)
            .withPassword(CLICKHOUSE_PASSWORD)
            .build();
   }
}
