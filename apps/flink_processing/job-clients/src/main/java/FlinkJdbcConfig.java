import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;

public class FlinkJdbcConfig {
    private static final String POSTGRES_URL = "jdbc:postgresql://host.docker.internal:5433/ods";
    private static final String POSTGRES_USER = "ods";
    private static final String POSTGRES_PASSWORD = "ods";


   public static  JdbcExecutionOptions execOption() {
       return JdbcExecutionOptions.builder()
               .withBatchSize(1000)
               .withBatchIntervalMs(200)
               .withMaxRetries(5)
               .build();
   }

   public static JdbcConnectionOptions connOption(){
    return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(POSTGRES_URL)
            .withDriverName("org.postgresql.Driver")
            .withUsername(POSTGRES_USER)
            .withPassword(POSTGRES_PASSWORD)
            .build();
   }
}
