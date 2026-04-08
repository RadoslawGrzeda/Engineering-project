package sink; import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CustomerIndicatorSink extends JdbcProcessSink<Client> {
    public  static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("customer_indicator_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.customer_indicator (person_id, type, is_active, created_at, updated_at, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?)" +
                                    " ON CONFLICT (person_id, type) DO UPDATE SET" +
                                    " is_active = EXCLUDED.is_active," +
                                    " updated_at = EXCLUDED.updated_at," +
                                    " correlation_id = EXCLUDED.correlation_id";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client client) -> {
            Client.AccountIndicator indicator = client.getAccountIndicators();
            preparedStatement.setString(1, client.getPersonId());
            preparedStatement.setString(2, indicator.getTypeAccountIndicator());
            if (indicator.getIsDeleted() != null) {
                preparedStatement.setBoolean(3, !indicator.getIsDeleted());
            } else {
                preparedStatement.setBoolean(3, true);
            }
            preparedStatement.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setString(6, client.getAccount().getCorrelation_id());
        };
    }

    @Override
    protected String getPersonId(Client client) {
        return client.getPersonId();
    }

    @Override
    protected String getCorrelation_id(Client client) {
        return client.getAccount().getCorrelation_id();
    }

    @Override
    protected String errorTag() {
        return "Error in CustomerIndicator Sink";
    }

    @Override
    protected String getSourceApplication() {
        return "CLIENTS";
    }

    @Override
    protected OutputTag<DeadLetter> getDeadLetterTag() {
        return DEAD_LETTER;
    }
}
