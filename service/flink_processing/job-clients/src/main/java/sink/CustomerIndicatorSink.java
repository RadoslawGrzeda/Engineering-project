package sink; import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;

public class CustomerIndicatorSink extends JdbcProcessSink<Client> {
    public  static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("customer_indicator_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.customer_indicator (person_id, type, is_active, correlation_id)" +
                                    " VALUES (?, ?, ?, ?)" +
                                    " ON CONFLICT (person_id, type) DO UPDATE SET" +
                                    " is_active = EXCLUDED.is_active," +
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
            preparedStatement.setString(2, indicator.getType());
            if (indicator.getIsActive() != null) {
                preparedStatement.setBoolean(3, indicator.getIsActive());
            } else {
                preparedStatement.setBoolean(3, true);
            }
            preparedStatement.setString(4, client.getAccount().getCorrelation_id());
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
