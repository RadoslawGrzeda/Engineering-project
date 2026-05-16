package sink; import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;

public class LoyaltyStatusSink extends JdbcProcessSink<Client> {
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("loyalty_status_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.loyalty_status (identifier_id, person_id, status_code, correlation_id)" +
                                    " VALUES (?, ?, ?, ?)" +
                                    " ON CONFLICT (identifier_id) DO UPDATE SET" +
                                    " person_id = EXCLUDED.person_id," +
                                    " status_code = EXCLUDED.status_code," +
                                    " correlation_id = EXCLUDED.correlation_id";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client client) -> {
            Client.Loyalty loyalty = client.getLoyalty();
            preparedStatement.setString(1, loyalty.getIdentifierId());
            preparedStatement.setString(2, client.getPersonId());
            preparedStatement.setString(3, loyalty.getStatusCode());
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
        return "Error in LoyaltyStatus Sink";
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
