package sink; import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.Date;
import java.sql.PreparedStatement;

public class LoyaltyStatusSink extends JdbcProcessSink<Client> {
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("loyalty_status_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.loyalty_status (identifier_id, person_id, status_code, is_current, start_date, end_date, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?, ?)" +
                                    " ON CONFLICT (identifier_id, person_id, status_code, start_date) DO UPDATE SET" +
                                    " is_current = EXCLUDED.is_current," +
                                    " end_date = EXCLUDED.end_date," +
                                    " correlation_id = EXCLUDED.correlation_id";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client client) -> {
            Client.Loyalty loyalty = client.getLoyalty();
            preparedStatement.setString(1, client.getPersonId());
            preparedStatement.setString(2, client.getPersonId());
            preparedStatement.setString(3, loyalty.getLoyaltyStatus());
            preparedStatement.setBoolean(4, true);
            if (loyalty.getStartDate() != null) {
                preparedStatement.setDate(5, Date.valueOf(loyalty.getStartDate()));
            } else {
                preparedStatement.setNull(5, java.sql.Types.DATE);
            }
            if (loyalty.getEndDate() != null) {
                preparedStatement.setDate(6, Date.valueOf(loyalty.getEndDate()));
            } else {
                preparedStatement.setNull(6, java.sql.Types.DATE);
            }
            preparedStatement.setString(7, client.getAccount().getCorrelation_id());
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
