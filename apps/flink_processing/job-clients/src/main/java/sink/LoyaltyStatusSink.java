package sink; import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LoyaltyStatusSink extends JdbcProcessSink<Client> {
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("loyalty_status_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.loyalty_status (identifier_id, status_code, is_current, start_date, end_date, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?)";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client client) -> {
            Client.Loyalty loyalty = client.getLoyalty();
            preparedStatement.setString(1, client.getPersonId());
            preparedStatement.setString(2, loyalty.getLoyaltyStatus());
            preparedStatement.setBoolean(3, true);
            if (loyalty.getStartDate() != null) {
                preparedStatement.setDate(4, Date.valueOf(loyalty.getStartDate()));
            } else {
                preparedStatement.setNull(4, java.sql.Types.DATE);
            }
            if (loyalty.getEndDate() != null) {
                preparedStatement.setDate(5, Date.valueOf(loyalty.getEndDate()));
            } else {
                preparedStatement.setNull(5, java.sql.Types.DATE);
            }
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
