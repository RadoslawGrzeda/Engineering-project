package sink; import dto.Client;



import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LoyaltyStatusSink implements JdbcStatementBuilder<Client> {

    public static final String SQL = "INSERT INTO loyalty_status (identifier_id, status_code, is_current, start_date, end_date, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?)";

    @Override
    public void accept(PreparedStatement preparedStatement, Client client) throws SQLException {
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
    }
}
