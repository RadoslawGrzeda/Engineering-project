package sink; import dto.Client;



import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CustomerIndicatorSink implements JdbcStatementBuilder<Client> {

    public static final String SQL = "INSERT INTO client.customer_indicator (person_id, type, is_active, created_at, updated_at, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?)";

    @Override
    public void accept(PreparedStatement preparedStatement, Client client) throws SQLException {
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
    }
}
