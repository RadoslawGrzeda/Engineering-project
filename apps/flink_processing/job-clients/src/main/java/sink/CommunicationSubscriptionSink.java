package sink; import dto.Client;



import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CommunicationSubscriptionSink implements JdbcStatementBuilder<Client.CommunicationSubscription> {
    public static final String SQL = "INSERT INTO communication_subscription (person_id, communication_code, value, date_of_subscription," +
                                    " date_of_unsubscription, reason_of_unsubscription, created_at, updated_at, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    @Override
    public void accept(PreparedStatement preparedStatement, Client.CommunicationSubscription comm) throws SQLException {
        preparedStatement.setString(1, comm.getPersonId());
        preparedStatement.setString(2, comm.getCommunityCode());
        preparedStatement.setString(3, comm.getCommunityCodeValue());
        if (comm.getDateOfSubscription() != null) {
            preparedStatement.setTimestamp(4, Timestamp.valueOf(comm.getDateOfSubscription()));
        } else {
            preparedStatement.setNull(4, java.sql.Types.TIMESTAMP);
        }
        if (comm.getDateOfUnsubscription() != null) {
            preparedStatement.setTimestamp(5, Timestamp.valueOf(comm.getDateOfUnsubscription()));
        } else {
            preparedStatement.setNull(5, java.sql.Types.TIMESTAMP);
        }
        preparedStatement.setString(6, comm.getReasonOfUnsubscription());
        preparedStatement.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setString(9, comm.getCorrelation_id());
    }
}
