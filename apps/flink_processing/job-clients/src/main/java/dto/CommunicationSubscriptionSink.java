package dto;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CommunicationSubscriptionSink implements JdbcStatementBuilder<Client.CommunicationSubscription> {
    public static final String SQL = "INSERT INTO client.communication_subscription (person_id, community_code, community_value, date_of_subscription," +
                                    " date_of_unsubscription, created_at, updated_at, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    @Override
    public void accept(PreparedStatement preparedStatement, Client.CommunicationSubscription comm) throws SQLException {
        preparedStatement.setString(1, comm.getPersonId());
        preparedStatement.setString(2, comm.getCommunityCode());
        preparedStatement.setString(3, comm.getCommunityCodeValue());
        preparedStatement.setTimestamp(4, Timestamp.valueOf(comm.getDateOfSubscription()));
        preparedStatement.setTimestamp(5, comm.getDateOfUnsubscription() == null ? null : Timestamp.valueOf(comm.getDateOfUnsubscription()));
        preparedStatement.setTimestamp(6, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setString(8, comm.getCorrelation_id());
    }
}
