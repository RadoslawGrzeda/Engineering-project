package dto;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CivilSink implements JdbcStatementBuilder<Client> {
    public static final String SQL = "INSERT INTO client.civil (person_id, civil_status, is_current, created_at, updated_at, correlation_id) VALUES (?, ?, ?, ?, ?, ?)";


    @Override
    public void accept(PreparedStatement preparedStatement, Client client) throws SQLException {
        preparedStatement.setString(1, client.getPersonId());
        preparedStatement.setString(2, client.getAccount().getCivilStatus());
        preparedStatement.setBoolean(3, true);
        preparedStatement.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setString(6, client.getAccount().getCorrelation_id());
    }
}
