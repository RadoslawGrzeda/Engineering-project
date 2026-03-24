package dto;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class GenderSink implements JdbcStatementBuilder<Client> {

    public static final String SQL =  "INSERT INTO gender (person_id, gender_code, gender_name, created_at) VALUES (?, ?, ?, ?)";
    @Override
    public void accept(PreparedStatement preparedStatement, Client client) throws SQLException {
                preparedStatement.setString(1, client.getPersonId());
                preparedStatement.setString(2, client.getAccount().getGenderCode());
                preparedStatement.setString(3, client.getAccount().getGenderCode().equals('F') ? "Female" : "Male");
                preparedStatement.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
    }
}
