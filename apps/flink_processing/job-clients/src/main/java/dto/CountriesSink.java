package dto;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CountriesSink implements JdbcStatementBuilder<Client> {
    public static final String SQL = "INSERT INTO countries (person_id, country_code, country_name, created_at, updated_at) VALUES (?, ?, ? , ?, ?)";
    @Override
    public void accept(PreparedStatement preparedStatement, Client client) throws SQLException {
        preparedStatement.setString(1, client.getPersonId());
        preparedStatement.setString(2, client.getAccount().getCountryCode());
        preparedStatement.setString(3, client.getAccount().getCountryName());
        preparedStatement.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
    }
}
