package sink; import dto.Client;



import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class  wCountriesSink implements JdbcStatementBuilder<Client.Nationality> {
    public static final String SQL = "INSERT INTO client.nationality (person_id, country_code, created_at, correlation_id) VALUES (?, ?, ?, ?)";
    @Override
    public void accept(PreparedStatement preparedStatement, Client.Nationality nationality) throws SQLException {
        preparedStatement.setString(1, nationality.getPersonId());
        preparedStatement.setString(2, nationality.getCountryCode());
        preparedStatement.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setString(4, nationality.getCorrelation_id());
    }
}
