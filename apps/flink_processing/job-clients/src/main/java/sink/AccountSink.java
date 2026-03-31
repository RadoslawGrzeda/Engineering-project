package sink; import dto.Client;




import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class AccountSink implements JdbcStatementBuilder<Client> {

    public static final String SQL = "INSERT INTO customer (" +
                                    "first_name, middle_name, last_name, birth_date, passport_number, gender_code," +
                                    "registration_date, creation_application, created_at, updated_at, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public void accept(PreparedStatement statement, Client client) throws SQLException {
        statement.setString(1, client.getAccount().getFirstName());
        statement.setString(2, client.getAccount().getMiddleName());
        statement.setString(3, client.getAccount().getLastName());
        statement.setDate(4, client.getAccount().getBirthDate());
        statement.setString(5, client.getAccount().getPassportNumber() == null ? null : client.getAccount().getPassportNumber().toUpperCase());
        statement.setString(6, client.getAccount().getGenderCode());
        statement.setDate(7, client.getAccount().getRegistrationDate());
        statement.setString(8, client.getAccount().getCreationApplication());
        statement.setTimestamp(9, Timestamp.valueOf(LocalDateTime.now()));
        statement.setTimestamp(10, Timestamp.valueOf(LocalDateTime.now()));
        statement.setString(11, client.getAccount().getCorrelation_id());
    }

}
