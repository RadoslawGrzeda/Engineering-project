package dto;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class AccountSink implements JdbcStatementBuilder<Client> {

    public static final String SQL= "INSERT INTO client.account (" +
                                    "person_id, first_name, middle_name, last_name, birth_date, passport_number," +
                                    "registration_date, creation_application, created_at, updated_at, correlation_id)" +
                                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public void accept(PreparedStatement statement, Client client) throws SQLException {
        statement.setString(1, client.getPersonId());
        statement.setString(2, client.getAccount().getFirstName());
        statement.setString(3, client.getAccount().getMiddleName());
        statement.setString(4, client.getAccount().getLastName());
        statement.setDate(5, client.getAccount().getBirthDate());
        statement.setString(6, client.getAccount().getPassportNumber() == null ? null : client.getAccount().getPassportNumber().toUpperCase());
        statement.setDate(7, client.getAccount().getRegistrationDate());
        statement.setString(8, client.getAccount().getCreationApplication());
        statement.setTimestamp(9, Timestamp.valueOf(LocalDateTime.now()));
        statement.setTimestamp(10, Timestamp.valueOf(LocalDateTime.now()));
        statement.setString(11, client.getAccount().getCorrelation_id());

    }

}
