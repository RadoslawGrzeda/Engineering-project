package Dto;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class AccountSink implements JdbcStatementBuilder<Client> {

    public static final String SQL= "INSERT INTO account (" +
                                    "person_id, first_name, middle_name, last_name, birth_date, passport_number, registration_date, creation_application, created_at, updated_at)" +
                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public void accept(PreparedStatement statement, Client client) throws SQLException {
        statement.setString(1, client.getPersonId());
        statement.setString(2, client.getAccount().getFirstName());
        statement.setString(3, client.getAccount().getMiddleName());
        statement.setString(4, client.getAccount().getLastName());
        statement.setDate(5, client.getAccount().getBirthDate());
        statement.setString(6, client.getAccount().getPassportNumber() == null ? null : client.getAccount().getPassportNumber().toUpperCase());
        statement.setDate(7, client.getAccount().getRegistrationDate());
        statement.setString(8, client.getAccount().getCreationApplication());
        statement.setDate(9, Date.valueOf(LocalDate.now()));
        statement.setDate(10, Date.valueOf(LocalDate.now()));

    }

}
