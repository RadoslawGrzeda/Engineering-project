package dto;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class DigitalAccessSink implements JdbcStatementBuilder<Client> {
    public static final String SQL = "INSERT INTO client.digital_access ( person_id, username, email_user, is_active, last_login_date, portal_user_confirmation_date, created_at, updated_at, correlation_id) VALUES (?, ?, ?, ?, ?,?,?, ?, ?)";

    @Override
    public void accept(PreparedStatement preparedStatement, Client client) throws SQLException {
preparedStatement.setString(1, client.getPersonId());
preparedStatement.setString(2, client.getDigitalAccess().getUsername());
preparedStatement.setString(3, client.getDigitalAccess().getEmailUser());
preparedStatement.setBoolean(4, client.getDigitalAccess().getIsActive());
preparedStatement.setTimestamp(5, Timestamp.valueOf(client.getDigitalAccess().getLastLoginDate()));
preparedStatement.setTimestamp(6, Timestamp.valueOf(client.getDigitalAccess().getPortalUserConfirmationDate()));
preparedStatement.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));
preparedStatement.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
preparedStatement.setString(9, client.getAccount().getCorrelation_id());

    }
}
