package sink; import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class DigitalAccessSink extends JdbcProcessSink<Client> {
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("digital_access_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.digital_access (person_id, username, email_user, is_active, last_login_date, portal_user_confirmation_date, created_at, updated_at, correlation_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client client) -> {
            Client.DigitalAccess da = client.getDigitalAccess();
            preparedStatement.setString(1, client.getPersonId());
            preparedStatement.setString(2, da.getUsername());
            preparedStatement.setString(3, da.getEmailUser());
            if (da.getIsActive() != null) {
                preparedStatement.setBoolean(4, da.getIsActive());
            } else {
                preparedStatement.setNull(4, java.sql.Types.BOOLEAN);
            }
            if (da.getLastLoginDate() != null) {
                preparedStatement.setTimestamp(5, Timestamp.valueOf(da.getLastLoginDate()));
            } else {
                preparedStatement.setNull(5, java.sql.Types.TIMESTAMP);
            }
            if (da.getPortalUserConfirmationDate() != null) {
                preparedStatement.setTimestamp(6, Timestamp.valueOf(da.getPortalUserConfirmationDate()));
            } else {
                preparedStatement.setNull(6, java.sql.Types.TIMESTAMP);
            }
            preparedStatement.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setString(9, client.getAccount().getCorrelation_id());
        };
    }

    @Override
    protected String getPersonId(Client client) {
        return client.getPersonId();
    }

    @Override
    protected String getCorrelation_id(Client client) {
        return client.getAccount().getCorrelation_id();
    }

    @Override
    protected String errorTag() {
        return "Error in DigitalAccess Sink";
    }

    @Override
    protected String getSourceApplication() {
        return "CLIENTS";
    }

    @Override
    protected OutputTag<DeadLetter> getDeadLetterTag() {
        return DEAD_LETTER;
    }
}
