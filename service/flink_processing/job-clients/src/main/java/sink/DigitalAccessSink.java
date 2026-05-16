package sink; import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class DigitalAccessSink extends JdbcProcessSink<Client> {
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("digital_access_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.digital_access (person_id, username, email_user, is_active, last_login_at, portal_user_confirmation_at, correlation_id) VALUES (?, ?, ?, ?, ?, ?, ?)" +
                                    " ON CONFLICT (person_id) DO UPDATE SET" +
                                    " username = EXCLUDED.username," +
                                    " email_user = EXCLUDED.email_user," +
                                    " is_active = EXCLUDED.is_active," +
                                    " last_login_at = EXCLUDED.last_login_at," +
                                    " correlation_id = EXCLUDED.correlation_id";

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
            if (da.getLastLoginAt() != null) {
                preparedStatement.setTimestamp(5, Timestamp.valueOf(da.getLastLoginAt().replace("T", " ")));
            } else {
                preparedStatement.setNull(5, java.sql.Types.TIMESTAMP);
            }
            if (da.getPortalUserConfirmationAt() != null) {
                preparedStatement.setTimestamp(6, Timestamp.valueOf(da.getPortalUserConfirmationAt().replace("T", " ")));
            } else {
                preparedStatement.setNull(6, java.sql.Types.TIMESTAMP);
            }
            preparedStatement.setString(7, client.getAccount().getCorrelation_id());
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
