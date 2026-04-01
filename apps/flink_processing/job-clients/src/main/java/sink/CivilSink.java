package sink; import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CivilSink extends JdbcProcessSink<Client> {
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("civil_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.civil (person_id, civil_status_type, is_current, created_at, updated_at, correlation_id) VALUES (?, ?, ?, ?, ?, ?)";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client client) -> {
            preparedStatement.setString(1, client.getPersonId());
            preparedStatement.setString(2, client.getAccount().getCivilStatus());
            preparedStatement.setBoolean(3, true);
            preparedStatement.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setString(6, client.getAccount().getCorrelation_id());
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
        return "Error in Civil Sink";
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
