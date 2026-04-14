package sink; import config.DeadLetter;


import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DeadLetterSink implements JdbcStatementBuilder<DeadLetter> {

    public static final String SQL =
            "INSERT INTO client.dead_letter " +
            "(person_id, correlation_id, source_application, error_code, error_message, raw_payload) " +
            "VALUES (?, ?, ?, ?, ?, ?::jsonb)" +
            " ON CONFLICT (person_id, correlation_id, error_code) DO NOTHING";

    @Override
    public void accept(PreparedStatement ps, DeadLetter dl) throws SQLException {
        ps.setString(1, dl.getPersonId());
        ps.setString(2, dl.getCorrelationId());
        ps.setString(3, dl.getSourceApplication());
        ps.setString(4, dl.getErrorCode());
        ps.setString(5, dl.getErrorMessage());
        ps.setString(6, dl.getRawPayload());
    }
}
