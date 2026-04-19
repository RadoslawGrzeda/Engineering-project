package sink;
//import config.DeadLetter;


import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import config.DeadLetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DeadLetterSink implements JdbcStatementBuilder<DeadLetter> {

    public static final String SQL =
            "INSERT INTO transaction.dead_letter" +
            " (transaction_id, correlation_id, transaction_date, location_code, error_code, error_message, raw_payload)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?::jsonb)";




    @Override
    public void accept(PreparedStatement ps, DeadLetter dl) throws SQLException {
        ps.setString(1, dl.getTransactionId());
        ps.setString(2, dl.getCorrelationId());
        ps.setTimestamp(3, dl.getTransactionDate());
        ps.setString(4, dl.getLocationCode());
        ps.setString(5, dl.getErrorCode());
        ps.setString(6, dl.getErrorMessage());
        ps.setString(7, dl.getRawPayload());
    }
}
