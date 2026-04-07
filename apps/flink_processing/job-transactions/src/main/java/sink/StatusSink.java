package sink;

import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Transaction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class StatusSink extends JdbcProcessSink<Transaction> {

    public static final OutputTag<DeadLetter> DEAD_LETTER =
            new OutputTag<>("status_dead_letter", TypeInformation.of(DeadLetter.class));

    public static final String SQL =
            "INSERT INTO transaction.transaction_status " +
            "(transaction_status_id, transaction_id, status, cancelled, payment_status, " +
            "transaction_status, is_current, created_at, correlation_id) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (transaction_status_id) DO UPDATE SET " +
            "status = EXCLUDED.status, " +
            "cancelled = EXCLUDED.cancelled, " +
            "payment_status = EXCLUDED.payment_status, " +
            "transaction_status = EXCLUDED.transaction_status, " +
            "is_current = EXCLUDED.is_current, " +
            "updated_at = CURRENT_TIMESTAMP, " +
            "correlation_id = EXCLUDED.correlation_id";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Transaction> getStatementBuilder() {
        return (PreparedStatement ps, Transaction tx) -> {
            String correlationId = tx.getCorrelationId();
            Transaction.Status s = tx.getStatus();
            ps.setString(1, s.getTransactionStatusId());
            ps.setString(2, tx.getTransactionId());
            ps.setString(3, s.getStatus());
            ps.setBoolean(4, s.getCancelled() != null ? s.getCancelled() : false);
            ps.setString(5, s.getPaymentStatus());
            ps.setString(6, s.getTransactionStatus());
            ps.setBoolean(7, s.getIsCurrent() != null ? s.getIsCurrent() : true);
            ps.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
            ps.setString(9, correlationId);
        };
    }

    @Override
    protected OutputTag<DeadLetter> getOutputTag() {
        return DEAD_LETTER;
    }

    @Override
    protected String getTag() {
        return "Error in Status Sink";
    }

    @Override
    protected String getCorrelationId(Transaction tx) {
        return tx.getCorrelationId();
    }

    @Override
    protected String getTransactionId(Transaction tx) {
        return tx.getTransactionId();
    }

    @Override
    protected String getLocationCode(Transaction tx) {
        return tx.getTransaction() != null ? tx.getTransaction().getLocationCode() : null;
    }

    @Override
    protected Timestamp getTransactionDate(Transaction tx) {
        return tx.getTransaction() != null ? parseTimestamp(tx.getTransaction().getDate()) : null;
    }

    private static Timestamp parseTimestamp(String dateStr) {
        if (dateStr == null || dateStr.isEmpty()) return null;
        return Timestamp.valueOf(LocalDateTime.parse(dateStr));
    }
}
