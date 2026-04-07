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

public class CashierSink extends JdbcProcessSink<Transaction> {

    public static final OutputTag<DeadLetter> DEAD_LETTER =
            new OutputTag<>("cashier_dead_letter", TypeInformation.of(DeadLetter.class));

    public static final String SQL =
            "INSERT INTO transaction.cashier " +
            "(cashier_id, location_code, created_at) " +
            "VALUES (?, ?, ?) " +
            "ON CONFLICT (cashier_id) DO UPDATE SET " +
            "location_code = EXCLUDED.location_code, " +
            "updated_at = CURRENT_TIMESTAMP";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Transaction> getStatementBuilder() {
        return (PreparedStatement ps, Transaction tx) -> {
            Transaction.TransactionHeader h = tx.getTransaction();
            ps.setString(1, h.getCashierId());
            ps.setString(2, h.getLocationCode());
            ps.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
        };
    }

    @Override
    protected OutputTag<DeadLetter> getOutputTag() {
        return DEAD_LETTER;
    }

    @Override
    protected String getTag() {
        return "Error in Cashier Sink";
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
