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

public class TransactionSink extends JdbcProcessSink<Transaction> {

    public static final OutputTag<DeadLetter> DEAD_LETTER =
            new OutputTag<>("transaction_dead_letter", TypeInformation.of(DeadLetter.class));

    public static final String SQL =
            "INSERT INTO transaction.transaction " +
            "(transaction_id, date, location_code, identifier_no, pos_id, printer_id, " +
            "metadata_id, currency_code, cashier_id, creation_date, correlation_id) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (transaction_id) DO NOTHING";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Transaction> getStatementBuilder() {
        return (PreparedStatement ps, Transaction tx) -> {
            Transaction.TransactionHeader h = tx.getTransaction();
            ps.setString(1, h.getTransactionId());
            ps.setTimestamp(2, parseTimestamp(h.getDate()));
            ps.setString(3, h.getLocationCode());
            ps.setString(4, h.getIdentifierNo());
            ps.setString(5, h.getPosId());
            ps.setString(6, h.getPrinterId());
            ps.setString(7, h.getMetadataId());
            ps.setString(8, h.getCurrencyCode());
            ps.setString(9, h.getCashierId());
            ps.setTimestamp(10, parseTimestamp(h.getCreationDate()));
            ps.setString(11, h.getCorrelationId());
        };
    }

    @Override
    protected OutputTag<DeadLetter> getOutputTag() {
        return DEAD_LETTER;
    }

    @Override
    protected String getTag() {
        return "Error in Transaction Sink";
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
