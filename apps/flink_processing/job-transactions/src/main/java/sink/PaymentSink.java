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

public class PaymentSink extends JdbcProcessSink<Transaction> {

    public static final OutputTag<DeadLetter> DEAD_LETTER =
            new OutputTag<>("payment_dead_letter", TypeInformation.of(DeadLetter.class));

    public static final String SQL =
            "INSERT INTO transaction.transaction_payment " +
            "(payment_id, transaction_id, method, total_value, total_net_value, " +
            "total_payment, discount_value, correlation_id) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (payment_id) DO NOTHING";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Transaction> getStatementBuilder() {
        return (PreparedStatement ps, Transaction tx) -> {
            String correlationId = tx.getCorrelationId();
            Transaction.Payment p = tx.getPayment();
            ps.setString(1, p.getPaymentId());
            ps.setString(2, tx.getTransactionId());
            ps.setString(3, p.getMethod());
            ps.setBigDecimal(4, p.getTotalValue());
            ps.setBigDecimal(5, p.getTotalNetValue());
            ps.setBigDecimal(6, p.getTotalPayment());
            ps.setBigDecimal(7, p.getDiscountValue());
            ps.setString(8, correlationId);
        };
    }

    @Override
    protected OutputTag<DeadLetter> getOutputTag() {
        return DEAD_LETTER;
    }

    @Override
    protected String getTag() {
        return "Error in Payment Sink";
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
