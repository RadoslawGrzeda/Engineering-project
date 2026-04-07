package sink;

import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Transaction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class TransactionLineSink extends JdbcProcessSink<Transaction> {

    public static final OutputTag<DeadLetter> DEAD_LETTER =
            new OutputTag<>("transaction_line_dead_letter", TypeInformation.of(DeadLetter.class));

    public static final String SQL =
            "INSERT INTO transaction.transaction_line " +
            "(transaction_line_id, transaction_id, prd_code, quantity, unit_price_net, " +
            "tax_rate, line_net_value, total_line_value, total_tax_amount, discount_value, correlation_id) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (transaction_line_id) DO NOTHING";

    @Override
    protected String getSQL() {
        return SQL;
    }




    @Override
    protected JdbcStatementBuilder<Transaction> getStatementBuilder() {
        return (PreparedStatement ps, Transaction tx) -> {
            if (tx.getLines() == null) return;
            for (Transaction.TransactionLine line : tx.getLines()) {
                String correlationId = tx.getCorrelationId();
                ps.setString(1, line.getTransactionLineId());
                ps.setString(2, tx.getTransactionId());
                ps.setString(3, line.getPrdCode());
                if (line.getQuantity() != null) {
                    ps.setBigDecimal(4, java.math.BigDecimal.valueOf(line.getQuantity()));
                } else {
                    ps.setNull(4, java.sql.Types.DECIMAL);
                }
                ps.setBigDecimal(5, line.getUnitPriceNet());
                ps.setBigDecimal(6,line.getTaxRate());
                ps.setBigDecimal(7, line.getLineNetValue());
                ps.setBigDecimal(8, line.getTotalLineValue());
                ps.setBigDecimal(9, line.getTotalTaxAmount());
                ps.setBigDecimal(10, line.getDiscountValue());
                ps.setString(11, correlationId);
                ps.addBatch();
            }
        };
    }

    @Override
    protected void executeStatement() throws SQLException {
        statement.executeBatch();
    }

    @Override
    protected OutputTag<DeadLetter> getOutputTag() {
        return DEAD_LETTER;
    }

    @Override
    protected String getTag() {
        return "Error in TransactionLine Sink";
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
