package sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.DeadLetter;
import config.FlinkClickHouseConfig;
import dto.Transaction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;

public class FactTransactionSink extends ProcessFunction<Transaction, Transaction> {

    private static final Logger LOG = LoggerFactory.getLogger(FactTransactionSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final OutputTag<DeadLetter> DEAD_LETTER =
            new OutputTag<>("fact_transaction_dead_letter", TypeInformation.of(DeadLetter.class));

    private static final String SQL =
            "INSERT INTO gold.fact_transactions " +
            "(transaction_id, transaction_date, location_code,identifier_no, pos_id, cashier_id, currency_code,  " +
            " payment_method, total_net_value, total_gross_value, discount_value, " +
            " status, payment_status, cancelled, " +
            " `lines.prd_code`, `lines.quantity`, `lines.unit_price_net`, `lines.tax_rate`, " +
            " `lines.line_net_value`, `lines.total_line_value`, `lines.tax_amount`, `lines.discount_value`, " +
            " correlation_id) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private transient Connection connection;
    private transient PreparedStatement stmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        JdbcConnectionOptions opts = FlinkClickHouseConfig.connOption();
        connection = DriverManager.getConnection(
                opts.getDbURL(),
                opts.getUsername().orElse(null),
                opts.getPassword().orElse(null)
        );
        stmt = connection.prepareStatement(SQL);
        LOG.info("[FactTransactionSink] ClickHouse connection opened");
    }

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<Transaction> out) throws Exception {
        MDC.put("correlation_id", tx.getCorrelationId() != null ? tx.getCorrelationId() : "-");
        MDC.put("transaction_id", tx.getTransactionId() != null ? tx.getTransactionId() : "-");

        try {
            bindStatement(stmt, tx);
            stmt.executeUpdate();
            LOG.info("[FactTransactionSink] Inserted fact_transaction {}", tx.getTransactionId());
            out.collect(tx);
        } catch (Exception e) {
            LOG.error("[FactTransactionSink] Error for {}: {}", tx.getTransactionId(), e.getMessage(), e);
            ctx.output(DEAD_LETTER, buildDeadLetter(tx, e));
        } finally {
            MDC.clear();
        }
    }

    private void bindStatement(PreparedStatement ps, Transaction tx) throws SQLException {
        Transaction.TransactionHeader h = tx.getTransaction();
        Transaction.Payment p = tx.getPayment();
        Transaction.Status s = tx.getStatus();
        List<Transaction.TransactionLine> lines = tx.getLines();

        ps.setString(1, h.getTransactionId());
        ps.setTimestamp(2, parseTimestamp(h.getDate()));
        ps.setString(3, h.getLocationCode());
        ps.setString(4, h.getIdentifierNo());

        ps.setString(5, h.getPosId());
        ps.setString(6, h.getCashierId());
        ps.setString(7, h.getCurrencyCode());

        ps.setString(8, p != null ? p.getMethod() : null);
        ps.setBigDecimal(9, p != null ? p.getTotalNetValue() : BigDecimal.ZERO);
        ps.setBigDecimal(10, p != null ? p.getTotalValue() : BigDecimal.ZERO);
        ps.setBigDecimal(11, p != null ? p.getDiscountValue() : BigDecimal.ZERO);

        ps.setString(12, s != null ? s.getTransactionStatus() : null);
        ps.setString(13, s != null ? s.getPaymentStatus() : null);
        ps.setBoolean(14, s != null && Boolean.TRUE.equals(s.getCancelled()));

        int size = lines != null ? lines.size() : 0;
        String[]     prdCodes        = new String[size];
        Integer[]    quantities      = new Integer[size];
        BigDecimal[] unitPricesNet   = new BigDecimal[size];
        BigDecimal[] taxRates        = new BigDecimal[size];
        BigDecimal[] lineNetValues   = new BigDecimal[size];
        BigDecimal[] totalLineValues = new BigDecimal[size];
        BigDecimal[] taxAmounts      = new BigDecimal[size];
        BigDecimal[] discountValues  = new BigDecimal[size];

        if (lines != null) {
            for (int i = 0; i < size; i++) {
                Transaction.TransactionLine l = lines.get(i);
                prdCodes[i]        = l.getPrdCode();
                quantities[i]      = l.getQuantity();
                unitPricesNet[i]   = l.getUnitPriceNet();
                taxRates[i]        = l.getTaxRate();
                lineNetValues[i]   = l.getLineNetValue();
                totalLineValues[i] = l.getTotalLineValue();
                taxAmounts[i]      = l.getTotalTaxAmount();
                discountValues[i]  = l.getDiscountValue();
            }
        }

        ps.setObject(15, prdCodes);
        ps.setObject(16, quantities);
        ps.setObject(17, unitPricesNet);
        ps.setObject(18, taxRates);
        ps.setObject(19, lineNetValues);
        ps.setObject(20, totalLineValues);
        ps.setObject(21, taxAmounts);
        ps.setObject(22, discountValues);

        ps.setString(23, tx.getCorrelationId());
    }

    private static Timestamp parseTimestamp(String dateStr) {
        if (dateStr == null || dateStr.isBlank()) return null;
        return Timestamp.valueOf(LocalDateTime.parse(dateStr));
    }

    private DeadLetter buildDeadLetter(Transaction tx, Exception e) {
        DeadLetter dl = new DeadLetter();
        dl.setTransactionId(tx.getTransactionId());
        dl.setCorrelationId(tx.getCorrelationId());
        if (tx.getTransaction() != null) {
            dl.setTransactionDateFromString(tx.getTransaction().getDate());
            dl.setLocationCode(tx.getTransaction().getLocationCode());
        }
        dl.setErrorCode("Error in FactTransactionSink");
        dl.setErrorMessage(e.getMessage());
        try {
            dl.setRawPayload(MAPPER.writeValueAsString(tx));
        } catch (JsonProcessingException ex) {
            dl.setRawPayload("serialization_error");
        }
        return dl;
    }

    @Override
    public void close() throws Exception {
        if (stmt != null) stmt.close();
        if (connection != null) connection.close();
        LOG.info("[FactTransactionSink] ClickHouse connection closed");
    }
}
