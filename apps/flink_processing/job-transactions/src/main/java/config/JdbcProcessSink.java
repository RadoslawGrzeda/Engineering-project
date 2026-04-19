package config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.sql.*;

public abstract class JdbcProcessSink<T> extends ProcessFunction<T, T> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcProcessSink.class);
    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected abstract String getSQL();
    protected abstract JdbcStatementBuilder<T> getStatementBuilder();
    protected abstract OutputTag<DeadLetter> getOutputTag();
    protected abstract String getTag();
    protected abstract String getCorrelationId(T value);
    protected abstract String getTransactionId(T value);
    protected abstract String getLocationCode(T value);
    protected abstract Timestamp getTransactionDate(T value);

    private transient Connection connection;
    protected transient PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws SQLException {
        JdbcConnectionOptions connOptions = FlinkClickHouseConfig.connOption();
        connection = DriverManager.getConnection(
                connOptions.getDbURL(),
                connOptions.getUsername().orElse(null),
                connOptions.getPassword().orElse(null)
        );
        statement = connection.prepareStatement(getSQL());
        LOG.info("[{}] JDBC connection opened", getTag());
    }

    @Override
    public void processElement(T value, ProcessFunction<T, T>.Context ctx, Collector<T> out) throws Exception {
        MDC.put("service", "flink-transactions");
        MDC.put("correlation_id", getCorrelationId(value) != null ? getCorrelationId(value) : "-");
        MDC.put("transaction_id", getTransactionId(value) != null ? getTransactionId(value) : "-");
        try {
            LOG.info("[{}] Writing to DB for transaction {}", getTag(), getTransactionId(value));
            getStatementBuilder().accept(statement, value);
            executeStatement();
            LOG.info("[{}] Successfully wrote transaction {}", getTag(), getTransactionId(value));
            out.collect(value);
        } catch (SQLException e) {
            LOG.error("[{}] SQL error for transaction {}: {}", getTag(), getTransactionId(value), e.getMessage(), e);
            ctx.output(getOutputTag(), buildDeadLetter(value, e));
        } catch (Exception e) {
            LOG.error("[{}] Unexpected error for transaction {}: {}", getTag(), getTransactionId(value), e.getMessage(), e);
            ctx.output(getOutputTag(), buildDeadLetter(value, e));
        } finally {
            MDC.clear();
        }
    }

    protected void executeStatement() throws SQLException {
        statement.executeUpdate();
    }

    protected DeadLetter buildDeadLetter(T value, Exception e) {
        DeadLetter dl = new DeadLetter();
        dl.setTransactionId(getTransactionId(value));
        dl.setCorrelationId(getCorrelationId(value));
        dl.setTransactionDate(getTransactionDate(value));
        dl.setLocationCode(getLocationCode(value));
        dl.setErrorCode(getTag());
        dl.setErrorMessage(e.getMessage());
        try {
            dl.setRawPayload(MAPPER.writeValueAsString(value));
        } catch (JsonProcessingException ex) {
            LOG.warn("[{}] Failed to serialize raw payload for transaction {}", getTag(), getTransactionId(value));
            dl.setRawPayload("serialization_error");
        }
        return dl;
    }

    @Override
    public void close() throws Exception {
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
        LOG.info("[{}] JDBC connection closed", getTag());
    }
}
