package sink;

import com.esotericsoftware.minlog.Log;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.FlinkJdbcConfig;
import dto.Client;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.configuration.Configuration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class JdbcProcessSink<T> extends ProcessFunction<T,T> {

    protected abstract String getSQL();
    protected abstract JdbcStatementBuilder<T> getStatementBuilder();
    protected abstract String getPersonId(T element);
    protected abstract String getCorrelation_id(T element);

    JdbcConnectionOptions connOptions = FlinkJdbcConfig.connOption();
    JdbcExecutionOptions execOptions = FlinkJdbcConfig.execOption();


    protected abstract String errorTag();
    protected abstract String getSourceApplication();
    public static final ObjectMapper objectMapper = new ObjectMapper();
    private transient Connection connection;
    private transient PreparedStatement statement;

    protected abstract OutputTag<DeadLetter> getDeadLetterTag();

    @Override
    public void open(Configuration parameters) throws Exception {
        JdbcConnectionOptions connOptions = FlinkJdbcConfig.connOption();
        connection = DriverManager.getConnection(
                connOptions.getDbURL(),
                connOptions.getUsername().orElse(null),
                connOptions.getPassword().orElse(null)
        );
        statement = connection.prepareStatement(getSQL());
    }
    @Override
    public void processElement(T value, Context ctx, Collector<T> out) throws Exception {
    try{
        getStatementBuilder().accept(statement, value);
        statement.executeUpdate();
        out.collect(value);
    }catch (SQLException e){
        Log.warn("Error while writing to DB",e);
        DeadLetter dl = new DeadLetter();
        dl.setPersonId(getPersonId(value));
        dl.setCorrelationId(getCorrelation_id(value));
        dl.setSourceApplication(getSourceApplication());
        dl.setErrorCode(errorTag());
        dl.setErrorMessage(e.getMessage());
        try{
            dl.setRawPayload(objectMapper.writeValueAsString(value));
        }catch (JsonProcessingException ex){
            dl.setRawPayload("serialization_error");
        }
        ctx.output(getDeadLetterTag(),dl);
    } catch (Exception e){
        Log.warn("Error while writing to DB",e);
        DeadLetter dl = new DeadLetter();
        dl.setPersonId(getPersonId(value));
        dl.setCorrelationId(getCorrelation_id(value));
        dl.setSourceApplication(getSourceApplication());
        dl.setErrorCode(errorTag());
        dl.setErrorMessage(e.getMessage());
        try{
            dl.setRawPayload(objectMapper.writeValueAsString(value));
        }catch (JsonProcessingException ex){
            dl.setRawPayload("serialization_error");
        }
        ctx.output(getDeadLetterTag(),dl);
    }
    }
    public void close() throws Exception {
        statement.close();
        connection.close();
    }
}



