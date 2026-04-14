package sink; import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class  CountriesSink extends JdbcProcessSink<Client.Nationality> {
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("countries_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.nationality (person_id, country_code, created_at, correlation_id) VALUES (?, ?, ?, ?)" +
                                    " ON CONFLICT (person_id, country_code) DO UPDATE SET" +
                                    " correlation_id = EXCLUDED.correlation_id";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client.Nationality> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client.Nationality nationality) -> {
            preparedStatement.setString(1, nationality.getPersonId());
            preparedStatement.setString(2, nationality.getCountryCode());
            preparedStatement.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setString(4, nationality.getCorrelation_id());
        };
    }

    @Override
    protected String getPersonId(Client.Nationality element) {
        return element.getPersonId();
    }

    @Override
    protected String getCorrelation_id(Client.Nationality element) {
        return element.getCorrelation_id();
    }

    @Override
    protected String errorTag() {
        return "Error in Countries Sink";
    }

    @Override
    protected String getSourceApplication() {
        return "CLIENTS";
    }

    @Override
    protected OutputTag<DeadLetter> getDeadLetterTag() {
        return DEAD_LETTER;
    }

    @Override
    protected Client getRawPayload(Client.Nationality element) {
        return element.getClient();
    }
}
