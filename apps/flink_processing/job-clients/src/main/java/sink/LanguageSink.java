package sink; import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class LanguageSink extends JdbcProcessSink<Client.Language> {
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("language_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.language (person_id, language_code, language_level, created_at, updated_at, correlation_id) VALUES (?, ?, ?, ?, ?, ?)";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client.Language> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client.Language language) -> {
            preparedStatement.setString(1, language.getPersonId());
            preparedStatement.setString(2, language.getLanguageCode());
            preparedStatement.setString(3, language.getLanguageLevel());
            preparedStatement.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setString(6, language.getCorrelation_id());
        };
    }

    @Override
    protected String getPersonId(Client.Language element) {
        return element.getPersonId();
    }

    @Override
    protected String getCorrelation_id(Client.Language element) {
        return element.getCorrelation_id();
    }

    @Override
    protected String errorTag() {
        return "Error in Language Sink";
    }

    @Override
    protected String getSourceApplication() {
        return "CLIENTS";
    }

    @Override
    protected OutputTag<DeadLetter> getDeadLetterTag() {
        return DEAD_LETTER;
    }
}
