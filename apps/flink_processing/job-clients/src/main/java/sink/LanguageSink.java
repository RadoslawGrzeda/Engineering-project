package sink; import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;

public class LanguageSink extends JdbcProcessSink<Client.Language> {
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("language_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.language (person_id, language_code, language_level, correlation_id) VALUES (?, ?, ?, ?)" +
                                    " ON CONFLICT (person_id, language_code) DO UPDATE SET" +
                                    " language_level = EXCLUDED.language_level," +
                                    " correlation_id = EXCLUDED.correlation_id";
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
            preparedStatement.setString(4, language.getCorrelation_id());
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
