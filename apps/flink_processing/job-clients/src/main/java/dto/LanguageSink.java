package dto;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class LanguageSink implements JdbcStatementBuilder<Client.Language> {

    public static final String SQL = "INSERT INTO language (person_id, language_code, language_name, language_level, created_at) VALUES (?, ?, ?, ?, ?)";

    @Override
    public void accept(PreparedStatement preparedStatement, Client.Language language) throws SQLException {
        preparedStatement.setString(1, language.getPersonId());
        preparedStatement.setString(2, language.getLanguageCode());
        preparedStatement.setString(3, language.getLanguageName());
        preparedStatement.setString(4, language.getLanguageLevel());
        preparedStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));

    }

}
