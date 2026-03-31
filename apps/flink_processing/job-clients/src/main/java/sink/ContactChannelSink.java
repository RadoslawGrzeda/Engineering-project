package sink; import dto.Client;



import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class ContactChannelSink implements JdbcStatementBuilder<Client.ContactChannel> {
    public static final String SQL = "INSERT INTO contact (person_id, contact_type, value, flag_main_type," +
                                    " preferred_channel, option_channel, flag_valid, created_at, updated_at, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    @Override
    public void accept(PreparedStatement preparedStatement, Client.ContactChannel contactChannel) throws SQLException {
        preparedStatement.setString(1, contactChannel.getPersonId());
        preparedStatement.setString(2, contactChannel.getChannelType());
        preparedStatement.setString(3, contactChannel.getValue());
        if (contactChannel.getFlagMainType() != null) {
            preparedStatement.setBoolean(4, contactChannel.getFlagMainType());
        } else {
            preparedStatement.setNull(4, java.sql.Types.BOOLEAN);
        }
        if (contactChannel.getPreferredChannel() != null) {
            preparedStatement.setBoolean(5, contactChannel.getPreferredChannel());
        } else {
            preparedStatement.setNull(5, java.sql.Types.BOOLEAN);
        }
        preparedStatement.setString(6, contactChannel.getOptionChannel());
        if (contactChannel.getFlagValid() != null) {
            preparedStatement.setBoolean(7, contactChannel.getFlagValid());
        } else {
            preparedStatement.setNull(7, java.sql.Types.BOOLEAN);
        }
        preparedStatement.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setTimestamp(9, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setString(10, contactChannel.getCorrelation_id());
    }

}
