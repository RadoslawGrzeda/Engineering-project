package dto;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class ContactChannelSink implements JdbcStatementBuilder<Client.ContactChannel> {
    public static final String SQL = "INSERT INTO client.contact_channel (person_id, channel_type, value, flag_main_type," +
                                    "preferred_channel, option_channel, flag_valid, created_at, updated_at, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    @Override
    public void accept(PreparedStatement preparedStatement, Client.ContactChannel contactChannel) throws SQLException {
        preparedStatement.setString(1, contactChannel.getPersonId());
        preparedStatement.setString(2, contactChannel.getChannelType());
        preparedStatement.setString(3, contactChannel.getValue());
        preparedStatement.setBoolean(4, contactChannel.getFlagMainType());
        preparedStatement.setBoolean(5, contactChannel.getPreferredChannel());
        preparedStatement.setString(6, contactChannel.getOptionChannel());
        preparedStatement.setBoolean(7, contactChannel.getFlagValid());
        preparedStatement.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setTimestamp(9, Timestamp.valueOf(LocalDateTime.now()));
        preparedStatement.setString(10, contactChannel.getCorrelation_id());





    }

}
