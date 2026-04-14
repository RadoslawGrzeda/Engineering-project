package sink; import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class ContactChannelSink extends JdbcProcessSink<Client.ContactChannel> {
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("contact_channel_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.contact (person_id, contact_type, value, flag_main_type," +
                                    " preferred_channel, option_channel, flag_valid, created_at, updated_at, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                                    " ON CONFLICT (person_id, contact_type, value) DO UPDATE SET" +
                                    " flag_main_type = EXCLUDED.flag_main_type," +
                                    " preferred_channel = EXCLUDED.preferred_channel," +
                                    " option_channel = EXCLUDED.option_channel," +
                                    " flag_valid = EXCLUDED.flag_valid," +
                                    " updated_at = EXCLUDED.updated_at," +
                                    " correlation_id = EXCLUDED.correlation_id";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client.ContactChannel> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client.ContactChannel contactChannel) -> {
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
            if (contactChannel.getOptionChannel() != null) {
                preparedStatement.setBoolean(6, contactChannel.getOptionChannel());
            } else {
                preparedStatement.setBoolean(6, true);
            }
            if (contactChannel.getFlagValid() != null) {
                preparedStatement.setBoolean(7, contactChannel.getFlagValid());
            } else {
                preparedStatement.setNull(7, java.sql.Types.BOOLEAN);
            }
            preparedStatement.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setTimestamp(9, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setString(10, contactChannel.getCorrelation_id());
        };
    }

    @Override
    protected String getPersonId(Client.ContactChannel element) {
        return element.getPersonId();
    }

    @Override
    protected String getCorrelation_id(Client.ContactChannel element) {
        return element.getCorrelation_id();
    }

    @Override
    protected String errorTag() {
        return "Error in ContactChannel Sink";
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
    protected Client getRawPayload(Client.ContactChannel element) {
        return element.getClient();
    }
}
