package sink; import dto.Client;



import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CommunicationSubscriptionSink extends JdbcProcessSink<Client.CommunicationSubscription> {
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("communication_subscription_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.communication_subscription (person_id, communication_code, value, date_of_subscription," +
                                    " date_of_unsubscription, reason_of_unsubscription, created_at, updated_at, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client.CommunicationSubscription> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client.CommunicationSubscription comm) -> {
            preparedStatement.setString(1, comm.getPersonId());
            preparedStatement.setString(2, comm.getCommunityCode());
            preparedStatement.setString(3, comm.getCommunityCodeValue());
            if (comm.getDateOfSubscription() != null) {
                preparedStatement.setTimestamp(4, Timestamp.valueOf(comm.getDateOfSubscription()));
            } else {
                preparedStatement.setNull(4, java.sql.Types.TIMESTAMP);
            }
            if (comm.getDateOfUnsubscription() != null) {
                preparedStatement.setTimestamp(5, Timestamp.valueOf(comm.getDateOfUnsubscription()));
            } else {
                preparedStatement.setNull(5, java.sql.Types.TIMESTAMP);
            }
            preparedStatement.setString(6, comm.getReasonOfUnsubscription());
            preparedStatement.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setString(9, comm.getCorrelation_id());
        };
    }

    @Override
    protected String getPersonId(Client.CommunicationSubscription element) {
        return element.getPersonId();
    }

    @Override
    protected String getCorrelation_id(Client.CommunicationSubscription element) {
        return element.getCorrelation_id();
    }

    @Override
    protected String errorTag() {
        return "Error in CommunicationSubscription Sink";
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
