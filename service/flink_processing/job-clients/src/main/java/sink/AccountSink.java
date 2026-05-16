package sink; import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Client;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class AccountSink extends JdbcProcessSink<Client> {

    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("account_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.customer (" +
                                    "person_id, first_name, middle_name, last_name, birth_date, passport_number, gender_code, civil_status_code," +
                                    "registration_date, creation_application, correlation_id, event_time)" +
                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                                    " ON CONFLICT (person_id) DO UPDATE SET" +
                                    " first_name = EXCLUDED.first_name," +
                                    " middle_name = EXCLUDED.middle_name," +
                                    " last_name = EXCLUDED.last_name," +
                                    " birth_date = EXCLUDED.birth_date," +
                                    " passport_number = EXCLUDED.passport_number," +
                                    " gender_code = EXCLUDED.gender_code," +
                                    " civil_status_code = EXCLUDED.civil_status_code," +
                                    " correlation_id = EXCLUDED.correlation_id," +
                                    " event_time = EXCLUDED.event_time";

    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client> getStatementBuilder() {
        return (PreparedStatement statement, Client client) -> {
            statement.setString(1, client.getPersonId());
            statement.setString(2, client.getAccount().getFirstName());
            statement.setString(3, client.getAccount().getMiddleName());
            statement.setString(4, client.getAccount().getLastName());
            statement.setDate(5, client.getAccount().getBirthDate());
            statement.setString(6, client.getAccount().getPassportNumber() == null ? null : client.getAccount().getPassportNumber().toUpperCase());
            statement.setString(7, client.getAccount().getGenderCode());
            statement.setString(8, client.getAccount().getCivilStatusCode());
            String registrationDate = client.getAccount().getRegistrationDate();
            if (registrationDate != null) {
                statement.setTimestamp(9, Timestamp.valueOf(LocalDateTime.parse(registrationDate, TIMESTAMP_FORMAT)));
            } else {
                statement.setNull(9, java.sql.Types.TIMESTAMP);
            }
            statement.setString(10, client.getAccount().getCreationApplication());
            statement.setString(11, client.getAccount().getCorrelation_id());
            String rawEventTime = client.getEventTimestamp();
            if (rawEventTime != null) {
                statement.setTimestamp(12, Timestamp.from(parseEventTimestamp(rawEventTime)));
            } else {
                statement.setNull(12, java.sql.Types.TIMESTAMP);
            }
        };
    }

    private static Instant parseEventTimestamp(String raw) {
        try {
            return Instant.parse(raw);
        } catch (Exception e) {
            return OffsetDateTime.parse(raw).toInstant();
        }
    }

    @Override
    protected String getPersonId(Client client) {
        return client.getAccount().getPersonId();
    }

    @Override
    protected String getCorrelation_id(Client client) {
        return client.getAccount().getCorrelation_id();
    }

    @Override
    protected String errorTag() {
        return "Error in Account Sink";
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
