package sink; import dto.Client;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class AccountSink extends JdbcProcessSink<Client> {

    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("account_dead_letter", TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.customer (" +
                                    "person_id, first_name, middle_name, last_name, birth_date, passport_number, gender_code," +
                                    "registration_date, creation_application, created_at, updated_at, correlation_id)" +
                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

//    public void accept(PreparedStatement statement, Client client) throws SQLException {
//        statement.setString(1, client.getPersonId());
//        statement.setString(2, client.getAccount().getFirstName());
//        statement.setString(3, client.getAccount().getMiddleName());
//        statement.setString(4, client.getAccount().getLastName());
//        statement.setDate(5, client.getAccount().getBirthDate());
//        statement.setString(6, client.getAccount().getPassportNumber() == null ? null : client.getAccount().getPassportNumber().toUpperCase());
//        statement.setString(7, client.getAccount().getGenderCode());
//        statement.setDate(8, client.getAccount().getRegistrationDate());
//        statement.setString(9, client.getAccount().getCreationApplication());
//        statement.setTimestamp(10, Timestamp.valueOf(LocalDateTime.now()));
//        statement.setTimestamp(11, Timestamp.valueOf(LocalDateTime.now()));
//        statement.setString(12, client.getAccount().getCorrelation_id());
//    }

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
            statement.setDate(8, client.getAccount().getRegistrationDate());
            statement.setString(9, client.getAccount().getCreationApplication());
            statement.setTimestamp(10, Timestamp.valueOf(LocalDateTime.now()));
            statement.setTimestamp(11, Timestamp.valueOf(LocalDateTime.now()));
            statement.setString(12, client.getAccount().getCorrelation_id());
        };
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
//package sink; import dto.Client;
//
//
//
//
//import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
//
//import java.sql.Date;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.sql.Timestamp;
//import java.time.LocalDate;
//import java.time.LocalDateTime;
//
//public class AccountSink implements JdbcStatementBuilder<Client> {
//
//    public static final String SQL = "INSERT INTO client.customer (" +
//            "person_id, first_name, middle_name, last_name, birth_date, passport_number, gender_code," +
//            "registration_date, creation_application, created_at, updated_at, correlation_id)" +
//            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
//
//    public void accept(PreparedStatement statement, Client client) throws SQLException {
//        statement.setString(1, client.getPersonId());
//        statement.setString(2, client.getAccount().getFirstName());
//        statement.setString(3, client.getAccount().getMiddleName());
//        statement.setString(4, client.getAccount().getLastName());
//        statement.setDate(5, client.getAccount().getBirthDate());
//        statement.setString(6, client.getAccount().getPassportNumber() == null ? null : client.getAccount().getPassportNumber().toUpperCase());
//        statement.setString(7, client.getAccount().getGenderCode());
//        statement.setDate(8, client.getAccount().getRegistrationDate());
//        statement.setString(9, client.getAccount().getCreationApplication());
//        statement.setTimestamp(10, Timestamp.valueOf(LocalDateTime.now()));
//        statement.setTimestamp(11, Timestamp.valueOf(LocalDateTime.now()));
//        statement.setString(12, client.getAccount().getCorrelation_id());
//    }
//
//}
