package sink;

import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Client;



import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;

public class AddressChannelSink extends JdbcProcessSink<Client.AddressChannel> {
    public Client client;
    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("address-dead-letter", org.apache.flink.api.common.typeinfo.TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.address (person_id, address_type," +
            " address_street, address_zip_code, address_city, country_code, correlation_id)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?)" +
            " ON CONFLICT (person_id, address_type) DO UPDATE SET" +
            " address_street = EXCLUDED.address_street," +
            " address_zip_code = EXCLUDED.address_zip_code," +
            " address_city = EXCLUDED.address_city," +
            " country_code = EXCLUDED.country_code," +
            " correlation_id = EXCLUDED.correlation_id";

    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client.AddressChannel> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client.AddressChannel addressChannel) -> {
        preparedStatement.setString(1, addressChannel.getPersonId());
        preparedStatement.setString(2, addressChannel.getAddressType());
        preparedStatement.setString(3, addressChannel.getAddressStreet());
        preparedStatement.setString(4, addressChannel.getAddressZipCode());
        preparedStatement.setString(5, addressChannel.getAddressCity());
        preparedStatement.setString(6, addressChannel.getCountryCode());
        preparedStatement.setString(7, addressChannel.getCorrelation_id());
    };
    }

    @Override
    protected String getPersonId(Client.AddressChannel element) {
        return element.getPersonId();
    }

    @Override
    protected String getCorrelation_id(Client.AddressChannel element) {
        return element.getCorrelation_id();
    }

    @Override
    protected String errorTag() {
        return "AddressChannelSink";
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
