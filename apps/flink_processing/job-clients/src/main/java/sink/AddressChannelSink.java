package sink;

import config.DeadLetter;
import config.JdbcProcessSink;
import dto.Client;



import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;

public class AddressChannelSink extends JdbcProcessSink<Client.AddressChannel> {

    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("address-dead-letter", org.apache.flink.api.common.typeinfo.TypeInformation.of(DeadLetter.class));
    public static final String SQL = "INSERT INTO client.address (person_id, address_type, option_channel," +
            " address_street, address_zip_code, address_city, country_code, geo_coordinates_x_value, geo_coordinates_y_value, is_current, created_at, updated_at, correlation_id)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
            " ON CONFLICT (person_id, address_type, option_channel) DO UPDATE SET" +
            " address_street = EXCLUDED.address_street," +
            " address_zip_code = EXCLUDED.address_zip_code," +
            " address_city = EXCLUDED.address_city," +
            " country_code = EXCLUDED.country_code," +
            " geo_coordinates_x_value = EXCLUDED.geo_coordinates_x_value," +
            " geo_coordinates_y_value = EXCLUDED.geo_coordinates_y_value," +
            " is_current = EXCLUDED.is_current," +
            " updated_at = EXCLUDED.updated_at," +
            " correlation_id = EXCLUDED.correlation_id";

   
    @Override
    protected String getSQL() {
        return SQL;
    }

    @Override
    protected JdbcStatementBuilder<Client.AddressChannel> getStatementBuilder() {
        return (PreparedStatement preparedStatement, Client.AddressChannel addressChannel) -> {
        preparedStatement.setString(1, addressChannel.getPersonId());
        preparedStatement.setString(2, addressChannel.getChannelType());
        preparedStatement.setString(3, addressChannel.getOptionChannel());
        preparedStatement.setString(4, addressChannel.getAddressAddress());
        preparedStatement.setString(5, addressChannel.getAddressZipCode());
        preparedStatement.setString(6, addressChannel.getAddressCity());
        preparedStatement.setString(7, addressChannel.getAddressCode());
            if (addressChannel.getLatitude() != null) {
                preparedStatement.setDouble(8, addressChannel.getLatitude());
            }
            else {
                preparedStatement.setNull(8, java.sql.Types.DOUBLE);
            }
            if (addressChannel.getLongitude() != null) {
                preparedStatement.setDouble(9, addressChannel.getLongitude());
            }
            else {
                preparedStatement.setNull(9, java.sql.Types.DOUBLE);
            }
        preparedStatement.setBoolean(10, addressChannel.getIsDeleted() == null);
        preparedStatement.setTimestamp(11, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
        preparedStatement.setTimestamp(12, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
        preparedStatement.setString(13, addressChannel.getCorrelation_id());
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
//public class AddressChannelSink implements JdbcStatementBuilder<Client.AddressChannel> {
//
//    public static final String SQL = "INSERT INTO client.address (person_id, address_type, option_channel," +
//            " address_street, address_zip_code, address_city, country_code, is_current, created_at, updated_at, correlation_id)" +
//            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
//
//    @Override
//    public void accept(java.sql.PreparedStatement preparedStatement, Client.AddressChannel addressChannel) throws java.sql.SQLException {
//        preparedStatement.setString(1, addressChannel.getPersonId());
//        preparedStatement.setString(2, addressChannel.getChannelType());
//        preparedStatement.setString(3, addressChannel.getOptionChannel());
//        preparedStatement.setString(4, addressChannel.getAddressAddress());
//        preparedStatement.setString(5, addressChannel.getAddressZipCode());
//        preparedStatement.setString(6, addressChannel.getAddressCity());
//        preparedStatement.setString(7, addressChannel.getAddressCode());
//        preparedStatement.setBoolean(8, addressChannel.getIsDeleted() == null);
//        preparedStatement.setTimestamp(9, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
//        preparedStatement.setTimestamp(10, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
//        preparedStatement.setString(11, addressChannel.getCorrelation_id());
//    }
//}
