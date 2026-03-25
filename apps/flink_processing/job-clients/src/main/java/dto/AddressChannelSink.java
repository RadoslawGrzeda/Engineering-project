package dto;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

public class AddressChannelSink implements JdbcStatementBuilder<Client.AddressChannel> {

    public static final String SQL = "INSERT INTO client.address_channel (person_id, channel_type, option_channel," +
            "address_street, address_zip_code, address_city, address_country, is_current, created_at, updated_at, correlation_id)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    @Override
    public void accept(java.sql.PreparedStatement preparedStatement, Client.AddressChannel addressChannel) throws java.sql.SQLException {
        preparedStatement.setString(1, addressChannel.getPersonId());
        preparedStatement.setString(2, addressChannel.getChannelType());
        preparedStatement.setString(3, addressChannel.getOptionChannel());
        preparedStatement.setString(4, addressChannel.getAddressAddress());
        preparedStatement.setString(5, addressChannel.getAddressZipCode());
        preparedStatement.setString(6, addressChannel.getAddressCity());
        preparedStatement.setString(7, addressChannel.getAddressCode());
        preparedStatement.setBoolean(8, addressChannel.getIsDeleted() == null ? false : true);
        preparedStatement.setTimestamp(9, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
        preparedStatement.setTimestamp(10, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
        preparedStatement.setString(11, addressChannel.getCorrelation_id());
    }
}
