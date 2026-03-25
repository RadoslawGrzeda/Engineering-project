import deserializer.ClientDeserializer;
import dto.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientProcessingJob {

    private static final Logger LOG = LoggerFactory.getLogger(ClientProcessingJob.class);
    private static final String TOPIC = "crm_client";
    private static final String GROUP_ID = "crm_group";
    private static final String BOOTSTRAP_SERVERS = "broker:9094";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<Client> source = KafkaSource.<Client>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new ClientDeserializer())
                .build();

        DataStream<Client> clientStream = env.fromSource(source,
                                                        WatermarkStrategy.noWatermarks(),
                                                        "Kafka Source");

        DataStream<Client> validClientStream= clientStream.flatMap(new ClientValidator());

        DataStream<Client.Language> languageStream = validClientStream.flatMap(
        (Client client, org.apache.flink.util.Collector<Client.Language> collector) -> {
            if ( client.getLanguages() != null) {
            for (Client.Language lang : client.getLanguages()) {
                        lang.setPersonId(client.getPersonId());
                        lang.setCorrelation_id(client.getAccount().getCorrelation_id());
                        collector.collect(lang);
                    }
                }}
        ).returns(Client.Language.class);

        /**
         * Insert into the account table
         */
        validClientStream.addSink(JdbcSink.sink(
                AccountSink.SQL,
                new AccountSink(),
                FlinkJdbcConfig.execOption(),
                FlinkJdbcConfig.connOption()
        ));

        languageStream.addSink(JdbcSink.sink(
                LanguageSink.SQL,
                new LanguageSink(),
                FlinkJdbcConfig.execOption(),
                FlinkJdbcConfig.connOption()
        ));
        validClientStream.addSink(JdbcSink.sink(
                GenderSink.SQL,
                new GenderSink(),
                FlinkJdbcConfig.execOption(),
                FlinkJdbcConfig.connOption()
        ));
        validClientStream.addSink(JdbcSink.sink(
                CountriesSink.SQL,
                new CountriesSink(),
                FlinkJdbcConfig.execOption(),
                FlinkJdbcConfig.connOption()
        ));
        validClientStream.flatMap((Client client, org.apache.flink.util.Collector<Client> collector) -> {
            if (client.getAccount().getCivilStatus() != null) {
                collector.collect(client);
            }
        }).returns(Client.class).addSink(JdbcSink.sink(
                CivilSink.SQL,
                new CivilSink(),
                FlinkJdbcConfig.execOption(),
                FlinkJdbcConfig.connOption()
        ));

        validClientStream.flatMap((Client client, org.apache.flink.util.Collector<Client> collector) -> {
        if (client.getDigitalAccess() != null) {
            collector.collect(client);
        }}).returns(Client.class).addSink(JdbcSink.sink(
                DigitalAccessSink.SQL,
                new DigitalAccessSink(),
                FlinkJdbcConfig.execOption(),
                FlinkJdbcConfig.connOption()
        ));

        /**
         * Insert into the contact channel table
         */
        validClientStream.flatMap((
                Client client, org.apache.flink.util.Collector<Client.ContactChannel> collector) -> {
                if (client.getContactChannels() != null)
                    for (Client.ContactChannel contactChannel : client.getContactChannels()){
                        contactChannel.setPersonId(client.getPersonId());
                        contactChannel.setCorrelation_id(client.getAccount().getCorrelation_id());
                        collector.collect(contactChannel);
                    }
                }
                ).returns(Client.ContactChannel.class).addSink(JdbcSink.sink(
                        ContactChannelSink.SQL,
                        new ContactChannelSink(),
                        FlinkJdbcConfig.execOption(),
                        FlinkJdbcConfig.connOption()
                ));

        /**
         * Insert into the communication_subscription table
         */
        validClientStream.flatMap(
                (Client client, org.apache.flink.util.Collector<Client.CommunicationSubscription> collector) -> {
                    if (client.getCommunicationSubscriptions() != null)
                        for (Client.CommunicationSubscription comm : client.getCommunicationSubscriptions()) {
                            comm.setPersonId(client.getPersonId());
                            comm.setCorrelation_id(client.getAccount().getCorrelation_id());
                            collector.collect(comm);
                        }
                }).returns(Client.CommunicationSubscription.class).addSink(JdbcSink.sink(
                        CommunicationSubscriptionSink.SQL,
                        new CommunicationSubscriptionSink(),
                        FlinkJdbcConfig.execOption(),
                        FlinkJdbcConfig.connOption()
                )
        );

        validClientStream.flatMap(
                (Client client, org.apache.flink.util.Collector<Client.AddressChannel> collector) -> {
                    if (client.getAddressChannels() != null)
                        for (Client.AddressChannel addressChannel : client.getAddressChannels()) {
                            addressChannel.setPersonId(client.getPersonId());
                            addressChannel.setCorrelation_id(client.getAccount().getCorrelation_id());
                            collector.collect(addressChannel);
                        }
                }).returns(Client.AddressChannel.class).addSink(JdbcSink.sink(
                        AddressChannelSink.SQL,
                        new AddressChannelSink(),
                        FlinkJdbcConfig.execOption(),
                        FlinkJdbcConfig.connOption()
                )
        );



        clientStream.print();

        env.execute("Client Processing Job");
    }
}
