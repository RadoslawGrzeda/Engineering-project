import config.DeadLetter;
import config.FlinkJdbcConfig;
//import config.GetGeoAddress;
import config.SinkValidator;
import deserializer.ClientDeserializer;
import dto.Client;
import sink.*;
import validator.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.connector.kafka.sink.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

public class ClientProcessingJob {


    private static final Logger LOG = LoggerFactory.getLogger(ClientProcessingJob.class);
    private static final Properties PROPERTIES = new Properties();

    static {
        try (InputStream input = ClientProcessingJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new IOException("Unable to find config.properties");
            }
            PROPERTIES.load(input);
        } catch (IOException e) {
            LOG.error("Error loading config.properties", e);
            throw new RuntimeException("Failed to initialize ClientProcessingJob", e);
        }
    }

    private static final String TOPIC = Objects.requireNonNull(PROPERTIES.getProperty("TOPIC"), "TOPIC property is required");
    private static final String GROUP_ID = Objects.requireNonNull(PROPERTIES.getProperty("GROUP_ID"), "GROUP_ID property is required");
    private static final String TOPIC_RETRY = Objects.requireNonNull(PROPERTIES.getProperty("TOPIC_RETRY"), "TOPIC property is required");
    private static final String GROUP_RETRY = Objects.requireNonNull(PROPERTIES.getProperty("GROUP_RETRY"), "GROUP_ID property is required");
    private static final String BOOTSTRAP_SERVERS = Objects.requireNonNull(PROPERTIES.getProperty("BOOTSTRAP_SERVERS"), "BOOTSTRAP_SERVERS property is required");
    private static final String SERVICE_NAME = Objects.requireNonNull(PROPERTIES.getProperty("SERVICE_NAME"), "SERVICE_NAME property is required");
    private static final String CHECKPOINT_PATH = Objects.requireNonNull(PROPERTIES.getProperty("CHECKPOINT_PATH"), "CHECKPOINT_PATH property is required");
    private static final String TOPIC_ADDRESS_PERSISTED = Objects.requireNonNull(PROPERTIES.getProperty("TOPIC_ADDRESS_PERSISTED"), "TOPIC_ADDRESS_PERSISTED property is required");

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(CHECKPOINT_PATH);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(5), Time.seconds(10)));

        KafkaSource<Client> mainStream = KafkaSource.<Client>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setProperty("enable.auto.commit", "false")
                .setValueOnlyDeserializer(new ClientDeserializer())
                .build();

        KafkaSource<Client> retryStream = KafkaSource.<Client>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC_RETRY)
                .setGroupId(GROUP_RETRY)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("enable.auto.commit", "false")
                .setValueOnlyDeserializer(new ClientDeserializer())
                .build();

        DataStream<Client> mainDataSteam=env.fromSource(mainStream, WatermarkStrategy.noWatermarks(),
                        "Main Kafka Source")
                        .filter(Objects::nonNull)
                        .name("Filter null main");

        DataStream<Client> retryDataSteam=env.fromSource(retryStream, WatermarkStrategy.noWatermarks(),
                        "Retry Kafka Source")
                        .filter(Objects::nonNull)
                        .name("Filter null retry");

        DataStream<Client> clientStream = mainDataSteam.union(retryDataSteam);

        SingleOutputStreamOperator<Client> validClientStream = clientStream
                .process(new ClientValidatorRequiredFields())
                .name("Validate required fields");

        SinkFunction<DeadLetter> deadLetterSink = JdbcSink.sink(
                DeadLetterSink.SQL,
                new DeadLetterSink(),
                FlinkJdbcConfig.execOption(),
                FlinkJdbcConfig.connOption()
        );

        validClientStream.getSideOutput(ClientValidatorRequiredFields.DEAD_LETTER_TAG)
                .addSink(deadLetterSink)
                .name("Dead Letter Sink (required)");

        SingleOutputStreamOperator<Client> accountResult = validClientStream.process(new AccountSink()).name("Customer Sink");

        accountResult.getSideOutput(AccountSink.DEAD_LETTER)
                .addSink(deadLetterSink)
                .name("Dead Letter Sink (account)");

        DataStream<Client.Language> languageStream = validClientStream.flatMap(
                (Client client, org.apache.flink.util.Collector<Client.Language> collector) -> {
                    MDC.put("service", SERVICE_NAME);
                    MDC.put("correlation_id", client.getAccount() != null ? client.getAccount().getCorrelation_id() : "-");
                    try {
                        if (client.getLanguages() != null) {
                            for (Client.Language lang : client.getLanguages()) {
                                lang.setPersonId(client.getPersonId());
                                lang.setCorrelation_id(client.getAccount().getCorrelation_id());
                                collector.collect(lang);
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn("Skipping languages for client {}: {}", client.getPersonId(), e.getMessage());
                    } finally {
                        MDC.clear();
                    }
                }).returns(Client.Language.class);

        SingleOutputStreamOperator<Client.Language> languageResult = languageStream.process(new LanguageSink()).name("Language Sink");
        languageResult.getSideOutput(LanguageSink.DEAD_LETTER).addSink(deadLetterSink).name("Dead Letter Sink (language)");

        DataStream<Client.Nationality> nationalityStream = validClientStream.flatMap(
                (Client client, org.apache.flink.util.Collector<Client.Nationality> collector) -> {
                    MDC.put("service", SERVICE_NAME);
                    MDC.put("correlation_id", client.getAccount() != null ? client.getAccount().getCorrelation_id() : "-");
                    try {
                        if (client.getNationalities() != null) {
                            for (Client.Nationality nat : client.getNationalities()) {
                                nat.setPersonId(client.getPersonId());
                                nat.setCorrelation_id(client.getAccount().getCorrelation_id());
                                collector.collect(nat);
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn("Skipping nationalities for client {}: {}", client.getPersonId(), e.getMessage());
                    } finally {
                        MDC.clear();
                    }
                }).returns(Client.Nationality.class);

        SingleOutputStreamOperator<Client.Nationality> validNationalities = nationalityStream
                .process(new NationalityValidator())
                .name("Validate nationality");

        validNationalities.getSideOutput(SinkValidator.DEAD_LETTER_TAG)
                .addSink(deadLetterSink).name("Dead Letter Sink (nationality)");

        SingleOutputStreamOperator<Client.Nationality> nationalityResult = validNationalities.process(new CountriesSink()).name("Nationality Sink");

        nationalityResult.getSideOutput(CountriesSink.DEAD_LETTER).addSink(deadLetterSink).name("Dead Letter Sink (nationality)");

        DataStream<Client.AddressChannel> addressStream = validClientStream.flatMap(
                (Client client, org.apache.flink.util.Collector<Client.AddressChannel> collector) -> {
                    MDC.put("service", SERVICE_NAME);
                    MDC.put("correlation_id", client.getAccount() != null ? client.getAccount().getCorrelation_id() : "-");
                    try {
                        if (client.getAddressChannels() != null) {
                            for (Client.AddressChannel addr : client.getAddressChannels()) {
                                addr.setPersonId(client.getPersonId());
                                addr.setCorrelation_id(client.getAccount().getCorrelation_id());
                                collector.collect(addr);
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn("Skipping addresses for client {}: {}", client.getPersonId(), e.getMessage());
                    } finally {
                        MDC.clear();
                    }
                }).returns(Client.AddressChannel.class);

        SingleOutputStreamOperator<Client.AddressChannel> addressResult = addressStream.process(new AddressChannelSink()).name("Address Sink");

        addressResult.getSideOutput(AddressChannelSink.DEAD_LETTER).addSink(deadLetterSink).name("Dead Letter Sink (address)");

        KafkaSink<Client.AddressChannel> addressGeocodingKafkaSink = KafkaSink.<Client.AddressChannel>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TOPIC_ADDRESS_PERSISTED)
                        .setValueSerializationSchema(new deserializer.AddressChannelSerializer())
                        .build())
                .build();

        addressResult.sinkTo(addressGeocodingKafkaSink).name("Address Geocoding Kafka Sink");

        DataStream<Client.ContactChannel> contactStream = validClientStream.flatMap(
                (Client client, org.apache.flink.util.Collector<Client.ContactChannel> collector) -> {
                    MDC.put("service", SERVICE_NAME);
                    MDC.put("correlation_id", client.getAccount() != null ? client.getAccount().getCorrelation_id() : "-");
                    try {
                        if (client.getContactChannels() != null) {
                            for (Client.ContactChannel cc : client.getContactChannels()) {
                                cc.setPersonId(client.getPersonId());
                                cc.setCorrelation_id(client.getAccount().getCorrelation_id());
                                collector.collect(cc);
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn("Skipping contacts for client {}: {}", client.getPersonId(), e.getMessage());
                    } finally {
                        MDC.clear();
                    }
                }).returns(Client.ContactChannel.class);

        SingleOutputStreamOperator<Client.ContactChannel> validContacts = contactStream
                .process(new ContactValidator())
                .name("Validate contact");

        validContacts.getSideOutput(SinkValidator.DEAD_LETTER_TAG)
                .addSink(deadLetterSink).name("Dead Letter Sink (contact)");

        SingleOutputStreamOperator<Client.ContactChannel> contactResult = validContacts.process(new ContactChannelSink()).name("Contact Sink");

        contactResult.getSideOutput(ContactChannelSink.DEAD_LETTER).addSink(deadLetterSink).name("Dead Letter Sink (contact)");

        DataStream<Client.CommunicationSubscription> commStream = validClientStream.flatMap(
                (Client client, org.apache.flink.util.Collector<Client.CommunicationSubscription> collector) -> {
                    MDC.put("service", SERVICE_NAME);
                    MDC.put("correlation_id", client.getAccount() != null ? client.getAccount().getCorrelation_id() : "-");
                    try {
                        if (client.getCommunicationSubscriptions() != null) {
                            for (Client.CommunicationSubscription comm : client.getCommunicationSubscriptions()) {
                                comm.setPersonId(client.getPersonId());
                                comm.setCorrelation_id(client.getAccount().getCorrelation_id());
                                collector.collect(comm);
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn("Skipping subscriptions for client {}: {}", client.getPersonId(), e.getMessage());
                    } finally {
                        MDC.clear();
                    }
                }).returns(Client.CommunicationSubscription.class);

        SingleOutputStreamOperator<Client.CommunicationSubscription> validComms = commStream
                .process(new CommunicationSubscriptionValidator())
                .name("Validate communication subscription");

        validComms.getSideOutput(SinkValidator.DEAD_LETTER_TAG)
                .addSink(deadLetterSink).name("Dead Letter Sink (communication subscription)");

        SingleOutputStreamOperator<Client.CommunicationSubscription> validCommsResult=validComms.process(new CommunicationSubscriptionSink()).name("Communication Subscription Sink");
        validCommsResult.getSideOutput(CommunicationSubscriptionSink.DEAD_LETTER).addSink(deadLetterSink).name("Dead Letter Sink (communication subscription)");

        SingleOutputStreamOperator<Client> digitalAccessStream = validClientStream.flatMap(
                (Client client, org.apache.flink.util.Collector<Client> collector) -> {
                    MDC.put("service", SERVICE_NAME);
                    MDC.put("correlation_id", client.getAccount() != null ? client.getAccount().getCorrelation_id() : "-");
                    try {
                        if (client.getDigitalAccess() != null) {
                            collector.collect(client);
                        }
                    } catch (Exception e) {
                        LOG.warn("Skipping digital access for client {}: {}", client.getPersonId(), e.getMessage());
                    } finally {
                        MDC.clear();
                    }
                }).returns(Client.class)
                .process(new DigitalAccessValidator())
                .name("Validate digital access");

        digitalAccessStream.getSideOutput(SinkValidator.DEAD_LETTER_TAG)
                .addSink(deadLetterSink).name("Dead Letter Sink (digital access)");

        SingleOutputStreamOperator<Client> digiResult=digitalAccessStream.process(new DigitalAccessSink()).name("Digital Access Sink");
        digiResult.getSideOutput(DigitalAccessSink.DEAD_LETTER).addSink(deadLetterSink).name("Dead Letter Sink (digital access)");
        SingleOutputStreamOperator<Client> loyaltyStream = validClientStream.flatMap(
                (Client client, org.apache.flink.util.Collector<Client> collector) -> {
                    MDC.put("service", SERVICE_NAME);
                    MDC.put("correlation_id", client.getAccount() != null ? client.getAccount().getCorrelation_id() : "-");
                    try {
                        if (client.getLoyalty() != null) {
                            collector.collect(client);
                        }
                    } catch (Exception e) {
                        LOG.warn("Skipping loyalty for client {}: {}", client.getPersonId(), e.getMessage());
                    } finally {
                        MDC.clear();
                    }
                }).returns(Client.class)
                .process(new LoyaltyValidator())
                .name("Validate loyalty status");
        loyaltyStream.getSideOutput(SinkValidator.DEAD_LETTER_TAG)
                .addSink(deadLetterSink).name("Dead Letter Sink (loyalty)");
        SingleOutputStreamOperator<Client> loyResult = loyaltyStream.process(new LoyaltyStatusSink()).name("Loyalty Status Sink");
        loyResult.getSideOutput(LoyaltyStatusSink.DEAD_LETTER).addSink(deadLetterSink).name("Dead Letter Sink (loyalty)");


        SingleOutputStreamOperator<Client> indicatorStream = validClientStream.flatMap(
                (Client client, org.apache.flink.util.Collector<Client> collector) -> {
                    MDC.put("service", SERVICE_NAME);
                    MDC.put("correlation_id", client.getAccount() != null ? client.getAccount().getCorrelation_id() : "-");
                    try {
                        if (client.getAccountIndicators() != null) {
                            collector.collect(client);
                        }
                    } catch (Exception e) {
                        LOG.warn("Skipping indicator for client {}: {}", client.getPersonId(), e.getMessage());
                    } finally {
                        MDC.clear();
                    }
                }).returns(Client.class)
                .process(new CustomerIndicatorValidator())
                .name("Validate customer indicator");
        indicatorStream.getSideOutput(SinkValidator.DEAD_LETTER_TAG)
                .addSink(deadLetterSink).name("Dead Letter Sink (indicator)");
        SingleOutputStreamOperator<Client> indResult = indicatorStream.process(new CustomerIndicatorSink()).name("Customer Indicator Sink");
        indResult.getSideOutput(CustomerIndicatorSink.DEAD_LETTER).addSink(deadLetterSink).name("Dead Letter Sink (indicator)");

        env.execute("Client Processing Job");
    }
}
