import Deserializer.ClientDeserializer;
import Dto.Client;
import Dto.LanguageSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import Dto.AccountSink;

import java.util.stream.Collector;

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

        DataStream<Client.Language> languageStream = clientStream.flatMap(
        (Client client, org.apache.flink.util.Collector<Client.Language> collector) -> {
                    for (Client.Language lang : client.getLanguages()) {
                        lang.setPersonId(client.getPersonId());
                        collector.collect(lang);
                    }
                }
        ).returns(Client.Language.class);


        clientStream.addSink(JdbcSink.sink(
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

        clientStream.print();

        env.execute("Client Processing Job");
    }
}
