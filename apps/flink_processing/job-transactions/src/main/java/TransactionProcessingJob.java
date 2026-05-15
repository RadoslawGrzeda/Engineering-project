//package com.retailplatform.transactions;

import deserializer.TransactionDeserializer;
import dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.jdbc.JdbcSink;
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
import config.DeadLetter;
import config.FlinkPostgresConfig;
import sink.*;
import validator.TransactionValidatorRequiredFields;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

public class TransactionProcessingJob {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionProcessingJob.class);
    private static final Properties PROPERTIES = new Properties();

    static {
        try (InputStream input = TransactionProcessingJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find config.properties");
            }
            PROPERTIES.load(input);
        } catch (IOException e) {
            LOG.error("Error loading config.properties", e);
            throw new RuntimeException(e);
        }
    }

    private static final String TOPIC = Objects.requireNonNull(PROPERTIES.getProperty("TOPIC"), "TOPIC property is required");
    private static final String BOOTSTRAP_SERVER = Objects.requireNonNull(PROPERTIES.getProperty("BOOTSTRAP_SERVER"), "BOOTSTRAP_SERVER property is required");
    private static final String GROUP_ID = Objects.requireNonNull(PROPERTIES.getProperty("GROUP_ID"), "GROUP_ID property is required");
    private static final String SERVICE_NAME = Objects.requireNonNull(PROPERTIES.getProperty("SERVICE_NAME"), "SERVICE_NAME property is required");
    private static final String CHECKPOINT_PATH = Objects.requireNonNull(PROPERTIES.getProperty("CHECKPOINT_PATH"), "CHECKPOINT_PATH property is required");

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
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(5),Time.seconds(10_000)));


        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setTopics(TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setProperty("enable.auto.commit", "false")
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();

        SinkFunction<DeadLetter> deadLetterSink = JdbcSink.sink(
                DeadLetterSink.SQL,
                new DeadLetterSink(),
                FlinkPostgresConfig.execOption(),
                FlinkPostgresConfig.connOption()
        );

        DataStream<Transaction> transactionStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Transaction Kafka Source")
                .filter(Objects::nonNull)
                .name("Filter null transactions");

        SingleOutputStreamOperator<Transaction> validStream = transactionStream
                .process(new TransactionValidatorRequiredFields())
                .name("Validate required fields");

        validStream.getSideOutput(TransactionValidatorRequiredFields.DEAD_LETTER_TAG)
                .addSink(deadLetterSink)
                .name("Dead Letter Sink (validation)");

        SingleOutputStreamOperator<Transaction> factResult = validStream
                .process(new FactTransactionSink())
                .name("Fact Transaction Sink");

        factResult.getSideOutput(FactTransactionSink.DEAD_LETTER)
                .addSink(deadLetterSink)
                .name("Dead Letter Sink (fact)");

        env.execute("Transaction Processing Job");
    }
}

