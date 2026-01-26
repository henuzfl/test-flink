package com.flink.test;

import com.flink.test.common.FlinkConstants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Flink Kafka Streaming Job in a separate module
 */
public class KafkaStreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamingJob.class);

    public static void main(String[] args) throws Exception {
        // Load Configuration
        org.apache.flink.api.java.utils.ParameterTool params = com.flink.test.common.ConfigLoader.loadConfig(args, "application.yml");


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        
        // Enable checkpointing configuration
        env.enableCheckpointing(params.getLong("checkpoint.interval", 10000));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(params.getLong("checkpoint.min.pause", 5000));
        env.getCheckpointConfig().setCheckpointTimeout(params.getLong("checkpoint.timeout", 60000));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // Only one checkpoint at a time
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // Retain checkpoint on cancellation
        
        // Restart strategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // Number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // Delay between restarts
        ));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", FlinkConstants.DEFAULT_KAFKA_SERVER))
                .setTopics(params.get("kafka.input.topic", FlinkConstants.INPUT_TOPIC))
                .setGroupId(params.get("kafka.group.id", "my-group"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<String> transformedStream = stream.map(value -> {
            LOG.info("Received message: {}", value);
            return "Processed: " + value;
        });

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", FlinkConstants.DEFAULT_KAFKA_SERVER))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(params.get("kafka.output.topic", FlinkConstants.OUTPUT_TOPIC))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        transformedStream.print();
        transformedStream.sinkTo(sink);

        LOG.info("Starting Flink Job in test-flink-job-kafka module...");
        env.execute("Flink Kafka Streaming Job");

    }
}
