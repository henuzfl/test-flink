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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing configuration
        env.enableCheckpointing(10000); // Checkpoint every 10 seconds
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000); // Minimum pause between checkpoints: 5 seconds
        env.getCheckpointConfig().setCheckpointTimeout(60000); // Checkpoint timeout: 60 seconds
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // Only one checkpoint at a time
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // Retain checkpoint on cancellation
        
        // Restart strategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // Number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // Delay between restarts
        ));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkConstants.DEFAULT_KAFKA_SERVER)
                .setTopics(FlinkConstants.INPUT_TOPIC)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<String> transformedStream = stream.map(value -> {
            LOG.info("Received message: {}", value);
            return "Processed: " + value;
        });

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(FlinkConstants.DEFAULT_KAFKA_SERVER)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(FlinkConstants.OUTPUT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        transformedStream.print();
        transformedStream.sinkTo(sink);

        LOG.info("Starting Flink Job in test-flink-job-kafka module...");
        env.execute("Flink Kafka Streaming Job");
    }
}
