package com.flink.test.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.test.common.FlinkConstants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Flink State and Checkpoint Experiment Job.
 * Accumulates 'value' per 'deviceId' using ValueState.
 */
public class StateExperimentJob {

    private static final Logger LOG = LoggerFactory.getLogger(StateExperimentJob.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // Load Configuration
        org.apache.flink.api.java.utils.ParameterTool params = com.flink.test.common.ConfigLoader.loadConfig(args, "application.yml");


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // 1. Checkpoint Configuration 
        // 只要集群配置了 state.checkpoints.dir，这里只需开启即可。
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

        // 2. Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", FlinkConstants.DEFAULT_KAFKA_SERVER))
                .setTopics(params.get("kafka.input.topic", FlinkConstants.INPUT_TOPIC))
                .setGroupId(params.get("kafka.group.id", "state-experiment-group"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 3. Process Logic
        DataStream<String> resultStream = rawStream
                .flatMap((String value, Collector<DeviceData> out) -> {
                    try {
                        // Handle dirty data to avoid NPE/Crash
                        DeviceData data = OBJECT_MAPPER.readValue(value, DeviceData.class);
                        if (data != null && data.getDeviceId() != null) {
                            out.collect(data);
                        }
                    } catch (Exception e) {
                        LOG.error("Failed to parse JSON: {}", value, e);
                    }
                })
                .returns(Types.POJO(DeviceData.class))
                .keyBy(DeviceData::getDeviceId)
                .process(new AccumulatorFunction());


        // 4. Sink (Print to stdout for easy experimentation)
        resultStream.print();

        // 5. Kafka Sink
        // Note: EXACTLY_ONCE needs checkpointing enabled (already enabled above) and a transactionalIdPrefix.
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("transaction.timeout.ms", params.get("kafka.transaction.timeout.ms", "600000"));

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", FlinkConstants.DEFAULT_KAFKA_SERVER))
                .setKafkaProducerConfig(kafkaProps)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(params.get("kafka.output.topic", FlinkConstants.VALUE_AGG_OUTPUT_TOPIC))
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(params.get("kafka.transaction.id.prefix", "state-experiment-job"))
                .build();

        resultStream.sinkTo(kafkaSink);

        LOG.info("Starting State Experiment Job...");
        env.execute("Flink State Experiment Job");
    }


    /**
     * State-based function to accumulate values.
     */
    public static class AccumulatorFunction extends KeyedProcessFunction<String, DeviceData, String> {
        
        // Keyed state for keeping the sum per deviceId
        private transient ValueState<Long> sumState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Long> descriptor = 
                new ValueStateDescriptor<>("device-sum-state", Types.LONG);
            sumState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(DeviceData value, Context ctx, Collector<String> out) throws Exception {
            // Get current sum from state
            Long currentSum = sumState.value();
            if (currentSum == null) {
                currentSum = 0L;
            }

            // Accumulate
            currentSum += value.getValue();

            // Store back to state
            sumState.update(currentSum);

            // Output result (Rule: Avoid INFO log in hot operator, using collector instead)
            out.collect("Device: " + ctx.getCurrentKey() + " | Current Value: " + value.getValue() + " | Total Sum: " + currentSum);
        }
    }
}
