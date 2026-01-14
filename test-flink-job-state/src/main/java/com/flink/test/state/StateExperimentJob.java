package com.flink.test.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.test.common.FlinkConstants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink State and Checkpoint Experiment Job.
 * Accumulates 'value' per 'deviceId' using ValueState.
 */
public class StateExperimentJob {

    private static final Logger LOG = LoggerFactory.getLogger(StateExperimentJob.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Checkpoint Configuration 
        // 只要集群配置了 state.checkpoints.dir，这里只需开启即可。
        env.enableCheckpointing(10000); 
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        // 2. Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkConstants.DEFAULT_KAFKA_SERVER)
                .setTopics(FlinkConstants.INPUT_TOPIC)
                .setGroupId("state-experiment-group")
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
