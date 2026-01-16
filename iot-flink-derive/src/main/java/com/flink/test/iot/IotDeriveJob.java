package com.flink.test.iot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.test.iot.function.DeriveProcessFunction;
import com.flink.test.iot.model.PointData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * IoT Point Derivation Job
 *
 * Requirements:
 * 1. Flink 1.9.x compatible (Target)
 * 2. Kafka Source (JSON)
 * 3. Keyed State Calculation
 * 4. Output to Console (Demo)
 */
public class IotDeriveJob {

    private static final Logger logger = LoggerFactory.getLogger(IotDeriveJob.class);
    private static final String OUTPUT_TOPIC = "output-topic-2";

    public static void main(String[] args) throws Exception {
        // 1. Environment Setup
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1); // Default for demo
        
        // WORKAROUND: For Java 9+ environment running Flink 1.9
        // Resolves "InaccessibleObjectException: Unable to make field private static final long java.util.Properties.serialVersionUID accessible"
        env.getConfig().disableClosureCleaner();

        // 2. Kafka Source Configuration
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.19.93.228:9092");
        props.setProperty("group.id", "iot-derive-group");
        // Ensure auto-commit is off to let Flink manage offsets if checkpointing is enabled
        // (Checkpointing not explicitly asked but good practice)
        
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "demo-flink-02",
                new SimpleStringSchema(),
                props);
        
        // Start from latest for demo, or group offsets
        consumer.setStartFromLatest();

        DataStream<String> rawStream = env.addSource(consumer);

        // 3. Parse JSON & Assign Watermarks
        // Using ObjectMapper to parse JSON
        final ObjectMapper mapper = new ObjectMapper();

        DataStream<PointData> pointStream = rawStream
                .flatMap(new FlatMapFunction<String, PointData>() {
                    @Override
                    public void flatMap(String value, Collector<PointData> out) {
                        try {
                            PointData data = mapper.readValue(value, PointData.class);
                            if (data != null && data.getPoint_code() != null) {
                                logger.info("Received PointData: company_id={}, device_code={}, point_code={}, value={}, ts={}", 
                                        data.getCompany_id(), data.getDevice_code(), data.getPoint_code(), 
                                        data.getValue(), data.getTs());
                                out.collect(data);
                            } else {
                                logger.warn("Invalid PointData (null or missing point_code): {}", value);
                            }
                        } catch (Exception e) {
                            // Ignore bad data in demo
                            logger.warn("Parse Error for JSON: {}, error: {}", value, e.getMessage());
                        }
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<PointData>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(PointData element) {
                        return element.getTs();
                    }
                });

        // 4. KeyBy and Process
        // Key: company_id + device_code
        DataStream<PointData> resultStream = pointStream
                .keyBy(new KeySelector<PointData, String>() {
                    @Override
                    public String getKey(PointData value) {
                        return value.getCompany_id() + "_" + value.getDevice_code();
                    }
                })
                .timeWindow(Time.minutes(15), Time.minutes(1))
                .process(new DeriveProcessFunction());

        // 5. Output to Kafka and Log
        // Convert PointData to JSON string for Kafka
        DataStream<String> resultJsonStream = resultStream.map(new RichMapFunction<PointData, String>() {
            private transient ObjectMapper jsonMapper;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                jsonMapper = new ObjectMapper();
            }

            @Override
            public String map(PointData pointData) throws Exception {
                try {
                    return jsonMapper.writeValueAsString(pointData);
                } catch (Exception e) {
                    logger.error("Failed to serialize PointData to JSON: {}", pointData, e);
                    return null;
                }
            }
        }).filter(json -> json != null);

        // Sink to Kafka
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                OUTPUT_TOPIC,
                new SimpleStringSchema(),
                props);
        resultJsonStream.addSink(kafkaProducer);
        
        // Also log for debugging (optional)
        resultStream.print("Derived Result");

        // 6. Execute
        logger.info("Starting IoT Flink Derive Job, output topic: {}", OUTPUT_TOPIC);
        env.execute("IoT Flink Derive Job");
    }
}
