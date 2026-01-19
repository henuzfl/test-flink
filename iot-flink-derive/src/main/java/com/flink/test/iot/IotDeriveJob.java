package com.flink.test.iot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.test.iot.function.DeriveProcessFunction;
import com.flink.test.iot.model.PointData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

/**
 * IoT 点位衍生计算作业 - 高性能生产版
 * 特性：
 * 1. Kafka Source + Flink CDC Source
 * 2. JSON 解析优化
 * 3. EventTime + Watermark 触发 onTimer
 * 4. Tuple2 KeyBy 避免字符串拼接
 * 5. Kafka Sink 批量发送，保证至少一次
 */
public class IotDeriveJob {

    private static final Logger logger = LoggerFactory.getLogger(IotDeriveJob.class);
    private static final String OUTPUT_TOPIC = "output-topic-2";

    public static void main(String[] args) throws Exception {
        // 创建 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); // 本地调试或测试可以使用 1，生产可按资源调整

        final ObjectMapper mapper = new ObjectMapper();

        // ========================
        // 1️⃣ Kafka Source
        // ========================
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.19.93.228:9092")
                .setTopics("demo-flink-02")
                .setGroupId("iot-derive-group-1")
                .setStartingOffsets(OffsetsInitializer.latest()) // 最新偏移量启动
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        // 1. 获取 Kafka 流并直接在 Source 端通过解析 JSON 生成 Watermark
        // 这种方式最健壮，能自动解决 Kafka 分区空闲导致的 Watermark 不推进问题
        // 1. 获取 Kafka 流，去掉 Watermark
        DataStream<PointData> pointStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        ).map(value -> {
            try {
                return mapper.readValue(value, PointData.class);
            } catch (Exception e) {
                return null;
            }
        }).filter(Objects::nonNull);

        // ========================
        // 3️⃣ Flink CDC Source
        // ========================
        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("useSSL", "false");
        jdbcProps.setProperty("allowPublicKeyRetrieval", "true");
        jdbcProps.setProperty("useUnicode", "true");
        jdbcProps.setProperty("characterEncoding", "UTF-8");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.19.93.240")
                .port(3306)
                .databaseList("demo_flink") 
                .tableList("demo_flink.iot_point_def") 
                .username("root")
                .password("ih9PExr0RNojo20r%")
                .serverTimeZone("Asia/Shanghai")
                .jdbcProperties(jdbcProps)
                .deserializer(new JsonDebeziumDeserializationSchema()) 
                .startupOptions(StartupOptions.initial()) 
                .build();

        // 3. 读取 MySQL 规则流，同样去掉 Watermark
        DataStream<String> ruleStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source"
        );

        // 广播规则状态
        BroadcastStream<String> broadcastRules = ruleStream.broadcast(DeriveProcessFunction.RULES_STATE_DESC);

        // ========================
        // 4️⃣ Connect + DeriveProcessFunction
        // ========================
        DataStream<PointData> resultStream = pointStream
                // 使用 Tuple2 作为 Key 避免字符串拼接
                .keyBy((KeySelector<PointData, Tuple2<Integer, String>>) value -> new Tuple2<>(value.getCompany_id(), value.getDevice_code()))
                .connect(broadcastRules)
                .process(new DeriveProcessFunction());

        // ========================
        // 5️⃣ Kafka Sink
        // ========================
        DataStream<String> resultJsonStream = resultStream.map(new RichMapFunction<>() {
            private transient ObjectMapper jsonMapper;

            @Override
            public void open(Configuration parameters) {
                jsonMapper = new ObjectMapper();
            }

            @Override
            public String map(PointData pointData) throws Exception {
                return jsonMapper.writeValueAsString(pointData);
            }
        });

        // Kafka 生产者配置
        Properties kafkaProps = new Properties();
        kafkaProps.put("linger.ms", "50"); // 批量发送延迟
        kafkaProps.put("batch.size", "16384"); // 批量大小

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("10.19.93.228:9092")
                .setKafkaProducerConfig(kafkaProps)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // 至少一次
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(OUTPUT_TOPIC)
                        .setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                        .build()
                )
                .build();

        // 写入 Kafka
        resultJsonStream.sinkTo(kafkaSink);

        // 调试输出（生产可注释）
        resultStream.print("Derived Result");

        logger.info("启动 IoT Flink 衍生计算作业 (CDC Enabled, Flink 1.19), 输出 topic: {}", OUTPUT_TOPIC);
        env.execute("IoT Flink Derive Job");
    }
}
