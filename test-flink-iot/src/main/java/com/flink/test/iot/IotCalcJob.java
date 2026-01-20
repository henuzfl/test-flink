package com.flink.test.iot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.test.iot.function.CalcProcessFunction;
import com.flink.test.iot.model.PointData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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

import java.util.Objects;
import java.util.Properties;

@Slf4j
public class IotCalcJob {

    private static final String OUTPUT_TOPIC = "output-topic-3";

    public static void main(String[] args) throws Exception {
        /**
         * 创建 Flink 流执行环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
        /**
         * 2️⃣ 读取 Kafka 点位数据流，并解析为 PointData 对象
         */
        DataStream<PointData> pointStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "iot data source"
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
        /**
         * 规则数据流
         */
        DataStream<String> ruleStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "iot rule source"
        );

        // 广播规则状态
        BroadcastStream<String> broadcastRules = ruleStream.broadcast(CalcProcessFunction.RULES_STATE_DESC);

        // ========================
        // 4️⃣ 计算
        // ========================
        DataStream<PointData> resultStream = pointStream
                .keyBy(new KeySelector<PointData, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> getKey(PointData value) {
                        return new Tuple2<>(value.getCompany_id(), value.getDevice_code());
                    }
                })
                .connect(broadcastRules)
                .process(new CalcProcessFunction());

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
        kafkaProps.put("linger.ms", "50");
        kafkaProps.put("batch.size", "16384");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("10.19.93.228:9092")
                .setKafkaProducerConfig(kafkaProps)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // 至少一次
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(OUTPUT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        // 写入 Kafka
        resultJsonStream.sinkTo(kafkaSink);

        // 调试输出（生产可注释）
        resultStream.print("Calc Result");

        log.info("启动 IoT Flink 衍生计算作业 (CDC Enabled, Flink 1.19), 输出 topic: {}", OUTPUT_TOPIC);
        env.execute("IoT Calc Job");
    }
}
