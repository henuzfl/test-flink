package com.ebc.iot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ebc.common.ConfigLoader;
import com.ebc.iot.function.CalcProcessFunction;
import com.ebc.iot.model.PointData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

import java.util.Objects;
import java.util.Properties;

@Slf4j
public class IotCalcJob {

    private static final String OUTPUT_TOPIC = "output-topic-3";

    public static void main(String[] args) throws Exception {
        // ========================
        // 0️⃣ Load Configuration
        // ========================
        ParameterTool params = ConfigLoader.loadConfig(args, "application.yml");


        /**
         * 创建 Flink 流执行环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        final ObjectMapper mapper = new ObjectMapper();
        // ========================
        // 1️⃣ Kafka Source
        // ========================
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "10.19.93.228:9092"))
                .setTopics(params.get("kafka.input.topic", "demo-flink-02"))
                .setGroupId(params.get("kafka.group.id", "iot-derive-group-1"))
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
                }).filter(Objects::nonNull).
                assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<PointData>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );

        // ========================
        // 3️⃣ Flink CDC Source
        // ========================
        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("useSSL", "false");
        jdbcProps.setProperty("allowPublicKeyRetrieval", "true");
        jdbcProps.setProperty("useUnicode", "true");
        jdbcProps.setProperty("characterEncoding", "UTF-8");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(params.get("mysql.hostname", "10.19.93.240"))
                .port(params.getInt("mysql.port", 3306))
                .databaseList(params.get("mysql.database", "demo_flink"))
                .tableList(params.get("mysql.table", "demo_flink.iot_point_def"))
                .username(params.get("mysql.username", "root"))
                .password(params.get("mysql.password", "ih9PExr0RNojo20r%"))
                .serverTimeZone(params.get("mysql.timezone", "Asia/Shanghai"))
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
        org.apache.flink.streaming.api.datastream.BroadcastStream<String> broadcastRules = ruleStream.broadcast(CalcProcessFunction.RULES_STATE_DESC);

        // ========================
        // 4️⃣ 窗口处理：按设备聚合，收集 15 分钟内所有点位的最新值，每分钟触发一次
        // ========================
        DataStream<Collection<PointData>> windowedStream = pointStream
                .keyBy(new KeySelector<PointData, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> getKey(PointData p) {
                        return new Tuple2<>(p.getCompany_id(), p.getDevice_code());
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(1)))
                .aggregate(new LatestPointAgg());

        // ========================
        // 5️⃣ 计算
        // ========================
        DataStream<PointData> resultStream = windowedStream
                .keyBy(new KeySelector<Collection<PointData>, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> getKey(Collection<PointData> coll) {
                        PointData first = coll.iterator().next();
                        return new Tuple2<>(first.getCompany_id(), first.getDevice_code());
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

        String outputTopic = params.get("kafka.output.topic", OUTPUT_TOPIC);
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "10.19.93.228:9092"))
                .setKafkaProducerConfig(kafkaProps)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // 至少一次
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        // 写入 Kafka
        resultJsonStream.sinkTo(kafkaSink);

        // 调试输出（生产可注释）
        resultStream.print("Calc Result");

        log.info("启动 IoT Flink 衍生计算作业 (CDC Enabled, Flink 1.19), 输出 topic: {}", outputTopic);
        env.execute("IoT Calc Job");

    }

    public static class LatestPointAgg
            implements AggregateFunction<PointData, Map<String, PointData>, Collection<PointData>> {

        @Override
        public Map<String, PointData> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, PointData> add(PointData value, Map<String, PointData> acc) {
            PointData cur = acc.get(value.getPoint_code());
            if (cur == null || value.getTs() >= cur.getTs()) {
                acc.put(value.getPoint_code(), value);
            }
            return acc;
        }

        @Override
        public Collection<PointData> getResult(Map<String, PointData> acc) {
            return new ArrayList<>(acc.values());
        }

        @Override
        public Map<String, PointData> merge(Map<String, PointData> a, Map<String, PointData> b) {
            for (Map.Entry<String, PointData> e : b.entrySet()) {
                PointData cur = a.get(e.getKey());
                if (cur == null || e.getValue().getTs() >= cur.getTs()) {
                    a.put(e.getKey(), e.getValue());
                }
            }
            return a;
        }
    }
}
