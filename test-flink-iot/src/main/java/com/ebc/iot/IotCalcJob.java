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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.List;
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
                ).flatMap((String value, Collector<PointData> out) -> {
                    try {
                        JsonNode root = mapper.readTree(value);
                        String payloadStr = root.path("payload").asText();
                        if (payloadStr == null || payloadStr.isEmpty()) {
                            // 兼容直接是 PointData 的格式
                            PointData data = mapper.readValue(value, PointData.class);
                            if (data != null && data.getTimestamp() != null) out.collect(data);
                            return;
                        }

                        JsonNode payload = mapper.readTree(payloadStr);
                        String meter = payload.path("meter").asText();
                        JsonNode datas = payload.path("datas");
                        
                        // 从 topic 中提取 companyId 和 projectId
                        // topic 格式: /data/asiainfo/xs2/udp/396/161
                        String topic = root.path("topic").asText("");
                        String[] parts = topic.split("/");
                        String companyId = parts.length >= 6 ? parts[5] : "unknown";
                        String projectId = parts.length >= 7 ? parts[6] : "unknown";
                        String gatewayCode = parts.length >= 5 ? parts[4] : "unknown";

                        if (datas.isArray()) {
                            for (JsonNode node : datas) {
                                PointData p = new PointData();
                                p.setCompany_id(companyId);
                                p.setProject_id(projectId);
                                p.setDevice_id(meter);
                                p.setGateway_code(gatewayCode);
                                p.setData_type("source"); // 输入数据认为是 source
                                p.setProperty_name(node.path("nm").asText());
                                p.setProperty_num_value(node.path("v").asDouble());
                                p.setTimestamp(node.path("ts").asLong() * 1000); // 假设 ts 是秒级
                                out.collect(p);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Failed to parse Kafka message: {}, error: {}", value, e.getMessage());
                    }
                }, TypeInformation.of(PointData.class)).filter(Objects::nonNull).
                assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<PointData>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, ts) -> {
                                    if (event.getTimestamp() == null) {
                                        return System.currentTimeMillis();
                                    }
                                    return event.getTimestamp();
                                })
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
                .keyBy(new KeySelector<PointData, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(PointData p) {
                        return new Tuple2<>(p.getCompany_id(), p.getDevice_id());
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(1)))
                .aggregate(new LatestPointAgg());

        // ========================
        // 5️⃣ 计算
        // ========================
        DataStream<List<PointData>> resultStream = windowedStream
                .keyBy(new KeySelector<Collection<PointData>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Collection<PointData> coll) {
                        if (coll == null || coll.isEmpty()) {
                            return new Tuple2<>("unknown", "unknown");
                        }
                        PointData first = coll.iterator().next();
                        String companyId = first.getCompany_id() != null ? first.getCompany_id() : "unknown";
                        String deviceId = first.getDevice_id() != null ? first.getDevice_id() : "unknown";
                        return new Tuple2<>(companyId, deviceId);
                    }
                })
                .connect(broadcastRules)
                .process(new CalcProcessFunction());


        // ========================
        // 5️⃣ Kafka Sink
        // ========================
        DataStream<String> resultJsonStream = resultStream.map(new RichMapFunction<List<PointData>, String>() {
            private transient ObjectMapper jsonMapper;

            @Override
            public void open(Configuration parameters) {
                jsonMapper = new ObjectMapper();
            }

            @Override
            public String map(List<PointData> points) throws Exception {
                if (points == null || points.isEmpty()) return null;
                
                PointData first = points.get(0);
                
                // 1. 构建内部 payload 结构
                Map<String, Object> innerPayload = new HashMap<>();
                List<Map<String, Object>> datas = new ArrayList<>();
                
                for (PointData p : points) {
                    Map<String, Object> dataPoint = new HashMap<>();
                    dataPoint.put("nm", p.getProperty_name());
                    dataPoint.put("v", p.getProperty_num_value());
                    dataPoint.put("ts", p.getTimestamp() != null ? p.getTimestamp() / 1000 : 0);
                    datas.add(dataPoint);
                }
                
                innerPayload.put("datas", datas);
                innerPayload.put("meter", first.getDevice_id());
                innerPayload.put("mid", "CALC-" + System.currentTimeMillis());
                
                // 2. 构建外部包装结构
                Map<String, Object> wrap = new HashMap<>();
                wrap.put("parseType", "calc");
                wrap.put("payload", jsonMapper.writeValueAsString(innerPayload));
                
                String companyId = first.getCompany_id() != null ? first.getCompany_id() : "unknown";
                String projectId = first.getProject_id() != null ? first.getProject_id() : "unknown";
                String topic = String.format("/data/asiainfo/calc/tcp/%s/%s", companyId, projectId);
                wrap.put("topic", topic);
                
                return jsonMapper.writeValueAsString(wrap);
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
        resultStream.print();

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
            String ptName = value.getProperty_name();
            if (ptName == null) return acc;
            
            PointData cur = acc.get(ptName);
            long newTs = value.getTimestamp() != null ? value.getTimestamp() : 0L;
            long curTs = (cur != null && cur.getTimestamp() != null) ? cur.getTimestamp() : -1L;
            
            if (cur == null || newTs >= curTs) {
                acc.put(ptName, value);
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
                if (cur == null || e.getValue().getTimestamp() >= cur.getTimestamp()) {
                    a.put(e.getKey(), e.getValue());
                }
            }
            return a;
        }
    }
}
