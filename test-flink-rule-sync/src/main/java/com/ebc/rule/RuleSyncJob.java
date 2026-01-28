package com.ebc.rule;

import com.ebc.common.config.ConfigLoader;
import com.ebc.common.model.DevicePointRule;
import com.ebc.rule.function.FullRuleSyncFunction;
import com.ebc.common.event.BusLakConfigEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import static com.ebc.rule.function.FullRuleSyncFunction.MODELS_STATE;
import static com.ebc.rule.function.FullRuleSyncFunction.OBJECTS_STATE;
import static com.ebc.rule.function.FullRuleSyncFunction.POINTS_STATE;

/**
 * 规则多表同步 Job (DataStream 模式)
 * 监听三张表：bus_object_point_data, bus_object_info, base_model_info
 */
public class RuleSyncJob {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ConfigLoader.loadConfig(args, "application.yml");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(params.getInt("flink.parallelism", 1));

        // 1. 定义源数据库 CDC Source (支持多表监听)
        String sourceHost = params.get("source-mysql.hostname", "localhost");
        int sourcePort = params.getInt("source-mysql.port", 3306);
        String sourceUser = params.get("source-mysql.username", "root");
        String sourcePass = params.get("source-mysql.password", "ih9PExr0RNojo20r%");
        String sourceDb = params.get("source-mysql.database", "zhny_business_lak");

        DataStream<BusLakConfigEvent> combinedStream = env.fromSource(
                MySqlSource.<String>builder()
                        .hostname(sourceHost).port(sourcePort).username(sourceUser).password(sourcePass)
                        .databaseList(sourceDb)
                        .tableList(sourceDb + ".bus_object_info", sourceDb + ".base_model_info", sourceDb + ".bus_object_point_data_test")
                        .splitSize(40960) // 设置分片大小
                        .fetchSize(10000)  // 设置抓取批次大小
                        .deserializer(new JsonDebeziumDeserializationSchema()).build(),
                WatermarkStrategy.noWatermarks(), "Combined CDC Source"
        ).flatMap((String json, Collector<BusLakConfigEvent> out) -> {
            try {
                JsonNode node = MAPPER.readTree(json);
                String table = node.path("source").path("table").asText();
                String op = node.path("op").asText();
                JsonNode data = "d".equals(op) ? node.get("before") : node.get("after");
                if (data != null && !data.isNull()) {
                    if ("bus_object_point_data_test".equals(table)) {
                        if (data.path("data_source").asInt() == 2) {
                            out.collect(new BusLakConfigEvent(table, op, data));
                        }
                    } else {
                        // 2. 其他表 (info, model) 全量同步
                        out.collect(new BusLakConfigEvent(table, op, data));
                    }
                }
            } catch (Exception e) {
                // 解析失败忽略或打日志
            }
        }, TypeInformation.of(BusLakConfigEvent.class)); // 显式提供类型信息获取器

        // 2. 连接广播流并进行处理
        DataStream<DevicePointRule> syncStream = env.fromElements("")
                .connect(combinedStream.broadcast(POINTS_STATE, OBJECTS_STATE, MODELS_STATE))
                .process(new FullRuleSyncFunction());

        // 3. 写入目标数据库 (demo_flink.iot_point_def)
        configureSink(syncStream, params);

        env.execute("Full Rule Sync Job");
    }

    private static void configureSink(DataStream<DevicePointRule> stream, ParameterTool params) {
        String targetUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=UTF-8",
                params.get("target-mysql.hostname"), params.getInt("target-mysql.port"), params.get("target-mysql.database"));

        String upsertSql = "INSERT INTO iot_point_def_test (company_id, device_code, point_code, point_type, value_type, expr_type, expr, depends_on, enabled) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "point_type=VALUES(point_type), value_type=VALUES(value_type), expr_type=VALUES(expr_type), " +
                "expr=VALUES(expr), depends_on=VALUES(depends_on), enabled=VALUES(enabled)";

        stream.addSink(JdbcSink.sink(
                upsertSql,
                (ps, rule) -> {
                    ps.setString(1, rule.getCompanyId());
                    ps.setString(2, rule.getDeviceCode());
                    ps.setString(3, rule.getPointCode());
                    ps.setInt(4, rule.getPointType());
                    ps.setInt(5, rule.getValueType());
                    ps.setInt(6, rule.getExprType());
                    ps.setString(7, rule.getExpr());
                    ps.setString(8, rule.getDependsOn());
                    ps.setInt(9, rule.getEnabled());
                },
                JdbcExecutionOptions.builder().withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(targetUrl).withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(params.get("target-mysql.username")).withPassword(params.get("target-mysql.password")).build()
        ));
    }
}
