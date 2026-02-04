package com.ebc.rule;

import com.ebc.common.config.ConfigLoader;
import com.ebc.common.event.BusLakConfigEvent;
import com.ebc.common.model.DevicePointRule;
import com.ebc.rule.config.RuleSyncConfig;
import com.ebc.rule.function.LakRuleSyncFunction;
import com.ebc.common.utils.JsonMapperUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import static com.ebc.rule.function.LakRuleSyncFunction.*;

/**
 * 规则多表同步 Job (DataStream 模式)
 * 监听三张表：bus_object_point_data, bus_object_info, base_model_info
 */
public class RuleSyncJob {

    private static final ObjectMapper MAPPER = JsonMapperUtils.getSnakeCaseMapper();

    public static void main(String[] args) throws Exception {
        ParameterTool params = ConfigLoader.loadConfig(args, "application.yml");
        RuleSyncConfig config = RuleSyncConfig.from(params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getFlink().getParallelism());
        env.getConfig().setGlobalJobParameters(params); // Keep for RichFunctions if needed, or pass config

        // 1. 定义源数据库 CDC Source (支持多表监听)
        RuleSyncConfig.MysqlConfig src = config.getSourceMysql();

        DataStream<BusLakConfigEvent> combinedStream = env.fromSource(
                MySqlSource.<String>builder()
                        .hostname(src.getHostname()).port(src.getPort()).username(src.getUsername()).password(src.getPassword())
                        .databaseList(src.getDatabase())
                        .tableList(src.getDatabase() + "." + src.getTableObject(), 
                                   src.getDatabase() + "." + src.getTableModel(), 
                                   src.getDatabase() + "." + src.getTablePoint())
                        .splitSize(40960) // 设置分片大小
                        .fetchSize(10000)
                        .serverTimeZone(params.get("mysql.timezone", "Asia/Shanghai"))
                        .deserializer(new JsonDebeziumDeserializationSchema()).build(),
                WatermarkStrategy.noWatermarks(), "Combined CDC Source"
        ).flatMap((String json, Collector<BusLakConfigEvent> out) -> {
            try {
                JsonNode node = MAPPER.readTree(json);
                String table = node.path("source").path("table").asText();
                String op = node.path("op").asText();
                JsonNode data = "d".equals(op) ? node.get("before") : node.get("after");
                if (data != null && !data.isNull()) {
                    if (src.getTablePoint().equals(table)) {
                        if (data.path("data_source").asInt() == 2) {
                            out.collect(new BusLakConfigEvent(table, op, data.toString()));
                        }
                    } else {
                        // 2. 其他表 (info, model) 全量同步
                        out.collect(new BusLakConfigEvent(table, op, data.toString()));
                    }
                }
            } catch (Exception e) {
                // 解析失败忽略或打日志
            }
        }, TypeInformation.of(BusLakConfigEvent.class)); // 显式提供类型信息获取器

        // 2. 连接广播流并进行处理
        DataStream<DevicePointRule> syncStream = env.fromElements("")
                .connect(combinedStream.broadcast(POINTS_STATE, OBJECTS_STATE, MODELS_STATE, DYNAMIC_WATCH_STATE))
                .process(new LakRuleSyncFunction(config));

        // 3. 写入目标数据库
        configureSink(syncStream, config);

        env.execute("Full Rule Sync Job");
    }

    private static void configureSink(DataStream<DevicePointRule> stream, RuleSyncConfig config) {
        RuleSyncConfig.MysqlConfig target = config.getTargetMysql();

        String upsertSql = String.format(
                "INSERT INTO %s (company_id, device_code, point_code, point_type, value_type, expr_type, expr, depends_on, enabled) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "point_type=VALUES(point_type), value_type=VALUES(value_type), expr_type=VALUES(expr_type), " +
                "expr=VALUES(expr), depends_on=VALUES(depends_on), enabled=VALUES(enabled)", target.getTable());

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
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(target.getJdbcUrl())
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(target.getUsername())
                        .withPassword(target.getPassword())
                        .build()
        ));
    }
}
