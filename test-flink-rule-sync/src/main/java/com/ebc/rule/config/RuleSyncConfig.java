package com.ebc.rule.config;

import org.apache.flink.api.java.utils.ParameterTool;
import lombok.Data;
import java.io.Serializable;

@Data
public class RuleSyncConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private FlinkConfig flink = new FlinkConfig();
    private MysqlConfig sourceMysql = new MysqlConfig();
    private MysqlConfig targetMysql = new MysqlConfig();

    @Data
    public static class FlinkConfig implements Serializable {
        private int parallelism = 1;
        private long checkpointInterval = 60000;
        private String checkpointMode = "EXACTLY_ONCE";
    }

    @Data
    public static class MysqlConfig implements Serializable {
        private String hostname;
        private int port;
        private String database;
        private String username;
        private String password;
        
        // Source specific
        private String tableObject;
        private String tableModel;
        private String tablePoint;
        
        // Target specific
        private String table;

        public String getJdbcUrl() {
            return String.format("jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=UTF-8",
                    hostname, port, database);
        }
    }

    public static RuleSyncConfig from(ParameterTool params) {
        RuleSyncConfig config = new RuleSyncConfig();
        
        // Flink
        config.flink.parallelism = params.getInt("flink.parallelism", 1);
        config.flink.checkpointInterval = params.getLong("flink.checkpoint.interval", 60000);
        config.flink.checkpointMode = params.get("flink.checkpoint.mode", "EXACTLY_ONCE");

        // Source Mysql
        config.sourceMysql.hostname = params.get("source-mysql.hostname", "localhost");
        config.sourceMysql.port = params.getInt("source-mysql.port", 3306);
        config.sourceMysql.database = params.get("source-mysql.database");
        config.sourceMysql.username = params.get("source-mysql.username");
        config.sourceMysql.password = params.get("source-mysql.password");
        config.sourceMysql.tableObject = params.get("source-mysql.table-object");
        config.sourceMysql.tableModel = params.get("source-mysql.table-model");
        config.sourceMysql.tablePoint = params.get("source-mysql.table-point");

        // Target Mysql
        config.targetMysql.hostname = params.get("target-mysql.hostname", "localhost");
        config.targetMysql.port = params.getInt("target-mysql.port", 3306);
        config.targetMysql.database = params.get("target-mysql.database");
        config.targetMysql.username = params.get("target-mysql.username");
        config.targetMysql.password = params.get("target-mysql.password");
        config.targetMysql.table = params.get("target-mysql.table");

        return config;
    }
}
