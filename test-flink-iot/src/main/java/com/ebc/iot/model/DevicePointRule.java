package com.ebc.iot.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对应数据库表 demo_flink.iot_point_def 的数据结构
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class DevicePointRule implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // 数据库原始字段
    private Integer companyId;      // company_id
    private String deviceCode;      // device_code
    private String pointCode;       // point_code
    private Integer pointType;      // point_type: 1=原始点位，2=派生点位
    private Integer valueType;      // value_type: 0=浮点 1=整型 2=布尔 3=字符串
    private Integer exprType;       // expr_type: 0=算术表达式 1=自定义公式
    private String expr;            // expr
    private String dependsOn;       // depends_on JSON 字符串
    private Integer enabled;        // enabled: 1=启用 0=禁用

    // 业务解析后的扩展字段
    private List<Dependency> dependencyList; // 完整的依赖对象列表
    private Map<String, String> varMapping;  // 变量名 -> 点位编码映射
    private String sourcePointCode;         // 来源点位

    /**
     * 从 Debezium JSON 的 after 节点解析规则 (静态工厂方法)
     */
    public static DevicePointRule fromJsonNode(JsonNode dataNode) {
        String deviceCode = dataNode.path("device_code").asText();
        String ptCode = dataNode.path("point_code").asText();
        int companyId = dataNode.path("company_id").asInt();
        int pointType = dataNode.path("point_type").asInt();
        int valueType = dataNode.path("value_type").asInt();
        int exprType = dataNode.path("expr_type").asInt();
        String expr = dataNode.path("expr").asText();
        String dependsOn = dataNode.path("depends_on").asText();
        int enabled = dataNode.path("enabled").asInt(1);

        List<Dependency> depList = new ArrayList<>();
        Map<String, String> varMap = new HashMap<>();
        String sourcePoint = null;

        if (dependsOn != null && !dependsOn.trim().isEmpty() && dependsOn.trim().startsWith("[")) {
            try {
                depList = MAPPER.readValue(dependsOn, new TypeReference<List<Dependency>>() {
                });
                for (Dependency dep : depList) {
                    String pCode = dep.getPoint_code();
                    if (pCode != null) {
                        if (exprType == 0 && dep.getVar() != null) {
                            varMap.put(dep.getVar(), pCode);
                        }
                        if (exprType == 1 && sourcePoint == null) {
                            sourcePoint = pCode;
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Bad depends_on JSON for {}: {}", ptCode, e.getMessage());
            }
        }

        return DevicePointRule.builder()
                .companyId(companyId)
                .deviceCode(deviceCode)
                .pointCode(ptCode)
                .pointType(pointType)
                .valueType(valueType)
                .exprType(exprType)
                .expr(expr)
                .dependsOn(dependsOn)
                .enabled(enabled)
                .dependencyList(depList)
                .varMapping(varMap)
                .sourcePointCode(sourcePoint)
                .build();
    }

    /**
     * 内部依赖类，对应 JSON 数组中的元素
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Dependency implements Serializable {
        private String var;
        private Integer company_id;
        private String device_code;
        private String point_code;
    }
}
