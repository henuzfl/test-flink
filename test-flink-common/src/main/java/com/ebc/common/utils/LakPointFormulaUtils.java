package com.ebc.common.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 专门用于解析 bus_object_point_data 表中 formula JSON 字段的工具类
 */
@Slf4j
public class LakPointFormulaUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Data
    @Builder
    public static class FormulaResult implements Serializable {
        private String expr;      // 解析出来的表达式
        private String dependsOn; // 符号化的依赖关系 JSON (用于入库 iot_point_def)
        @Builder.Default
        private int exprType = 0; // 0: 算术表达式, 1: 自定义函数/逻辑
    }

    /**
     * 解析 Lak 系统的点位公式 JSON
     *
     * @param formulaJson 原始 JSON 字符串
     * @param companyId   企业 ID (用于构建依赖关系)
     * @return 解析后的结果对象
     */
    public static FormulaResult parse(String formulaJson, Integer companyId) {
        if (formulaJson == null || formulaJson.trim().isEmpty()) {
            return FormulaResult.builder().expr("").dependsOn("[]").build();
        }

        // 如果不是 JSON 格式，直接返回原始内容
        if (!formulaJson.trim().startsWith("{")) {
            return FormulaResult.builder().expr(formulaJson).dependsOn("[]").build();
        }

        try {
            JsonNode root = MAPPER.readTree(formulaJson);
            String rawFormula = root.path("formula").asText();
            JsonNode vars = root.get("vars");

            ArrayNode dependsOnArray = MAPPER.createArrayNode();
            boolean hasVarName = false;

            if (vars != null && vars.isArray()) {
                for (JsonNode v : vars) {
                    ObjectNode dep = MAPPER.createObjectNode();
                    String varName = v.path("name").asText();
                    String dataCode = v.path("data_code").asText();
                    String objectCode = v.path("object_code").asText();
                    
                    if (varName != null && !varName.isEmpty()) {
                        hasVarName = true;
                    }

                    dep.put("var", varName);
                    dep.put("company_id", companyId);
                    dep.put("device_code", objectCode);
                    dep.put("point_code", dataCode);
                    dependsOnArray.add(dep);
                }
            }

            // 业务判定逻辑：
            // 1. 如果 vars 中所有变量的 name 都为空，通常表示这是一个内置处理函数（如 DAY_DIFF）
            // 2. 如果包含变量名（如 a, b），则是普通的算术表达式
            int exprType = hasVarName ? 0 : 1;

            return FormulaResult.builder()
                    .expr(rawFormula)
                    .dependsOn(MAPPER.writeValueAsString(dependsOnArray))
                    .exprType(exprType)
                    .build();

        } catch (Exception e) {
            log.error("Failed to parse formula JSON: {}", formulaJson, e);
            // 解析失败降级：尝试使用原始字符串
            return FormulaResult.builder()
                    .expr(formulaJson)
                    .dependsOn("[]")
                    .exprType(0)
                    .build();
        }
    }
}
