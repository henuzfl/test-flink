package com.ebc.common.utils;

import com.ebc.common.model.FormulaDependency;
import com.ebc.common.model.FormulaResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 专门用于解析 bus_object_point_data 表中 formula JSON 字段的工具类
 */
@Slf4j
public class LakPointFormulaUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 解析 Lak 系统的点位公式 JSON
     *
     * @param formulaJson 原始 JSON 字符串
     * @param companyId   企业 ID (用于构建依赖关系)
     * @return 解析后的结果对象
     */
    public static FormulaResult parse(String formulaJson, Integer companyId) {
        if (formulaJson == null || formulaJson.trim().isEmpty()) {
            return FormulaResult.builder().expr("").dependsOn(Collections.emptyList()).build();
        }

        // 如果不是 JSON 格式，直接返回原始内容
        if (!formulaJson.trim().startsWith("{")) {
            return FormulaResult.builder().expr(formulaJson).dependsOn(Collections.emptyList()).build();
        }

        try {
            JsonNode root = MAPPER.readTree(formulaJson);
            String rawFormula = root.path("formula").asText();
            JsonNode vars = root.get("vars");

            List<FormulaDependency> dependencies = new ArrayList<>();
            boolean hasVarName = false;

            if (vars != null && vars.isArray()) {
                for (JsonNode v : vars) {
                    String varName = v.path("name").asText();
                    String dataCode = v.path("data_code").asText();
                    String objectCode = v.path("object_code").asText();
                    
                    if (varName != null && !varName.isEmpty()) {
                        hasVarName = true;
                    }

                    dependencies.add(FormulaDependency.builder()
                            .var(varName)
                            .companyId(companyId)
                            .deviceCode(objectCode)
                            .pointCode(dataCode)
                            .build());
                }
            }

            // 业务判定逻辑：
            // 1. 如果 vars 中所有变量的 name 都为空，通常表示这是一个内置处理函数（如 DAY_DIFF）
            // 2. 如果包含变量名（如 a, b），则是普通的算术表达式
            int exprType = hasVarName ? 0 : 1;

            return FormulaResult.builder()
                    .expr(rawFormula)
                    .dependsOn(dependencies)
                    .exprType(exprType)
                    .build();

        } catch (Exception e) {
            log.error("Failed to parse formula JSON: {}", formulaJson, e);
            // 解析失败降级：尝试使用原始字符串
            return FormulaResult.builder()
                    .expr(formulaJson)
                    .dependsOn(Collections.emptyList())
                    .exprType(0)
                    .build();
        }
    }
}
