package com.flink.test.iot.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 对应数据库表 demo_flink.iot_point_def 的数据结构
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeriveRule implements Serializable {
    private static final long serialVersionUID = 1L;

    // 数据库原始字段
    private Integer companyId;      // company_id
    private String deviceCode;      // device_code
    private String pointCode;       // point_code (即原 targetCode)
    private Integer pointType;      // point_type: 1=原始点位，2=派生点位
    private Integer valueType;      // value_type: 0=浮点 1=整型 2=布尔 3=字符串
    private Integer exprType;       // expr_type: 0=算术表达式 1=自定义公式
    private String expr;            // expr (即原 expression)
    private String dependsOn;       // depends_on JSON 字符串
    private Integer enabled;        // enabled: 1=启用 0=禁用

    // 业务解析后的扩展字段 (由 processBroadcastElement 解析)
    private List<Dependency> dependencyList; // 完整的依赖对象列表
    private Map<String, String> varMapping;  // 变量名 -> 点位编码映射 (算术表达式用)
    private String sourcePointCode;         // 来源点位 (自定义公式用)

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
