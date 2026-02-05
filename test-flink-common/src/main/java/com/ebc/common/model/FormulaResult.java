package com.ebc.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 公式解析结果
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FormulaResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private String expr;                    // 解析出的表达式
    private List<FormulaDependency> dependsOn; // 结构化的依赖列表
    
    @Builder.Default
    private int exprType = 0;               // 0: 算术表达式, 1: 函数/逻辑, 2: 常量值

    private List<FormulaParam> params;      // 函数参数列表

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FormulaParam implements Serializable {
        private String name;
        private String value;
    }
}
