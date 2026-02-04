package com.ebc.rule.handler.strategy;

/**
 * 计算下级设备点位平均值
 */
public class CalcAvgTransformer extends AbstractAggregationTransformer {
    private static final long serialVersionUID = 1L;

    @Override
    protected String buildExpression(int count) {
        StringBuilder expr = new StringBuilder();
        if (count > 1) {
            expr.append("(");
        }
        for (int i = 0; i < count; i++) {
            if (i > 0) expr.append(" + ");
            expr.append(getVarName(i));
        }
        if (count > 1) {
            expr.append(")");
        }
        expr.append(" / ").append(count);
        return expr.toString();
    }
}
