package com.ebc.iot.function.strategy;

import com.ebc.common.model.DevicePointRule;
import org.apache.commons.jexl3.*;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

import java.util.Map;

public class ArithmeticCalcFunc implements CalcFuncStrategy {
    private static final long serialVersionUID = 1L;

    private transient MapState<String, Double> pointValueState;
    private transient JexlEngine jexl;

    @Override
    public void open(RuntimeContext ctx) {
        pointValueState = ctx.getMapState(new MapStateDescriptor<>(
                "point-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));
        
        // 初始化 JEXL 引擎
        jexl = new JexlBuilder().create();
    }

    @Override
    public void calculate(DevicePointRule rule, CalculationContext ctx) throws Exception {
        JexlContext context = new MapContext();
        boolean ready = true;

        for (Map.Entry<String, String> entry : rule.getVarMapping().entrySet()) {
            Double v = pointValueState.get(entry.getValue());
            if (v == null) {
                ready = false;
                break;
            }
            context.set(entry.getKey(), v);
        }

        if (ready) {
            try {
                JexlExpression expression = jexl.createExpression(rule.getExpr());
                Object result = expression.evaluate(context);
                
                if (result instanceof Number) {
                    double val = ((Number) result).doubleValue();
                    ctx.emit(rule.getPointCode(), val);
                    pointValueState.put(rule.getPointCode(), val);
                }
            } catch (Exception e) {
                // 如果表达式解析或执行失败，记录日志但不中断作业
            }
        }
    }
}
