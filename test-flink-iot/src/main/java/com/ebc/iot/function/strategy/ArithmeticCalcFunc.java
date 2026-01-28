package com.ebc.iot.function.strategy;

import com.ebc.common.model.DevicePointRule;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

import java.util.HashMap;
import java.util.Map;

public class ArithmeticCalcFunc implements CalcFuncStrategy {
    private static final long serialVersionUID = 1L;

    private transient MapState<String, Double> pointValueState;

    @Override
    public void open(RuntimeContext ctx) {
        pointValueState = ctx.getMapState(new MapStateDescriptor<>(
                "point-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));
    }

    @Override
    public void calculate(DevicePointRule rule, CalculationContext ctx) throws Exception {
        Map<String, Double> context = new HashMap<>();
        boolean ready = true;

        for (Map.Entry<String, String> entry : rule.getVarMapping().entrySet()) {
            Double v = pointValueState.get(entry.getValue());
            if (v == null) {
                ready = false;
                break;
            }
            context.put(entry.getKey(), v);
        }

        if (ready) {
            Expression expression = new ExpressionBuilder(rule.getExpr())
                    .variables(context.keySet())
                    .build();
            context.forEach(expression::setVariable);
            double val = expression.evaluate();
            ctx.emit(rule.getPointCode(), val);
            pointValueState.put(rule.getPointCode(), val);
        }
    }
}
