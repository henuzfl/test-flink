package com.ebc.iot.function.strategy.diff;

import com.ebc.iot.function.strategy.CalcFuncStrategy;
import com.ebc.iot.function.strategy.CalculationContext;
import com.ebc.common.model.DevicePointRule;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;


public class TotalDiffCalcFunc implements CalcFuncStrategy {
    private static final long serialVersionUID = 1L;

    private transient MapState<String, Double> pointValueState;
    private transient MapState<String, Double> historyValueState;

    @Override
    public void open(RuntimeContext ctx) {
        pointValueState = ctx.getMapState(new MapStateDescriptor<>(
                "point-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));

        // 总差累计的初始值设置为永远不过期
        historyValueState = ctx.getMapState(new MapStateDescriptor<>(
                "history-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));
    }

    @Override
    public void calculate(DevicePointRule rule, CalculationContext ctx) throws Exception {
        Double cur = pointValueState.get(rule.getSourcePointCode());
        if (cur == null) return;

        String startKey = rule.getSourcePointCode() + "_total_start";
        Double start = historyValueState.get(startKey);
        
        if (start == null) {
            historyValueState.put(startKey, cur);
            start = cur;
        }

        ctx.emit(rule.getPointCode(), cur - start);
    }
}
