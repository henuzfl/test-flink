package com.flink.test.iot.function.strategy.diff;

import com.flink.test.iot.function.strategy.CalcFuncStrategy;
import com.flink.test.iot.function.strategy.CalculationContext;
import com.flink.test.iot.model.DevicePointRule;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class MonthDiffCalcFunc implements CalcFuncStrategy {
    private static final long serialVersionUID = 1L;

    private transient MapState<String, Double> pointValueState;
    private transient MapState<String, Double> historyValueState;

    @Override
    public void open(RuntimeContext ctx){
        pointValueState = ctx.getMapState(new MapStateDescriptor<>(
                "point-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));

        // 设置 TTL 为 35 天
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(35))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<String, Double> historyDesc = new MapStateDescriptor<>(
                "history-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO);
        historyDesc.enableTimeToLive(ttlConfig);
        
        historyValueState = ctx.getMapState(historyDesc);
    }

    @Override
    public void calculate(DevicePointRule rule, CalculationContext ctx) throws Exception {
        Double cur = pointValueState.get(rule.getSourcePointCode());
        if (cur == null) return;

        Instant instant = Instant.ofEpochMilli(ctx.getTimestamp());
        LocalDateTime dt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        String monthStr = dt.format(DateTimeFormatter.ofPattern("yyyyMM"));
        
        String startKey = rule.getSourcePointCode() + "_month_" + monthStr + "_start";
        Double start = historyValueState.get(startKey);
        
        if (start == null) {
            historyValueState.put(startKey, cur);
            start = cur;
        }

        ctx.emit(rule.getPointCode(), cur - start);
    }
}
