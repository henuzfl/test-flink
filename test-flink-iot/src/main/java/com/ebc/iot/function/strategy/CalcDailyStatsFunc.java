package com.ebc.iot.function.strategy;

import com.ebc.common.model.DevicePointRule;
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
import java.util.List;

/**
 * 每日统计计算策略 (DAILY_STATS)
 * 支持: max, min, avg
 */
public class CalcDailyStatsFunc implements CalcFuncStrategy {
    private static final long serialVersionUID = 1L;

    private transient MapState<String, Double> pointValueState;
    private transient MapState<String, Double> historyValueState;

    @Override
    public void open(RuntimeContext ctx) {
        pointValueState = ctx.getMapState(new MapStateDescriptor<>(
                "point-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));

        // 设置 TTL 为 2 天，确保跨天数据能正确保留
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(2))
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

        // 获取统计类型 (max/min/avg)
        String type = "max";
        List<DevicePointRule.RuleParam> params = rule.getParamList();
        if (params != null) {
            for (DevicePointRule.RuleParam p : params) {
                if ("type".equals(p.getName())) {
                    type = p.getValue().toLowerCase();
                    break;
                }
            }
        }

        // 获取当前日期
        Instant instant = Instant.ofEpochMilli(ctx.getTimestamp());
        LocalDateTime dt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        String dayStr = dt.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        String baseKey = rule.getSourcePointCode() + "_day_" + dayStr;
        double result = 0;

        switch (type) {
            case "max":
                String maxKey = baseKey + "_max";
                Double oldMax = historyValueState.get(maxKey);
                result = (oldMax == null) ? cur : Math.max(oldMax, cur);
                historyValueState.put(maxKey, result);
                break;
            case "min":
                String minKey = baseKey + "_min";
                Double oldMin = historyValueState.get(minKey);
                result = (oldMin == null) ? cur : Math.min(oldMin, cur);
                historyValueState.put(minKey, result);
                break;
            case "avg":
                String sumKey = baseKey + "_sum";
                String countKey = baseKey + "_count";
                Double oldSum = historyValueState.get(sumKey);
                Double oldCount = historyValueState.get(countKey);
                
                double newSum = (oldSum == null) ? cur : oldSum + cur;
                double newCount = (oldCount == null) ? 1 : oldCount + 1;
                
                historyValueState.put(sumKey, newSum);
                historyValueState.put(countKey, newCount);
                result = newSum / newCount;
                break;
            default:
                return;
        }

        ctx.emit(rule.getPointCode(), result);
        pointValueState.put(rule.getPointCode(), result);
    }
}
