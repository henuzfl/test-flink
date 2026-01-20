package com.flink.test.iot.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.test.iot.function.strategy.CalculationContext;
import com.flink.test.iot.model.DevicePointRule;
import com.flink.test.iot.model.PointData;
import com.flink.test.iot.function.strategy.CalcFuncStrategy;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

@Slf4j
public class CalcProcessFunction extends KeyedBroadcastProcessFunction<Tuple2<Integer, String>, PointData, String, PointData> {

    public static final MapStateDescriptor<Tuple2<Integer, String>, Map<String, DevicePointRule>> RULES_STATE_DESC =
            new MapStateDescriptor<>(
                    "rules-broadcast-state",
                    TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {
                    }),
                    TypeInformation.of(new TypeHint<Map<String, DevicePointRule>>() {
                    }));

    private transient MapState<Long, List<PointData>> windowBuckets; // 按分钟桶
    private transient MapState<String, Double> pointValueState; // 最新值
    private transient MapState<Long, Boolean> registeredTimers; // 避免重复注册 Timer
    private transient CalcFuncStrategyFactory strategyFactory;

    @Override
    public void open(Configuration parameters) throws Exception {
        windowBuckets = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("window-buckets", BasicTypeInfo.LONG_TYPE_INFO,
                        TypeInformation.of(new TypeHint<>() {
                        })));

        pointValueState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("point-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));

        registeredTimers = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("registered-timers", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO));

        // 为每个算子子任务创建并初始化私有的策略工厂
        strategyFactory = new CalcFuncStrategyFactory();
        strategyFactory.init(getRuntimeContext());
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<PointData> out) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode root = objectMapper.readTree(value);
        String op = root.path("op").asText();
        JsonNode dataNode = root.path("after");
        if (dataNode.isMissingNode() || dataNode.isNull()) return;
        int pointType = dataNode.path("point_type").asInt();
        if (pointType != 2) {
            return;
        }
        String deviceCode = dataNode.path("device_code").asText();
        String ptCode = dataNode.path("point_code").asText();
        int companyId = dataNode.path("company_id").asInt();
        int enabled = dataNode.path("enabled").asInt(1);

        Tuple2<Integer, String> ruleKey = new Tuple2<>(companyId, deviceCode);
        BroadcastState<Tuple2<Integer, String>, Map<String, DevicePointRule>> ruleState = ctx.getBroadcastState(RULES_STATE_DESC);
        Map<String, DevicePointRule> deviceRules = ruleState.get(ruleKey);
        if (deviceRules == null) deviceRules = new HashMap<>();

        if (enabled == 0 || "d".equals(op)) {
            deviceRules.remove(ptCode);
            log.info("Rule removed via CDC: company={}, device={}, point={}", companyId, deviceCode, ptCode);
        } else {
            DevicePointRule rule = DevicePointRule.fromJsonNode(dataNode);
            deviceRules.put(ptCode, rule);
            log.info("Rule updated via CDC: company={}, device={}, point={}, exprType={}",
                    companyId, deviceCode, ptCode, rule.getExprType());
        }

        if (deviceRules.isEmpty()) ruleState.remove(ruleKey);
        else ruleState.put(ruleKey, deviceRules);
    }

    @Override
    public void processElement(PointData value, ReadOnlyContext ctx, Collector<PointData> out) throws Exception {
        long currentProcTime = ctx.timerService().currentProcessingTime();
        long minuteTs = (currentProcTime / 60000) * 60000;

        List<PointData> bucket = windowBuckets.get(minuteTs);
        if (bucket == null) bucket = new ArrayList<>();
        bucket.add(value);
        windowBuckets.get(minuteTs); // Trigger access for some Flink versions
        windowBuckets.put(minuteTs, bucket);

        if (registeredTimers.get(minuteTs) == null) {
            long triggerTs = minuteTs + 60000;
            ctx.timerService().registerProcessingTimeTimer(triggerTs);
            registeredTimers.put(minuteTs, true);
            log.info("ProcessingTime: Registered timer for {} at bucket {}", triggerTs, minuteTs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PointData> out) throws Exception {
        long windowSize = 15 * 60_000L;
        long windowStart = timestamp - windowSize;
        long windowEnd = timestamp;

        Tuple2<Integer, String> key = ctx.getCurrentKey();
        Integer companyId = key.f0;
        String deviceCode = key.f1;

        long completedMinute = timestamp - 60000;
        List<PointData> freshData = windowBuckets.get(completedMinute);

        if (freshData != null) {
            for (PointData p : freshData)
                pointValueState.put(p.getPoint_code(), p.getValue());
        }

        Map<String, DevicePointRule> rulesMap = ctx.getBroadcastState(RULES_STATE_DESC).get(key);
        if (rulesMap != null) {
            for (DevicePointRule rule : rulesMap.values()) {
                try {
                    CalcFuncStrategy strategy = strategyFactory.getStrategy(rule);
                    if (strategy != null) {
                        strategy.calculate(rule, new CalculationContext() {
                            @Override
                            public long getTimestamp() {
                                return windowEnd;
                            }

                            @Override
                            public void emit(String ptCode, Double value) {
                                PointData p = new PointData();
                                p.setCompany_id(companyId);
                                p.setDevice_code(deviceCode);
                                p.setPoint_code(ptCode);
                                p.setValue(value);
                                p.setTs(windowEnd);
                                out.collect(p);
                            }
                        });
                    }
                } catch (Exception e) {
                    log.error("Rule eval failed for {}: {}", rule.getPointCode(), e.getMessage());
                }
            }
        }

        // 清理超时桶
        Iterator<Long> it = windowBuckets.keys().iterator();
        while (it.hasNext()) {
            Long tsKey = it.next();
            if (tsKey < windowStart) it.remove();
        }

        // 移除已触发 Timer
        registeredTimers.remove(completedMinute);
    }
}
