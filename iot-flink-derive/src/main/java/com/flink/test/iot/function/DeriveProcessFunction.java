package com.flink.test.iot.function;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.test.iot.model.DeriveRule;
import com.flink.test.iot.model.PointData;
import com.flink.test.iot.strategy.CalculationContext;
import com.flink.test.iot.strategy.DeriveStrategy;
import com.flink.test.iot.strategy.DeriveStrategyFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 稳定触发 Timer 的 DeriveProcessFunction
 * 使用策略模式处理不同的推算公式
 * 数据源对应 demo_flink.iot_point_def 表
 */
@Slf4j
public class DeriveProcessFunction extends KeyedBroadcastProcessFunction<Tuple2<Integer, String>, PointData, String, PointData> {

    public static final MapStateDescriptor<String, Map<String, DeriveRule>> RULES_STATE_DESC =
            new MapStateDescriptor<>(
                    "rules-broadcast-state",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Map<String, DeriveRule>>() {}));

    private transient MapState<Long, List<PointData>> windowBuckets; // 按分钟桶
    private transient MapState<String, Double> pointValueState; // 最新值
    private transient MapState<String, Double> historyValueState; // 单调累加初值
    private transient MapState<String, Long> indexState; // 单调索引
    private transient MapState<Long, Boolean> registeredTimers; // 避免重复注册 Timer
    private transient ObjectMapper objectMapper;
    private transient DeriveStrategyFactory strategyFactory;

    @Override
    public void open(Configuration parameters) throws Exception {
        windowBuckets = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("window-buckets", BasicTypeInfo.LONG_TYPE_INFO,
                        TypeInformation.of(new TypeHint<List<PointData>>() {})));

        pointValueState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("point-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));

        historyValueState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("history-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));

        indexState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("index-state", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO));

        registeredTimers = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("registered-timers", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO));

        objectMapper = new ObjectMapper();

        // 为每个算子子任务创建并初始化私有的策略工厂
        strategyFactory = new DeriveStrategyFactory();
        strategyFactory.init(getRuntimeContext());
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<PointData> out) throws Exception {
        JsonNode root = objectMapper.readTree(value);
        String op = root.path("op").asText();
        JsonNode dataNode = root.path("after");
        if (dataNode.isMissingNode() || dataNode.isNull()) return;

        // 根据表结构映射
        int pointType = dataNode.path("point_type").asInt();
        // 我们只关注派生点位 (point_type = 2)
        if (pointType != 2) {
            return;
        }

        String deviceCode = dataNode.path("device_code").asText();
        String ptCode = dataNode.path("point_code").asText();
        int companyId = dataNode.path("company_id").asInt();
        int valueType = dataNode.path("value_type").asInt();
        int exprType = dataNode.path("expr_type").asInt();
        String expr = dataNode.path("expr").asText();
        String dependsOn = dataNode.path("depends_on").asText();
        int enabled = dataNode.path("enabled").asInt(1);

        BroadcastState<String, Map<String, DeriveRule>> ruleState = ctx.getBroadcastState(RULES_STATE_DESC);
        Map<String, DeriveRule> deviceRules = ruleState.get(deviceCode);
        if (deviceRules == null) deviceRules = new HashMap<>();

        if (enabled == 0 || "d".equals(op)) {
            deviceRules.remove(ptCode);
            log.info("Rule removed via CDC: device={}, point={}", deviceCode, ptCode);
        } else {
            DeriveRule rule = parseRule(companyId, deviceCode, ptCode, pointType, valueType, exprType, expr, dependsOn, enabled);
            deviceRules.put(ptCode, rule);
            log.info("Rule updated via CDC: device={}, point={}, exprType={}", deviceCode, ptCode, exprType);
        }

        if (deviceRules.isEmpty()) ruleState.remove(deviceCode);
        else ruleState.put(deviceCode, deviceRules);
    }

    private DeriveRule parseRule(int companyId, String deviceCode, String ptCode, int pointType, int valueType, 
                                 int exprType, String expr, String dependsOn, int enabled) {
        List<DeriveRule.Dependency> depList = new ArrayList<>();
        Map<String, String> varMap = new HashMap<>();
        String sourcePoint = null;

        if (dependsOn != null && dependsOn.trim().startsWith("[")) {
            try {
                depList = objectMapper.readValue(dependsOn, new TypeReference<List<DeriveRule.Dependency>>() {});
                for (DeriveRule.Dependency dep : depList) {
                    String pCode = dep.getPoint_code();
                    if (pCode != null) {
                        if (exprType == 0 && dep.getVar() != null) {
                            varMap.put(dep.getVar(), pCode);
                        }
                        if (exprType == 1 && sourcePoint == null) {
                            sourcePoint = pCode;
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Bad depends_on JSON for {}: {}", ptCode, e.getMessage());
            }
        }
        
        return DeriveRule.builder()
                .companyId(companyId)
                .deviceCode(deviceCode)
                .pointCode(ptCode)
                .pointType(pointType)
                .valueType(valueType)
                .exprType(exprType)
                .expr(expr)
                .dependsOn(dependsOn)
                .enabled(enabled)
                .dependencyList(depList)
                .varMapping(varMap)
                .sourcePointCode(sourcePoint)
                .build();
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
            for (PointData p : freshData) pointValueState.put(p.getPoint_code(), p.getValue());
        }

        Map<String, DeriveRule> rulesMap = ctx.getBroadcastState(RULES_STATE_DESC).get(deviceCode);
        if (rulesMap != null) {
            for (DeriveRule rule : rulesMap.values()) {
                try {
                    DeriveStrategy strategy = strategyFactory.getStrategy(rule);
                    if (strategy != null) {
                        strategy.calculate(rule, new CalculationContext() {
                            @Override
                            public long getTimestamp() { return windowEnd; }

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
