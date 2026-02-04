package com.ebc.iot.function;

import com.ebc.common.model.DevicePointRule;
import com.ebc.common.model.PointData;
import com.ebc.iot.function.strategy.CalcFuncStrategy;
import com.ebc.iot.function.strategy.CalculationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

@Slf4j
public class CalcProcessFunction extends KeyedBroadcastProcessFunction<Tuple2<String, String>, Collection<PointData>, String, List<PointData>> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final MapStateDescriptor<Tuple3<String, String, String>, Map<String, DevicePointRule>> RULES_STATE_DESC =
            new MapStateDescriptor<>(
                    "rules-broadcast-state",
                    TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {
                    }),
                    TypeInformation.of(new TypeHint<Map<String, DevicePointRule>>() {
                    }));

    private transient MapState<String, Double> pointValueState; // 存储各点位的最新值
    private transient CalcFuncStrategyFactory strategyFactory;

    @Override
    public void open(Configuration parameters) {
        pointValueState = getRuntimeContext()
                .getMapState(
                        new MapStateDescriptor<>("point-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));
        strategyFactory = new CalcFuncStrategyFactory();
        strategyFactory.init(getRuntimeContext());
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<List<PointData>> out) throws Exception {
        JsonNode root = MAPPER.readTree(value);
        String op = root.path("op").asText();
        JsonNode dataNode = "d".equals(op) ? root.path("before") : root.path("after");
        JsonNode beforeNode = root.path("before");
        if (dataNode.isMissingNode() || dataNode.isNull()) {
            return;
        }
        int pointType = dataNode.path("point_type").asInt();
        int dataSource = dataNode.path("data_source").asInt();

        DevicePointRule newRule = ("d".equals(op)) ? null : DevicePointRule.fromJsonNode(dataNode);
        DevicePointRule oldRule = (beforeNode.isMissingNode() || beforeNode.isNull()) ? null : DevicePointRule.fromJsonNode(beforeNode);

        BroadcastState<Tuple3<String, String, String>, Map<String, DevicePointRule>> ruleState = ctx.getBroadcastState(RULES_STATE_DESC);

        // 0. 处理常量点位定义 (使用特定的 TriggerPoint 为 "" 存储在 rules-broadcast-state 中)
        if (oldRule != null && oldRule.getExprType() != null && oldRule.getExprType() == 2) {
            Tuple3<String, String, String> defKey = new Tuple3<>(oldRule.getCompanyId(), oldRule.getDeviceCode(), "");
            Map<String, DevicePointRule> map = ruleState.get(defKey);
            if (map != null) {
                map.remove(oldRule.getPointCode());
                if (map.isEmpty()) ruleState.remove(defKey);
                else ruleState.put(defKey, map);
            }
        }

        if (newRule != null && newRule.getExprType() != null && newRule.getExprType() == 2) {
            // 存入广播状态，用于处理“冷启动”设备
            Tuple3<String, String, String> defKey = new Tuple3<>(newRule.getCompanyId(), newRule.getDeviceCode(), "");
            Map<String, DevicePointRule> map = ruleState.get(defKey);
            if (map == null) map = new HashMap<>();
            map.put(newRule.getPointCode(), newRule);
            ruleState.put(defKey, map);

            // 直接推送到当前所有活跃设备的 pointValueState
            Double val = parseConstantValue(newRule.getExpr());
            if (val != null) {
                final String ptCode = newRule.getPointCode();
                final Double finalVal = val;
                ctx.applyToKeyedState(new MapStateDescriptor<>("point-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO),
                    (k, s) -> {
                        if (k.f0.equals(newRule.getCompanyId()) && k.f1.equals(newRule.getDeviceCode())) {
                            s.put(ptCode, finalVal);
                        }
                    });
            }
        }

        if (pointType != 2) {
            return;
        }

        // 1. 清理旧依赖的索引
        if (oldRule != null) {
            for (DevicePointRule.Dependency dep : oldRule.getDependencyList()) {
                Tuple3<String, String, String> key = new Tuple3<>(oldRule.getCompanyId(), dep.getDevice_code(), dep.getPoint_code());
                Map<String, DevicePointRule> map = ruleState.get(key);
                if (map != null) {
                    map.remove(oldRule.getPointCode());
                    if (map.isEmpty()) {
                        ruleState.remove(key);
                    } else {
                        ruleState.put(key, map);
                    }
                }
            }
        }

        // 2. 建立新依赖的索引
        if (newRule != null && newRule.getEnabled() != 0 && !"d".equals(op)) {
            for (DevicePointRule.Dependency dep : newRule.getDependencyList()) {
                // 注意：这里使用的是依赖关系中的点位作为 Key 的一部分
                Tuple3<String, String, String> key = new Tuple3<>(newRule.getCompanyId(), dep.getDevice_code(), dep.getPoint_code());
                Map<String, DevicePointRule> map = ruleState.get(key);
                if (map == null) {
                    map = new HashMap<>();
                }
                map.put(newRule.getPointCode(), newRule);
                ruleState.put(key, map);
            }
            log.info("Rule updated by dependencies: company={}, device={}, resultPoint={}, deps={}",
                    newRule.getCompanyId(), newRule.getDeviceCode(), newRule.getPointCode(), newRule.getDependencyList().size());
        }
    }

    @Override
    public void processElement(Collection<PointData> values, ReadOnlyContext ctx, Collector<List<PointData>> out) throws Exception {
        if (values == null || values.isEmpty()) return;

        long maxTs = values.stream()
                .map(PointData::getTimestamp)
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .max().orElse(System.currentTimeMillis());

        // 1. 同步常量到 pointValueState (处理新设备的冷启动)
        Tuple2<String, String> currentKey = ctx.getCurrentKey();
        Tuple3<String, String, String> defKey = new Tuple3<>(currentKey.f0, currentKey.f1, "");
        Map<String, DevicePointRule> constants = ctx.getBroadcastState(RULES_STATE_DESC).get(defKey);
        if (constants != null) {
            for (DevicePointRule rule : constants.values()) {
                if (!pointValueState.contains(rule.getPointCode())) {
                    Double val = parseConstantValue(rule.getExpr());
                    if (val != null) pointValueState.put(rule.getPointCode(), val);
                }
            }
        }

        // 2. 更新本次快照中所有点位的状态 (实时值优先级更高)
        for (PointData value : values) {
            pointValueState.put(value.getProperty_name(), value.getProperty_num_value());
        }

        // 2. 根据本次到达的点位，精准匹配受影响的规则
        Map<String, DevicePointRule> triggeredRules = new HashMap<>();
        for (PointData p : values) {
            Tuple3<String, String, String> triggerKey = new Tuple3<>(p.getCompany_id(), p.getDevice_id(), p.getProperty_name());
            Map<String, DevicePointRule> rules = ctx.getBroadcastState(RULES_STATE_DESC).get(triggerKey);
            if (rules != null) {
                triggeredRules.putAll(rules);
            }
        }

        if (!triggeredRules.isEmpty()) {
            PointData firstData = values.iterator().next();
            String projectId = firstData.getProject_id();
            String gatewayCode = firstData.getGateway_code();

            List<PointData> results = new ArrayList<>();

            for (DevicePointRule rule : triggeredRules.values()) {
                try {
                    CalcFuncStrategy strategy = strategyFactory.getStrategy(rule);
                    if (strategy != null) {
                        strategy.calculate(rule, new CalculationContext() {
                            @Override
                            public long getTimestamp() {
                                return maxTs;
                            }

                            @Override
                            public void emit(String ptCode, Double calcValue) {
                                PointData p = PointData.builder()
                                        .company_id(rule.getCompanyId())
                                        .device_id(rule.getDeviceCode())
                                        .project_id(projectId)
                                        .property_name(ptCode)
                                        .property_num_value(calcValue)
                                        .property_value(calcValue != null ? String.valueOf(calcValue) : null)
                                        .data_type("calc")
                                        .gateway_code(gatewayCode)
                                        .create_date(System.currentTimeMillis())
                                        .data_date(maxTs)
                                        .timestamp(maxTs)
                                        .build();
                                results.add(p);

                                // 同时将计算出的衍生点位存回状态，以支持链式计算
                                try {
                                    pointValueState.put(ptCode, calcValue);
                                } catch (Exception ignored) {
                                }
                            }
                        });
                    }
                } catch (Exception e) {
                    log.error("Rule eval failed for {}: {}", rule.getPointCode(), e.getMessage());
                }
            }

            if (!results.isEmpty()) {
                out.collect(results);
            }
        }
    }

    private Double parseConstantValue(String expr) {
        if (expr == null || expr.isEmpty()) return null;
        try {
            if (expr.startsWith("{")) {
                JsonNode exprNode = MAPPER.readTree(expr);
                if (exprNode.has("formula")) {
                    return exprNode.path("formula").asDouble();
                }
            } else {
                return Double.parseDouble(expr);
            }
        } catch (Exception e) {
            log.warn("Failed to parse constant from expr: {}, error: {}", expr, e.getMessage());
        }
        return null;
    }


}
