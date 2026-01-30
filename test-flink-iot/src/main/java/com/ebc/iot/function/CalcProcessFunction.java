package com.ebc.iot.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ebc.iot.function.strategy.CalculationContext;
import com.ebc.common.model.DevicePointRule;
import com.ebc.common.model.PointData;
import com.ebc.iot.function.strategy.CalcFuncStrategy;
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
    public void open(Configuration parameters) throws Exception {
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
        if (pointType != 2) {
            return;
        }
        DevicePointRule newRule = ("d".equals(op)) ? null : DevicePointRule.fromJsonNode(dataNode);
        DevicePointRule oldRule = (beforeNode.isMissingNode() || beforeNode.isNull()) ? null : DevicePointRule.fromJsonNode(beforeNode);

        BroadcastState<Tuple3<String, String, String>, Map<String, DevicePointRule>> ruleState = ctx.getBroadcastState(RULES_STATE_DESC);

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

        // 1. 更新本次快照中所有点位的状态
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


}
