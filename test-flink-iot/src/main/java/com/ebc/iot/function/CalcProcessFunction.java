package com.ebc.iot.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ebc.iot.function.strategy.CalculationContext;
import com.ebc.iot.model.DevicePointRule;
import com.ebc.iot.model.PointData;
import com.ebc.iot.function.strategy.CalcFuncStrategy;
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
public class CalcProcessFunction extends KeyedBroadcastProcessFunction<Tuple2<String, String>, Collection<PointData>, String, List<PointData>> {

    public static final MapStateDescriptor<Tuple2<String, String>, Map<String, DevicePointRule>> RULES_STATE_DESC =
            new MapStateDescriptor<>(
                    "rules-broadcast-state",
                    TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                    }),
                    TypeInformation.of(new TypeHint<Map<String, DevicePointRule>>() {
                    }));

    private transient MapState<String, Double> pointValueState; // 存储各点位的最新值（来自于上游 15 分钟窗口聚合后的全量快照）
    private transient CalcFuncStrategyFactory strategyFactory;

    @Override
    public void open(Configuration parameters) throws Exception {
        pointValueState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("point-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));

        // 为每个算子子任务创建并初始化私有的策略工厂
        strategyFactory = new CalcFuncStrategyFactory();
        strategyFactory.init(getRuntimeContext());
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<List<PointData>> out) throws Exception {
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

        Tuple2<String, String> ruleKey = new Tuple2<>(String.valueOf(companyId), deviceCode);
        BroadcastState<Tuple2<String, String>, Map<String, DevicePointRule>> ruleState = ctx.getBroadcastState(RULES_STATE_DESC);
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
    public void processElement(Collection<PointData> values, ReadOnlyContext ctx, Collector<List<PointData>> out) throws Exception {
        if (values == null || values.isEmpty()) return;

        // 获取最大的时间戳作为本次计算的时间戳
        long maxTs = values.stream()
                .map(PointData::getTimestamp)
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .max().orElse(System.currentTimeMillis());
        
        // 1. 同时更新本次快照中所有点位的状态
        for (PointData value : values) {
            pointValueState.put(value.getProperty_name(), value.getProperty_num_value());
        }

        // 2. 触发对应设备规则的计算（只触发一次！）
        Tuple2<String, String> key = ctx.getCurrentKey();
        Map<String, DevicePointRule> rulesMap = ctx.getBroadcastState(RULES_STATE_DESC).get(key);

        if (rulesMap != null) {
            // 从输入数据中提取一些公共字段，如 project_id, gateway_code
            PointData firstData = values.iterator().next();
            String projectId = firstData.getProject_id();
            String gatewayCode = firstData.getGateway_code();

            List<PointData> results = new ArrayList<>();

            for (DevicePointRule rule : rulesMap.values()) {
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
                                PointData p = new PointData();
                                p.setCompany_id(key.f0);
                                p.setDevice_id(key.f1);
                                p.setProject_id(projectId);
                                p.setProperty_name(ptCode);
                                p.setProperty_num_value(calcValue);
                                p.setProperty_value(calcValue != null ? String.valueOf(calcValue) : null);
                                p.setData_type("calc");
                                p.setGateway_code(gatewayCode);
                                p.setCreate_date(System.currentTimeMillis());
                                p.setData_date(maxTs);
                                p.setTimestamp(maxTs);
                                results.add(p);
                                
                                // 同时将计算出的衍生点位存回状态，以支持链式计算
                                try {
                                    pointValueState.put(ptCode, calcValue);
                                } catch (Exception ignored) {}
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
