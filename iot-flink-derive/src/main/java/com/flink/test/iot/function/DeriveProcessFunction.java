package com.flink.test.iot.function;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.test.iot.model.PointData;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * 稳定触发 Timer 的 DeriveProcessFunction
 * EventTime 窗口：15 分钟滑动，每分钟触发一次
 */
public class DeriveProcessFunction extends KeyedBroadcastProcessFunction<Tuple2<Integer, String>, PointData, String, PointData> {

    private static final Logger logger = LoggerFactory.getLogger(DeriveProcessFunction.class);

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

    @Override
    public void open(Configuration parameters) {
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
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<PointData> out) throws Exception {
        JsonNode root = objectMapper.readTree(value);
        String op = root.path("op").asText();
        JsonNode dataNode = root.path("after");
        if (dataNode.isMissingNode() || dataNode.isNull()) return;

        String deviceCode = dataNode.path("device_code").asText();
        String ptCode = dataNode.path("point_code").asText();
        String expr = dataNode.path("expr").asText();
        int exprType = dataNode.path("expr_type").asInt();
        String dependsJson = dataNode.path("depends_on").asText();
        int enabled = dataNode.path("enabled").asInt(1);

        BroadcastState<String, Map<String, DeriveRule>> ruleState = ctx.getBroadcastState(RULES_STATE_DESC);
        Map<String, DeriveRule> deviceRules = ruleState.get(deviceCode);
        if (deviceRules == null) deviceRules = new HashMap<>();

        if (enabled == 0 || "d".equals(op)) {
            deviceRules.remove(ptCode);
            logger.info("Rule removed via CDC: device={}, point={}", deviceCode, ptCode);
        } else {
            DeriveRule rule = parseRule(ptCode, expr, exprType, dependsJson);
            deviceRules.put(ptCode, rule);
            logger.info("Rule updated via CDC: device={}, point={}, type={}", deviceCode, ptCode, exprType);
        }

        if (deviceRules.isEmpty()) ruleState.remove(deviceCode);
        else ruleState.put(deviceCode, deviceRules);
    }

    private DeriveRule parseRule(String ptCode, String expr, int exprType, String dependsJson) {
        List<String> deps = new ArrayList<>();
        Map<String, String> varMap = new HashMap<>();
        String sourcePoint = null;

        if (dependsJson != null && dependsJson.trim().startsWith("[")) {
            try {
                List<Map<String, Object>> list = objectMapper.readValue(dependsJson, new TypeReference<List<Map<String, Object>>>() {});
                for (Map<String, Object> item : list) {
                    String pCode = (String) item.get("point_code");
                    if (pCode != null) {
                        deps.add(pCode);
                        if (exprType == 0 && item.containsKey("var")) varMap.put((String) item.get("var"), pCode);
                        if (exprType == 1) sourcePoint = pCode;
                    }
                }
            } catch (Exception e) {
                logger.warn("Bad depends_on JSON: {}", dependsJson, e);
            }
        }
        return new DeriveRule(ptCode, expr, exprType, deps, varMap, sourcePoint);
    }

    @Override
    public void processElement(PointData value, ReadOnlyContext ctx, Collector<PointData> out) throws Exception {
        // 使用当前系统处理时间进行分桶
        long currentProcTime = ctx.timerService().currentProcessingTime();
        long minuteTs = (currentProcTime / 60000) * 60000;

        List<PointData> bucket = windowBuckets.get(minuteTs);
        if (bucket == null) bucket = new ArrayList<>();
        bucket.add(value);
        windowBuckets.put(minuteTs, bucket);

        // 注册 ProcessingTime Timer (下一分钟 00 秒触发)
        if (registeredTimers.get(minuteTs) == null) {
            long triggerTs = minuteTs + 60000;
            ctx.timerService().registerProcessingTimeTimer(triggerTs);
            registeredTimers.put(minuteTs, true);
            logger.info("ProcessingTime: Registered timer for {} at bucket {}", triggerTs, minuteTs);
        }
        
        logger.info("Data In: device={}, point={}, ts={}, value={}", 
             value.getDevice_code(), value.getPoint_code(), value.getTs(), value.getValue());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PointData> out) throws Exception {
        long windowSize = 15 * 60_000L;
        long windowStart = timestamp - windowSize;
        long windowEnd = timestamp;

        // 直接从 Tuple2 获取字段
        Tuple2<Integer, String> key = ctx.getCurrentKey();
        Integer companyId = key.f0;
        String deviceCode = key.f1;

        long completedMinute = timestamp - 60000;
        List<PointData> freshData = windowBuckets.get(completedMinute);
        
        logger.info("Timer triggered at {}, processing minute {}, data count={}", timestamp, completedMinute, (freshData == null ? 0 : freshData.size()));

        if (freshData != null) {
            for (PointData p : freshData) pointValueState.put(p.getPoint_code(), p.getValue());
        }

        Map<String, DeriveRule> rulesMap = ctx.getBroadcastState(RULES_STATE_DESC).get(deviceCode);
        if (rulesMap != null) {
            logger.info("Running {} rules for device {}", rulesMap.size(), deviceCode);
            for (DeriveRule rule : rulesMap.values()) {
                try {
                    if (rule.exprType == 0) calculateArithmetic(companyId, deviceCode, windowEnd, rule, out);
                    else if (rule.exprType == 1) calculateMonotonic(companyId, deviceCode, windowEnd, rule, out);
                } catch (Exception e) {
                    logger.warn("Rule eval failed: {}", rule.targetCode);
                }
            }
        }

        // 清理超时桶
        Iterator<Long> it = windowBuckets.keys().iterator();
        while (it.hasNext()) {
            Long tsKey = it.next();
            if (tsKey < windowStart) windowBuckets.remove(tsKey);
        }

        // 移除已触发 Timer
        registeredTimers.remove(completedMinute);
    }

    private void calculateArithmetic(Integer companyId, String deviceCode, long ts, DeriveRule rule, Collector<PointData> out) throws Exception {
        Map<String, Double> context = new HashMap<>();
        boolean ready = true;
        for (Map.Entry<String, String> entry : rule.varMapping.entrySet()) {
            Double v = pointValueState.get(entry.getValue());
            if (v == null) { ready = false; break; }
            context.put(entry.getKey(), v);
        }
        if (ready) {
            Expression expression = new ExpressionBuilder(rule.expression)
                    .variables(context.keySet())
                    .build();
            context.forEach(expression::setVariable);
            double val = expression.evaluate();
            emitDerived(companyId, deviceCode, rule.targetCode, val, ts, out);
            pointValueState.put(rule.targetCode, val);
        }
    }

    private void calculateMonotonic(Integer companyId, String deviceCode, long ts, DeriveRule rule, Collector<PointData> out) throws Exception {
        Double cur = pointValueState.get(rule.sourcePointCode);
        if (cur == null) return;

        double diff = 0;
        boolean emit = false;

        Instant instant = Instant.ofEpochMilli(ts);
        LocalDateTime dt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        if ("TOTAL_DIFF".equals(rule.expression)) {
            String startKey = companyId + "_" + deviceCode + "_" + rule.sourcePointCode + "_total_start";
            Double start = historyValueState.get(startKey);
            if (start == null) { historyValueState.put(startKey, cur); start = cur; }
            diff = cur - start; emit = true;
        } else if ("DAY_DIFF".equals(rule.expression)) {
            String dayStr = dt.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
            String startKey = companyId + "_" + deviceCode + "_" + rule.sourcePointCode + "_day_" + dayStr + "_start";
            Double start = historyValueState.get(startKey);
            if (start == null) { historyValueState.put(startKey, cur); start = cur; }
            diff = cur - start; emit = true;
        } else if ("MONTH_DIFF".equals(rule.expression)) {
            String monthStr = dt.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMM"));
            String startKey = companyId + "_" + deviceCode + "_" + rule.sourcePointCode + "_month_" + monthStr + "_start";
            Double start = historyValueState.get(startKey);
            if (start == null) { historyValueState.put(startKey, cur); start = cur; }
            diff = cur - start; emit = true;
        } else if ("YEAR_DIFF".equals(rule.expression)) {
            String yearStr = dt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy"));
            String startKey = companyId + "_" + deviceCode + "_" + rule.sourcePointCode + "_year_" + yearStr + "_start";
            Double start = historyValueState.get(startKey);
            if (start == null) { historyValueState.put(startKey, cur); start = cur; }
            diff = cur - start; emit = true;
        }

        if (emit) emitDerived(companyId, deviceCode, rule.targetCode, diff, ts, out);
    }

    private void emitDerived(Integer companyId, String deviceCode, String code, Double val, long ts, Collector<PointData> out) {
        PointData p = new PointData();
        p.setCompany_id(companyId);
        p.setDevice_code(deviceCode);
        p.setPoint_code(code);
        p.setValue(val);
        p.setTs(ts);
        out.collect(p);
    }

    public static class DeriveRule {
        public String targetCode;
        public String expression;
        public int exprType;
        public List<String> dependencies;
        public Map<String, String> varMapping;
        public String sourcePointCode;

        public DeriveRule() {}
        public DeriveRule(String targetCode, String expression, int exprType,
                          List<String> dependencies, Map<String, String> varMapping, String sourcePointCode) {
            this.targetCode = targetCode;
            this.expression = expression;
            this.exprType = exprType;
            this.dependencies = dependencies;
            this.varMapping = varMapping;
            this.sourcePointCode = sourcePointCode;
        }
    }
}
