package com.flink.test.iot.function;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.test.iot.model.PointData;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * Core Logic for Point Derivation
 * 1. Arithmetic Expressions
 * 2. Time-window based differences for monotonic increasing values
 */
public class DeriveProcessFunction extends ProcessWindowFunction<PointData, PointData, String, TimeWindow> {

    private static final Logger logger = LoggerFactory.getLogger(DeriveProcessFunction.class);

    // --- State ---
    // Latest values for all points: point_code -> value
    private transient MapState<String, Double> pointValueState;
    
    // Snapshots for monotonic calculations: metric_key -> value
    private transient MapState<String, Double> historyValueState;

    // Track current time window indices to detect switching
    private transient ValueState<Long> currentDayState;
    private transient ValueState<Long> currentMonthState;
    private transient ValueState<Long> currentYearState;

    // --- Dynamic Rules ---
    // Key: device_code -> List<Rule> (Optimized for fast lookup per device)
    // Access must be thread-safe if updated asynchronously
    private volatile Map<String, List<DeriveRule>> deviceRulesMap = new HashMap<>();

    // Timer for reloading
    private transient java.util.concurrent.ScheduledExecutorService scheduler;

    @Override
    public void open(Configuration parameters) throws Exception {
        // State Descriptors
        pointValueState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("point-values", String.class, Double.class));
        
        historyValueState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("history-values", String.class, Double.class));
        
        currentDayState = getRuntimeContext().getState(new ValueStateDescriptor<>("current-day", Long.class));
        currentMonthState = getRuntimeContext().getState(new ValueStateDescriptor<>("current-month", Long.class));
        currentYearState = getRuntimeContext().getState(new ValueStateDescriptor<>("current-year", Long.class));

        // Initial Load
        loadRulesFromMysql();

        // Schedule Periodic Refresh (e.g., every 1 minute)
        scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::loadRulesFromMysql, 1, 1, java.util.concurrent.TimeUnit.MINUTES);
    }

    @Override
    public void close() throws Exception {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }


    private void loadRulesFromMysql() {
        // Database Config
        String url = "jdbc:mysql://10.19.93.240:3306/zhny_rbac?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&serverTimezone=Asia/Shanghai";
        String user = "root";
        String pwd = "ih9PExr0RNojo20r%";
        
        Map<String, List<DeriveRule>> newRulesMap = new HashMap<>();

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, user, pwd);
             java.sql.Statement stmt = conn.createStatement()) {
            
            // Query enabled rules
            String sql = "SELECT device_code, point_code, expr, expr_type, depends_on FROM demo_flink.iot_point_def WHERE point_type = 2 AND enabled = 1";
            try (java.sql.ResultSet rs = stmt.executeQuery(sql)) {
                ObjectMapper mapper = new ObjectMapper();
                
                while (rs.next()) {
                    String devCode = rs.getString("device_code");
                    String ptCode = rs.getString("point_code");
                    String expr = rs.getString("expr");
                    int exprType = rs.getInt("expr_type");
                    String dependsJson = rs.getString("depends_on");

                    List<String> deps = new ArrayList<>();
                    Map<String, String> varMap = new HashMap<>();
                    String sourcePoint = null;

                    // Parse JSON: [{"var":"a", "point_code":"p1"}, ...]
                    if (dependsJson != null && dependsJson.trim().startsWith("[")) {
                         try {
                             List<Map<String, Object>> list = mapper.readValue(dependsJson, new TypeReference<List<Map<String, Object>>>(){});
                             for (Map<String, Object> item : list) {
                                 String pCode = (String) item.get("point_code");
                                 if (pCode != null) {
                                     deps.add(pCode);
                                     
                                     // For arithmetic: "var" -> "point_code"
                                     if (exprType == 0 && item.containsKey("var")) {
                                         varMap.put((String) item.get("var"), pCode);
                                     }
                                     
                                     // For monotonic: usually just one dependency
                                     if (exprType == 1) {
                                         sourcePoint = pCode;
                                     }
                                 }
                             }
                         } catch (Exception e) {
                             logger.error("Bad depends_on JSON for point_code: {}, error: {}", ptCode, e.getMessage());
                         }
                    }

                    DeriveRule rule = new DeriveRule(ptCode, expr, exprType, deps, varMap, sourcePoint);
                    newRulesMap.computeIfAbsent(devCode, k -> new ArrayList<>()).add(rule);
                }
            }
            
            this.deviceRulesMap = newRulesMap;
            int totalRules = newRulesMap.values().stream().mapToInt(List::size).sum();
            logger.info("Rules reloaded. Total devices with rules: {}, total rules: {}", 
                    newRulesMap.size(), totalRules);
            // Log rules details
            newRulesMap.forEach((deviceCode, rules) -> {
                logger.info("Device {} has {} rules:", deviceCode, rules.size());
                rules.forEach(rule -> {
                    logger.info("  - targetCode: {}, exprType: {}, expression: {}, dependencies: {}", 
                            rule.targetCode, rule.exprType, rule.expression, rule.dependencies);
                });
            });

        } catch (Exception e) {
            logger.error("Failed to load rules from MySQL", e);
        }
    }

    @Override
    public void process(String key, Context ctx, Iterable<PointData> elements, Collector<PointData> out) throws Exception {
        long windowEnd = ctx.window().getEnd();
        long slideSize = 60000L; // 1 minute
        
        // Metadata from first element (assuming all in same key share this)
        Integer companyId = null;
        String deviceCode = null;
        
        // 1. Update State with latest data from this slide
        for (PointData input : elements) {
            if (companyId == null) {
                companyId = input.getCompany_id();
                deviceCode = input.getDevice_code();
            }
            if (input.getTs() >= windowEnd - slideSize) {
                // Update latest value
                pointValueState.put(input.getPoint_code(), input.getValue());
            }
        }

        // If no data in window (unlikely given trigger), skip
        if (deviceCode == null) return;

        // 2. Evaluate All Rules Once per Window Trigger
        List<DeriveRule> rules = deviceRulesMap.get(deviceCode);
        if (rules != null) {
            long evalTs = windowEnd; // Use window end as the derivation time
            for (DeriveRule rule : rules) {
                try {
                    if (rule.exprType == 0) {
                        calculateArithmetic(companyId, deviceCode, evalTs, rule, out);
                    } else if (rule.exprType == 1) {
                        calculateMonotonic(companyId, deviceCode, evalTs, rule, out);
                    }
                } catch (Exception e) {
                    logger.error("Error evaluating rule for device: {}, rule: {}", deviceCode, rule.targetCode, e);
                }
            }
        }
    }

    private void calculateArithmetic(Integer companyId, String deviceCode, long ts, DeriveRule rule, Collector<PointData> out) throws Exception {
        Map<String, Double> context = new HashMap<>(); // Var -> Value
        boolean ready = true;
        
        for (Map.Entry<String, String> entry : rule.varMapping.entrySet()) {
            String varName = entry.getKey();
            String depPointCode = entry.getValue();
            
            Double v = pointValueState.get(depPointCode);
            if (v == null) {
                ready = false;
                break;
            }
            context.put(varName, v);
        }

        if (ready) {
            try {
                ExpressionBuilder builder = new ExpressionBuilder(rule.expression);
                for (String varName : context.keySet()) {
                    builder.variable(varName);
                }
                Expression expression = builder.build();
                for (Map.Entry<String, Double> entry : context.entrySet()) {
                    expression.setVariable(entry.getKey(), entry.getValue());
                }
                
                double derivedValue = expression.evaluate();

                emitDerived(companyId, deviceCode, rule.targetCode, derivedValue, ts, out);

                pointValueState.put(rule.targetCode, derivedValue);

            } catch (Exception e) {
                logger.warn("Failed to evaluate arithmetic expression for rule targetCode: {}, expression: {}", 
                        rule.targetCode, rule.expression);
            }
        }
    }

    private void calculateMonotonic(Integer companyId, String deviceCode, long ts, DeriveRule rule, Collector<PointData> out) throws Exception {
        // Monotonic calculations depend on a source point
        Double currentVal = pointValueState.get(rule.sourcePointCode);
        if (currentVal == null) {
            return; // No value for source yet
        }
        
        String func = rule.expression; 
        Instant instant = Instant.ofEpochMilli(ts);
        LocalDateTime dt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        double diff = 0.0;
        boolean emit = false;

        // Note: For monotonic logic to work in periodic mode, we must rely on the state 
        // retaining the 'start' value. The current logic checks specific day/month indices.
        // This is valid as long as 'ts' keeps advancing.

        if ("DAY_DIFF".equals(func)) {
            long dayIdx = dt.toLocalDate().toEpochDay();
            Long savedDay = currentDayState.value();
            String startKey = rule.sourcePointCode + "_day_start";
            
            if (savedDay == null || dayIdx > savedDay) {
                historyValueState.put(startKey, currentVal); // Reset start to current
                currentDayState.update(dayIdx);
            }
            Double start = historyValueState.get(startKey);
            if (start != null) {
                diff = currentVal - start;
                emit = true;
            }

        } else if ("MONTH_DIFF".equals(func)) {
            long monthIdx = dt.getYear() * 12 + dt.getMonthValue();
            Long savedMonth = currentMonthState.value();
            String startKey = rule.sourcePointCode + "_month_start";

            if (savedMonth == null || monthIdx > savedMonth) {
                historyValueState.put(startKey, currentVal);
                currentMonthState.update(monthIdx);
            }
            Double start = historyValueState.get(startKey);
            if (start != null) {
                diff = currentVal - start;
                emit = true;
            }

        } else if ("YEAR_DIFF".equals(func)) {
            long yearIdx = dt.getYear();
            Long savedYear = currentYearState.value();
            String startKey = rule.sourcePointCode + "_year_start";

            if (savedYear == null || yearIdx > savedYear) {
                historyValueState.put(startKey, currentVal);
                currentYearState.update(yearIdx);
            }
            Double start = historyValueState.get(startKey);
            if (start != null) {
                diff = currentVal - start;
                emit = true;
            }

        } else if ("TOTAL_DIFF".equals(func)) {
            String startKey = rule.sourcePointCode + "_total_start";
            Double start = historyValueState.get(startKey);
            if (start == null) {
                historyValueState.put(startKey, currentVal);
                start = currentVal;
            }
            diff = currentVal - start;
            emit = true;
        }

        if (emit) {
            emitDerived(companyId, deviceCode, rule.targetCode, diff, ts, out);
        }
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

    // Helper Class for Rule
    public static class DeriveRule {
        String targetCode;
        String expression;
        int exprType; // 0=Math, 1=Custom(Monotonic)
        List<String> dependencies;
        Map<String, String> varMapping; // varName -> pointCode
        String sourcePointCode;         // for monotonic

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
