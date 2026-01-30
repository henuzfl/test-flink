package com.ebc.rule.handler;

import com.ebc.common.event.BusLakConfigEvent;
import com.ebc.common.model.BaseModelInfo;
import com.ebc.common.model.BusObjectInfo;
import com.ebc.common.model.BusObjectPointData;
import com.ebc.common.model.DevicePointRule;
import com.ebc.common.model.FormulaResult;
import com.ebc.common.utils.JsonMapperUtils;
import com.ebc.common.utils.LakPointFormulaUtils;
import com.ebc.rule.config.RuleSyncConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ebc.rule.function.LakRuleSyncFunction.MODELS_STATE;
import static com.ebc.rule.function.LakRuleSyncFunction.OBJECTS_STATE;
import static com.ebc.rule.function.LakRuleSyncFunction.POINTS_STATE;

/**
 * 专门处理 LAK 配置变更逻辑的处理器
 */
@Slf4j
public class LakConfigHandler implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = JsonMapperUtils.getSnakeCaseMapper();

    private final RuleSyncConfig config;

    private static final Pattern FUNC_PATTERN = Pattern.compile("^([a-zA-Z0-9_]+)\\s*\\((.*)\\)$");

    private TransformerStrategyFactory strategyFactory;

    public LakConfigHandler(RuleSyncConfig config) {
        this.config = config;
        strategyFactory = new TransformerStrategyFactory();
        strategyFactory.init();
    }

    /**
     * 处理配置变更事件
     */
    public void handle(BusLakConfigEvent event, BroadcastProcessFunction<?, ?, ?>.Context ctx, Collector<DevicePointRule> out) throws Exception {
        JsonNode data = MAPPER.readTree(event.getData());
        String op = event.getOp();
        String table = event.getTable();

        RuleSyncConfig.MysqlConfig src = config.getSourceMysql();

        if (src.getTablePoint().equals(table)) {
            // --- 点位变更 ---
            BusObjectPointData p = MAPPER.convertValue(data, BusObjectPointData.class);
            BroadcastState<Integer, BusObjectPointData> state = ctx.getBroadcastState(POINTS_STATE);

            if ("d".equals(op)) {
                // 删除时，尝试从状态中先拿到设备信息以便能正确定位目标记录
                BusObjectInfo info = ctx.getBroadcastState(OBJECTS_STATE).get(p.getObjectId());
                state.remove(p.getDataId());
                emitPointRule(p, info, 0, ctx, out);
            } else {
                state.put(p.getDataId(), p);
                BusObjectInfo info = ctx.getBroadcastState(OBJECTS_STATE).get(p.getObjectId());
                if (info != null) {
                    emitPointRule(p, info, info.getStatus(), ctx, out);
                }
            }
        } else if (src.getTableObject().equals(table)) {
            // --- 设备信息同步 ---
            BusObjectInfo info = MAPPER.convertValue(data, BusObjectInfo.class);
            BroadcastState<Integer, BusObjectInfo> state = ctx.getBroadcastState(OBJECTS_STATE);
            if ("d".equals(op)) state.remove(info.getObjectId());
            else state.put(info.getObjectId(), info);
        } else if (src.getTableModel().equals(table)) {
            // --- 模型信息同步 ---
            BaseModelInfo model = MAPPER.convertValue(data, BaseModelInfo.class);
            BroadcastState<Integer, BaseModelInfo> state = ctx.getBroadcastState(MODELS_STATE);
            if ("d".equals(op)) state.remove(model.getModelId());
            else state.put(model.getModelId(), model);
        }
    }

    /**
     * 核心规则构建与下发
     */
    private void emitPointRule(BusObjectPointData p, BusObjectInfo info, int enabled, BroadcastProcessFunction<?, ?, ?>.Context ctx, Collector<DevicePointRule> out) throws Exception {
        if (p == null) return;

        // 1. 解析公式
        FormulaResult fr = LakPointFormulaUtils.parse(p.getFormula(), p.getCompanyId());

        if (fr.getExprType() == 1) {
            Matcher matcher = FUNC_PATTERN.matcher(fr.getExpr());
            if (matcher.matches()) {
                String funcName = matcher.group(1);
                String argsStr = matcher.group(2);
                List<String> args = parseArguments(argsStr);

                fr = strategyFactory.getStrategy(funcName).transform(ctx, info, funcName, args, fr.getDependsOn());
            }
        }

        // 3. 将结构化依赖转为 JSON 字符串入库
        String dependsOnStr = "[]";
        try {
            dependsOnStr = MAPPER.writeValueAsString(fr.getDependsOn());
        } catch (Exception e) {
            log.error("Failed to serialize dependsOn", e);
        }

        DevicePointRule rule = DevicePointRule.builder()
                .companyId(String.valueOf(p.getCompanyId()))
                .deviceCode(info != null ? info.getObjectCode() : "unknown")
                .pointCode(p.getDataCode())
                .pointType(2)
                .valueType(parseDataType(p.getDataType()))
                .exprType(fr.getExprType())
                .expr(fr.getExpr())
                .dependsOn(dependsOnStr)
                .enabled(enabled)
                .build();

        out.collect(rule);
    }

    private int parseDataType(String dt) {
        try {
            return Integer.parseInt(dt);
        } catch (Exception e) {
            return 0;
        }
    }

    private List<String> parseArguments(String argsStr) {
        List<String> args = new ArrayList<>();
        if (argsStr == null || argsStr.trim().isEmpty()) {
            return args;
        }
        // 简单按逗号分割（暂不考虑嵌套括号或引号内逗号的极端情况）
        String[] split = argsStr.split(",");
        for (String s : split) {
            // 移除首尾空格及引号
            args.add(s.trim().replaceAll("^['\"]|['\"]$", ""));
        }
        return args;
    }
}
