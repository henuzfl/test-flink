package com.ebc.rule.function;

import com.ebc.common.event.BusLakConfigEvent;
import com.ebc.common.model.BaseModelInfo;
import com.ebc.common.model.BusObjectInfo;
import com.ebc.common.model.BusObjectPointData;
import com.ebc.common.model.DevicePointRule;
import com.ebc.rule.config.RuleSyncConfig;
import com.ebc.common.utils.LakPointFormulaUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 核心逻辑：监听设备、模型、点位三表变化，在内存中 Join 并同步规则
 */
public class FullRuleSyncFunction extends BroadcastProcessFunction<String, BusLakConfigEvent, DevicePointRule> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);

    // 状态描述符定义
    public static final MapStateDescriptor<Integer, BusObjectPointData> POINTS_STATE =
            new MapStateDescriptor<>("points-state", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(BusObjectPointData.class));

    public static final MapStateDescriptor<Integer, BusObjectInfo> OBJECTS_STATE =
            new MapStateDescriptor<>("objects-state", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(BusObjectInfo.class));

    public static final MapStateDescriptor<Integer, BaseModelInfo> MODELS_STATE =
            new MapStateDescriptor<>("models-state", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(BaseModelInfo.class));

    private final RuleSyncConfig config;

    public FullRuleSyncFunction(RuleSyncConfig config) {
        this.config = config;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        // Config is passed via constructor, no need to load from global parameters
    }

    @Override
    public void processBroadcastElement(BusLakConfigEvent event, Context ctx, Collector<DevicePointRule> out) throws Exception {
        handleLakConfigChange(event, ctx, out);
    }

    private void handleLakConfigChange(BusLakConfigEvent event, Context ctx, Collector<DevicePointRule> out) throws Exception {
        JsonNode data = event.getData();
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
                emitPointRule(p, info, 0, out);
            } else {
                state.put(p.getDataId(), p);
                BusObjectInfo info = ctx.getBroadcastState(OBJECTS_STATE).get(p.getObjectId());
                if (info != null) {
                    emitPointRule(p, info, info.getStatus(), out);
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
    private void emitPointRule(BusObjectPointData p, BusObjectInfo info, int enabled, Collector<DevicePointRule> out) {
        if (p == null) return;

        // 解析公式
        LakPointFormulaUtils.FormulaResult fr = LakPointFormulaUtils.parse(p.getFormula(), p.getCompanyId());

        DevicePointRule rule = DevicePointRule.builder()
                .companyId(String.valueOf(p.getCompanyId()))
                .deviceCode(info != null ? info.getObjectCode() : "unknown") // 删除时 info 可能为 null，但 p 里有基本信息
                .pointCode(p.getDataCode())
                .pointType(2)
                .valueType(parseDataType(p.getDataType()))
                .exprType(fr.getExprType())
                .expr(fr.getExpr())
                .dependsOn(fr.getDependsOn())
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

    @Override
    public void processElement(String value, ReadOnlyContext ctx, Collector<DevicePointRule> out) {

    }
}
