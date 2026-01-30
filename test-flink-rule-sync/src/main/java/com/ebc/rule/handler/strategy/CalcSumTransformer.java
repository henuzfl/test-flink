package com.ebc.rule.handler.strategy;

import com.ebc.common.model.BaseModelInfo;
import com.ebc.common.model.BusObjectInfo;
import com.ebc.common.model.FormulaDependency;
import com.ebc.common.model.FormulaResult;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;

import java.util.*;

import static com.ebc.rule.function.LakRuleSyncFunction.DYNAMIC_WATCH_STATE;
import static com.ebc.rule.function.LakRuleSyncFunction.MODELS_STATE;
import static com.ebc.rule.function.LakRuleSyncFunction.OBJECTS_STATE;

public class CalcSumTransformer implements TransformerStrategy {
    private static final long serialVersionUID = 1L;

    @Override
    public FormulaResult transform(BroadcastProcessFunction<?, ?, ?>.Context ctx, BusObjectInfo deviceInfo, Integer pointDataId, String funcName, List<String> args, List<FormulaDependency> dependencies) throws Exception {
        if (deviceInfo == null || args == null || args.size() < 2) {
            return new FormulaResult();
        }

        String targetModelCode = args.get(0);
        String targetPointCode = args.get(1);

        // 1. 获取目标模型的 ID
        Integer targetModelId = null;
        for (Map.Entry<Integer, BaseModelInfo> entry : ctx.getBroadcastState(MODELS_STATE).immutableEntries()) {
            if (deviceInfo.getCompanyId().equals(entry.getValue().getCompanyId()) && targetModelCode.equals(entry.getValue().getObjectCode())) {
                targetModelId = entry.getKey();
                break;
            }
        }
        if (targetModelId == null) {
            return new FormulaResult();
        }

        // --- 注册动态订阅 ---
        String watchKey = deviceInfo.getObjectId() + ":" + targetModelCode;
        Set<Integer> watchers = ctx.getBroadcastState(DYNAMIC_WATCH_STATE).get(watchKey);
        if (watchers == null) {
            watchers = new HashSet<>();
        }
        watchers.add(pointDataId);
        ctx.getBroadcastState(DYNAMIC_WATCH_STATE).put(watchKey, watchers);
        // 2. 筛选出下级所有的为 targetModelId 的设备
        List<FormulaDependency> subDependencies = new ArrayList<>();
        int index = 0;
        for (Map.Entry<Integer, BusObjectInfo> entry : ctx.getBroadcastState(OBJECTS_STATE).immutableEntries()) {
            BusObjectInfo subInfo = entry.getValue();
            if (subInfo.getParentId() != null && subInfo.getParentId().equals(deviceInfo.getObjectId())
                    && subInfo.getModelId() != null && subInfo.getModelId().equals(targetModelId)) {

                subDependencies.add(FormulaDependency.builder()
                        .companyId(deviceInfo.getCompanyId())
                        .deviceCode(subInfo.getObjectCode())
                        .pointCode(targetPointCode)
                        .var(getVarName(index++))
                        .build());
            }
        }

        // 3. 构建公式和结果
        FormulaResult result = new FormulaResult();
        if (subDependencies.isEmpty()) {
            result.setExpr("0");
        } else {
            StringBuilder expr = new StringBuilder();
            for (int i = 0; i < index; i++) {
                if (i > 0) expr.append(" + ");
                expr.append(getVarName(i));
            }
            result.setExpr(expr.toString());
        }
        result.setDependsOn(subDependencies);
        result.setExprType(0); // 0 表示算术表达式模式 (a + b)
        return result;
    }

    /**
     * 将索引转为变量名 (0->a, 1->b, ..., 25->z, 26->aa...)
     */
    private String getVarName(int index) {
        StringBuilder sb = new StringBuilder();
        while (index >= 0) {
            sb.insert(0, (char) ('a' + (index % 26)));
            index = (index / 26) - 1;
        }
        return sb.toString();
    }
}
