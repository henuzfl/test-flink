package com.ebc.rule.handler.strategy;

import com.ebc.common.model.BaseModelInfo;
import com.ebc.common.model.BusObjectInfo;
import com.ebc.common.model.FormulaDependency;
import com.ebc.common.model.FormulaResult;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.ebc.rule.function.LakRuleSyncFunction.MODELS_STATE;
import static com.ebc.rule.function.LakRuleSyncFunction.OBJECTS_STATE;

public class CalcSumTransformer implements TransformerStrategy {
    private static final long serialVersionUID = 1L;

    @Override
    public FormulaResult transform(BroadcastProcessFunction<?, ?, ?>.Context ctx, BusObjectInfo deviceInfo, String funcName, List<String> args, List<FormulaDependency> dependencies) throws Exception {
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
        // 2. 筛选出下级所有的为 targetModelId 的设备
        List<FormulaDependency> subDependencies = new ArrayList<>();
        int count = 1;
        for (Map.Entry<Integer, BusObjectInfo> entry : ctx.getBroadcastState(OBJECTS_STATE).immutableEntries()) {
            BusObjectInfo subInfo = entry.getValue();
            if (subInfo.getParentId() != null && subInfo.getParentId().equals(deviceInfo.getObjectId())
                    && subInfo.getModelId() != null && subInfo.getModelId().equals(targetModelId)) {

                subDependencies.add(FormulaDependency.builder()
                        .companyId(deviceInfo.getCompanyId())
                        .deviceCode(subInfo.getObjectCode())
                        .pointCode(targetPointCode)
                        .var("#" + count++)
                        .build());
            }
        }

        // 3. 构建公式和结果
        FormulaResult result = new FormulaResult();
        if (subDependencies.isEmpty()) {
            result.setExpr("0");
        } else {
            StringBuilder expr = new StringBuilder();
            for (int i = 1; i < count; i++) {
                if (i > 1) expr.append(" + ");
                expr.append("#").append(i);
            }
            result.setExpr(expr.toString());
        }
        result.setDependsOn(subDependencies);
        result.setExprType(1);
        return result;
    }
}
