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

/**
 * 抽象聚合转换器，封装了寻找下级设备并建立依赖的通用逻辑
 */
public abstract class AbstractAggregationTransformer implements TransformerStrategy {
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
        int count = 0;
        for (Map.Entry<Integer, BusObjectInfo> entry : ctx.getBroadcastState(OBJECTS_STATE).immutableEntries()) {
            BusObjectInfo subInfo = entry.getValue();
            if (subInfo.getParentId() != null && subInfo.getParentId().equals(deviceInfo.getObjectId())
                    && subInfo.getModelId() != null && subInfo.getModelId().equals(targetModelId)) {

                subDependencies.add(FormulaDependency.builder()
                        .companyId(deviceInfo.getCompanyId())
                        .deviceCode(subInfo.getObjectCode())
                        .pointCode(targetPointCode)
                        .var(getVarName(count++))
                        .build());
            }
        }

        // 3. 构建公式和结果
        FormulaResult result = new FormulaResult();
        result.setDependsOn(subDependencies);
        result.setExprType(0); // 算术表达式模式
        
        if (subDependencies.isEmpty()) {
            result.setExpr("0");
        } else {
            result.setExpr(buildExpression(count));
        }
        
        return result;
    }

    /**
     * 子类实现具体的表达式生成逻辑
     * @param count 依赖的点位数量
     * @return 表达式字符串
     */
    protected abstract String buildExpression(int count);

    /**
     * 将索引转为变量名 (0->a, 1->b, ..., 25->z, 26->aa...)
     */
    protected String getVarName(int index) {
        StringBuilder sb = new StringBuilder();
        while (index >= 0) {
            sb.insert(0, (char) ('a' + (index % 26)));
            index = (index / 26) - 1;
        }
        return sb.toString();
    }
}
