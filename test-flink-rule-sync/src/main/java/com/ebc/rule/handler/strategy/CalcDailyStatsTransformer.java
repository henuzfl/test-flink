package com.ebc.rule.handler.strategy;

import com.ebc.common.model.BusObjectInfo;
import com.ebc.common.model.FormulaDependency;
import com.ebc.common.model.FormulaResult;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;

import java.util.List;

/**
 * 每日统计转换器 (calcDailyStats)
 * 格式: calcDailyStats('pointCode', 'max|min|avg')
 */
public class CalcDailyStatsTransformer implements TransformerStrategy {
    private static final long serialVersionUID = 1L;

    @Override
    public FormulaResult transform(BroadcastProcessFunction<?, ?, ?>.Context ctx, BusObjectInfo deviceInfo, Integer pointDataId, String funcName, List<String> args, List<FormulaDependency> dependencies) throws Exception {
        if (args == null || args.size() < 2) {
            return new FormulaResult();
        }

        String targetPointCode = args.get(0);
        String type = args.get(1).toLowerCase();

        FormulaDependency dependency = FormulaDependency.builder()
                .companyId(deviceInfo.getCompanyId())
                .deviceCode(deviceInfo.getObjectCode())
                .pointCode(targetPointCode)
                .var("a")
                .build();

        return FormulaResult.builder()
                .expr("DAILY_STATS")
                .exprType(1)
                .dependsOn(List.of(dependency))
                .params(List.of(
                        FormulaResult.FormulaParam.builder().name("type").value(type).build()
                ))
                .build();
    }
}
