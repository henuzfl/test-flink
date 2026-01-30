package com.ebc.rule.handler.strategy;

import com.ebc.common.model.BusObjectInfo;
import com.ebc.common.model.FormulaDependency;
import com.ebc.common.model.FormulaResult;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;

import java.util.List;

public class EnergyUseCalcNewTransformer implements TransformerStrategy {
    private static final long serialVersionUID = 1L;

    @Override
    public FormulaResult transform(BroadcastProcessFunction<?, ?, ?>.Context ctx, BusObjectInfo deviceInfo, Integer pointDataId, String funcName, List<String> args, List<FormulaDependency> dependencies) {
        String deviceCode = deviceInfo != null ? deviceInfo.getObjectCode() : "unknown";
        FormulaResult result = new FormulaResult();
        String type = args.get(1).toLowerCase();
        switch (type) {
            case "day":
                result.setExpr("DAY_DIFF");
                break;
            case "month":
                result.setExpr("MONTH_DIFF");
                break;
            case "year":
                result.setExpr("YEAR_DIFF");
                break;
            case "all":
                result.setExpr("TOTAL_DIFF");
                break;
            default:
                result.setExpr("DIFF_DAY");
                break;
        }
        String arg0 = args.get(0);
        FormulaDependency dependency = FormulaDependency.builder()
                .companyId(deviceInfo.getCompanyId())
                .deviceCode(deviceCode)
                .pointCode(arg0)
                .var("a")
                .build();
        result.setDependsOn(List.of(dependency));
        result.setExprType(1);
        return result;
    }
}
