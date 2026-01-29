package com.ebc.rule.handler.strategy;

import com.ebc.common.model.FormulaDependency;
import com.ebc.common.model.FormulaResult;

import java.util.List;

public class EnergyUseCalcNewTransformer implements TransformerStrategy {
    private static final long serialVersionUID = 1L;

    @Override
    public FormulaResult transform(int companyId, String deviceCode, String funcName, List<String> args, List<FormulaDependency> dependencies) {
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
                .companyId(companyId)
                .deviceCode(deviceCode)
                .pointCode(arg0)
                .var("#1")
                .build();
        result.setDependsOn(List.of(dependency));
        result.setExprType(1);
        return result;
    }
}
