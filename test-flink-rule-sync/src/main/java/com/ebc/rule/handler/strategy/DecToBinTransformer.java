package com.ebc.rule.handler.strategy;

import com.ebc.common.model.BusObjectInfo;
import com.ebc.common.model.FormulaDependency;
import com.ebc.common.model.FormulaResult;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;

import java.util.List;

/**
 * 取位公式转换器 (decToBin)
 * 场景：decToBin('IoStauts', 1)
 * 第一个参数为被转换点位，第二个参数为取位（从右往左数，1代表第0位，或1代表第1位？通常取位1是指第1位）
 * 按照用户描述：“位效是从右往左数的”，通常 1 代表最右边那一比特。
 */
public class DecToBinTransformer implements TransformerStrategy {
    private static final long serialVersionUID = 1L;

    @Override
    public FormulaResult transform(BroadcastProcessFunction<?, ?, ?>.Context ctx, BusObjectInfo deviceInfo, Integer pointDataId, String funcName, List<String> args, List<FormulaDependency> dependencies) throws Exception {
        if (args == null || args.size() < 2) {
            return new FormulaResult();
        }

        String pointCode = args.get(0);
        String bitIndex = args.get(1);

        FormulaDependency dependency = FormulaDependency.builder()
                .companyId(deviceInfo.getCompanyId())
                .deviceCode(deviceInfo.getObjectCode())
                .pointCode(pointCode)
                .var("a")
                .build();

        return FormulaResult.builder()
                .expr("DEC_TO_BIN")
                .exprType(1)
                .dependsOn(List.of(dependency))
                .params(List.of(
                        FormulaResult.FormulaParam.builder().name("bit").value(bitIndex).build()
                ))
                .build();
    }
}
