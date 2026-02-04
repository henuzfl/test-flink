package com.ebc.rule.handler.strategy;

import com.ebc.common.model.BusObjectInfo;
import com.ebc.common.model.FormulaDependency;
import com.ebc.common.model.FormulaResult;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 将公式翻译为算术表达式
 * 场景：多点位间单一算 (CalcConstant)
 * 例如：calcConstant('PVDailyPowerGeneration*GenPrice')
 * 翻译为: 表达式: a * b, 依赖: a=PVDailyPowerGeneration, b=GenPrice (同设备)
 */
public class CalcConstantTransformer implements TransformerStrategy {
    private static final long serialVersionUID = 1L;

    // 匹配标识符（点位编码），排除数字开头的常量
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    @Override
    public FormulaResult transform(BroadcastProcessFunction<?, ?, ?>.Context ctx, BusObjectInfo deviceInfo, Integer pointDataId, String funcName, List<String> args, List<FormulaDependency> dependencies) throws Exception {
        if (args == null || args.isEmpty()) {
            return new FormulaResult();
        }

        String rawFormula = args.get(0);

        List<FormulaDependency> newDependencies = new ArrayList<>();
        Map<String, String> varMap = new HashMap<>();
        
        Matcher matcher = IDENTIFIER_PATTERN.matcher(rawFormula);
        StringBuilder sb = new StringBuilder();
        int lastEnd = 0;
        int count = 0;

        while (matcher.find()) {
            // 添加非匹配部分（操作符、空格、数字常量等）
            sb.append(rawFormula, lastEnd, matcher.start());
            
            String pointCode = matcher.group();
            
            // 检查是否已分配变量名
            String varName = varMap.get(pointCode);
            if (varName == null) {
                varName = getVarName(count++);
                varMap.put(pointCode, varName);
                
                // 建立同设备点的依赖
                newDependencies.add(FormulaDependency.builder()
                        .companyId(deviceInfo.getCompanyId())
                        .deviceCode(deviceInfo.getObjectCode())
                        .pointCode(pointCode)
                        .var(varName)
                        .build());
            }
            sb.append(varName);
            lastEnd = matcher.end();
        }
        sb.append(rawFormula.substring(lastEnd));

        return FormulaResult.builder()
                .expr(sb.toString())
                .dependsOn(newDependencies)
                .exprType(0) // 最终产出是算术表达式模式
                .build();
    }

    /**
     * 将索引转为变量名 (0->a, 1->b, ..., 25->z, 26->aa...)
     */
    private String getVarName(int index) {
        StringBuilder sb = new StringBuilder();
        int i = index;
        do {
            sb.insert(0, (char) ('a' + (i % 26)));
            i = (i / 26) - 1;
        } while (i >= 0);
        return sb.toString();
    }
}
