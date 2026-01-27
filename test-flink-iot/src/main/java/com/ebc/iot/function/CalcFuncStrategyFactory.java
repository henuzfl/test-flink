package com.ebc.iot.function;

import com.ebc.iot.function.strategy.ArithmeticCalcFunc;
import com.ebc.iot.function.strategy.CalcFuncStrategy;
import com.ebc.iot.function.strategy.*;
import com.ebc.iot.function.strategy.diff.DayDiffCalcFunc;
import com.ebc.iot.function.strategy.diff.MonthDiffCalcFunc;
import com.ebc.iot.function.strategy.diff.TotalDiffCalcFunc;
import com.ebc.iot.function.strategy.diff.YearDiffCalcFunc;
import com.ebc.iot.model.DevicePointRule;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 策略工厂
 */
public class CalcFuncStrategyFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, CalcFuncStrategy> strategies = new HashMap<>();

    /**
     * 在算子的 open 方法中调用，为当前子任务实例初始化专属策略
     */
    public void init(RuntimeContext ctx) {
        strategies.put("ARITHMETIC", new ArithmeticCalcFunc());
        strategies.put("TOTAL_DIFF", new TotalDiffCalcFunc());
        strategies.put("DAY_DIFF", new DayDiffCalcFunc());
        strategies.put("MONTH_DIFF", new MonthDiffCalcFunc());
        strategies.put("YEAR_DIFF", new YearDiffCalcFunc());

        for (CalcFuncStrategy s : strategies.values()) {
            s.open(ctx);
        }
    }

    /**
     * 根据规则返回对应的解析策略
     */
    public CalcFuncStrategy getStrategy(DevicePointRule rule) {
        if (rule.getExprType() == 0) {
            return strategies.get("ARITHMETIC");
        }
        return strategies.get(rule.getExpr());
    }
}
