package com.flink.test.iot.strategy;

import com.flink.test.iot.model.DeriveRule;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 策略工厂，不再使用全局静态实例，避免 Flink 并行度下的状态竞争问题
 */
public class DeriveStrategyFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, DeriveStrategy> strategies = new HashMap<>();

    /**
     * 在算子的 open 方法中调用，为当前子任务实例初始化专属策略
     */
    public void init(RuntimeContext ctx) throws Exception {
        strategies.put("ARITHMETIC", new ArithmeticStrategy());
        strategies.put("TOTAL_DIFF", new TotalDiffStrategy());
        strategies.put("DAY_DIFF", new DayDiffStrategy());
        strategies.put("MONTH_DIFF", new MonthDiffStrategy());
        strategies.put("YEAR_DIFF", new YearDiffStrategy());

        for (DeriveStrategy s : strategies.values()) {
            s.open(ctx);
        }
    }

    /**
     * 根据规则返回对应的解析策略
     */
    public DeriveStrategy getStrategy(DeriveRule rule) {
        if (rule.getExprType() == 0) {
            return strategies.get("ARITHMETIC");
        }
        return strategies.get(rule.getExpr());
    }
}
