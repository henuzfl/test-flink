package com.ebc.iot.function.strategy;

import com.ebc.iot.model.DevicePointRule;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

public interface CalcFuncStrategy extends Serializable {
    /**
     * 初始化策略所需的状态
     *
     * @param ctx Flink 运行时上下文
     */
    void open(RuntimeContext ctx);

    /**
     * 执行推算逻辑
     *
     * @param rule 推算规则
     * @param ctx  计算上下文
     * @throws Exception 异常
     */
    void calculate(DevicePointRule rule, CalculationContext ctx) throws Exception;
}
