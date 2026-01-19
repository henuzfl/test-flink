package com.flink.test.iot.strategy;

import com.flink.test.iot.model.DeriveRule;
import com.flink.test.iot.model.PointData;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public interface DeriveStrategy extends Serializable {
    /**
     * 初始化策略所需的状态
     * 
     * @param ctx Flink 运行时上下文
     * @throws Exception 异常
     */
    void open(RuntimeContext ctx) throws Exception;

    /**
     * 执行推算逻辑
     * 
     * @param rule 推算规则
     * @param ctx 计算上下文
     * @throws Exception 异常
     */
    void calculate(DeriveRule rule, CalculationContext ctx) throws Exception;
}
