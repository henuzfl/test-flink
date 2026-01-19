package com.flink.test.iot.strategy;

/**
 * 推算任务的上下文环境，封装了元数据和输出能力
 */
public interface CalculationContext {
    /**
     * 获取当前计算的时间戳
     */
    long getTimestamp();

    /**
     * 发送推算出的点位数据
     * 
     * @param ptCode 点位编码
     * @param value 点位值
     */
    void emit(String ptCode, Double value);
}
