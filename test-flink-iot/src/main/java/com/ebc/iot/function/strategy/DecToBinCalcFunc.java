package com.ebc.iot.function.strategy;

import com.ebc.common.model.DevicePointRule;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

import java.util.List;

/**
 * 十进制取位计算策略 (DEC_TO_BIN)
 * 逻辑：获取点位值，转为二进制，取出指定位的值
 */
public class DecToBinCalcFunc implements CalcFuncStrategy {
    private static final long serialVersionUID = 1L;

    private transient MapState<String, Double> pointValueState;

    @Override
    public void open(RuntimeContext ctx) {
        pointValueState = ctx.getMapState(new MapStateDescriptor<>(
                "point-values", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));
    }

    @Override
    public void calculate(DevicePointRule rule, CalculationContext ctx) throws Exception {
        // 1. 获取依赖点位编码
        String sourcePoint = rule.getSourcePointCode();
        if (sourcePoint == null) return;

        // 2. 获取当前点位值
        Double value = pointValueState.get(sourcePoint);
        if (value == null) return;

        // 3. 获取取位索引参数
        int bitIndex = 0;
        List<DevicePointRule.RuleParam> params = rule.getParamList();
        if (params != null) {
            for (DevicePointRule.RuleParam p : params) {
                if ("bit".equals(p.getName())) {
                    try {
                        bitIndex = Integer.parseInt(p.getValue());
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                    break;
                }
            }
        }

        // 4. 执行取位计算
        // 位数是从右往左数的，1代表第1位（二进制中的2^0位）
        // 如果 bitIndex == 1, 则取 value & 1
        // 如果 bitIndex == 2, 则取 (value >> 1) & 1
        if (bitIndex > 0) {
            long longVal = value.longValue();
            int bitResult = (int) ((longVal >> (bitIndex - 1)) & 1);
            
            double finalValue = (double) bitResult;
            ctx.emit(rule.getPointCode(), finalValue);
            pointValueState.put(rule.getPointCode(), finalValue);
        }
    }
}
