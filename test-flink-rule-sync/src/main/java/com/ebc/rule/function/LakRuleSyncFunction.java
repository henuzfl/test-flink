package com.ebc.rule.function;

import com.ebc.common.event.BusLakConfigEvent;
import com.ebc.common.model.BaseModelInfo;
import com.ebc.common.model.BusObjectInfo;
import com.ebc.common.model.BusObjectPointData;
import com.ebc.common.model.DevicePointRule;
import com.ebc.rule.config.RuleSyncConfig;
import com.ebc.rule.handler.LakConfigHandler;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Set;

/**
 * 核心逻辑：监听设备、模型、点位三表变化，在内存中 Join 并同步规则
 * 逻辑已委托给 LakConfigHandler 处理
 */
public class LakRuleSyncFunction extends BroadcastProcessFunction<String, BusLakConfigEvent, DevicePointRule> {

    // 状态描述符定义 (必须在 Function 中公开，以便 Job 注册广播流)
    public static final MapStateDescriptor<Integer, BusObjectPointData> POINTS_STATE =
            new MapStateDescriptor<>("points-state", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(BusObjectPointData.class));

    public static final MapStateDescriptor<Integer, BusObjectInfo> OBJECTS_STATE =
            new MapStateDescriptor<>("objects-state", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(BusObjectInfo.class));

    public static final MapStateDescriptor<Integer, BaseModelInfo> MODELS_STATE =
            new MapStateDescriptor<>("models-state", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(BaseModelInfo.class));

    // 动态订阅状态：Key=parentId:modelCode, Value=Set<pointDataId>
    public static final MapStateDescriptor<String, Set<Integer>> DYNAMIC_WATCH_STATE =
            new MapStateDescriptor<>("dynamic-watch-state", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Set<Integer>>() {}));

    private final LakConfigHandler handler;

    public LakRuleSyncFunction(RuleSyncConfig config) {
        this.handler = new LakConfigHandler(config);
    }

    @Override
    public void processBroadcastElement(BusLakConfigEvent event, Context ctx, Collector<DevicePointRule> out) throws Exception {
        handler.handle(event, ctx, out);
    }

    @Override
    public void processElement(String value, ReadOnlyContext ctx, Collector<DevicePointRule> out) {
    }
}
