package com.ebc.rule.handler.strategy;

import com.ebc.common.model.BusObjectInfo;
import com.ebc.common.model.FormulaDependency;
import com.ebc.common.model.FormulaResult;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;

import java.io.Serializable;
import java.util.List;

public interface TransformerStrategy extends Serializable {
    FormulaResult transform(BroadcastProcessFunction<?, ?, ?>.Context ctx, BusObjectInfo deviceInfo, String funcName, List<String> args, List<FormulaDependency> dependencies) throws Exception;
}
