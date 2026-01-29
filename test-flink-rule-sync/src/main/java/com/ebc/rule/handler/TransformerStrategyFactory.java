package com.ebc.rule.handler;

import com.ebc.rule.handler.strategy.EnergyUseCalcNewTransformer;
import com.ebc.rule.handler.strategy.TransformerStrategy;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TransformerStrategyFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, TransformerStrategy> strategies = new HashMap<>();

    public void init() {
        strategies.put("energyUseCalcNew", new EnergyUseCalcNewTransformer());
    }

    public TransformerStrategy getStrategy(String transformerType) {
        return strategies.get(transformerType);
    }
}
