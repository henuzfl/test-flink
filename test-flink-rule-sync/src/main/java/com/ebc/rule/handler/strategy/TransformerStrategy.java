package com.ebc.rule.handler.strategy;

import com.ebc.common.model.FormulaDependency;
import com.ebc.common.model.FormulaResult;

import java.io.Serializable;
import java.util.List;

public interface TransformerStrategy extends Serializable {
    FormulaResult transform(int companyId, String deviceCode, String funcName, List<String> args, List<FormulaDependency> dependencies);
}
