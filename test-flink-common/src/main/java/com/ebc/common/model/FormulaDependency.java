package com.ebc.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 公式依赖关系明细
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FormulaDependency implements Serializable {
    private static final long serialVersionUID = 1L;

    private String var;         // 变量名，如 a, b 或 #1, #2
    private Integer companyId;  // 企业ID
    private String deviceCode;  // 设备编号
    private String pointCode;   // 点位编号
}
