package com.ebc.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * 对应 base_model_info 表的数据结构
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BaseModelInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer modelId;
    private Integer parentId;
    private Integer companyId;
    private String objectType;
    private String objectCode;
    private String objectName;
    private String deviceType;
    private Integer isCustom;
    private Integer isDisplay;
    private Integer isDataMonitor;
    private Integer isAlarm;
    private Integer isAnalysis;
    private Integer isFault;
    private Integer isCare;
    private Integer isLog;
    private String tabJson;
    private String hiddenCode;
    private Integer status;
    private String description;
    private Integer createUserId;
    private Date updateTime;
    private Date createTime;
}
