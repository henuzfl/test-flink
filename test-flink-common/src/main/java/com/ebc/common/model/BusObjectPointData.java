package com.ebc.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * 对应 bus_object_point_data 表的数据结构
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BusObjectPointData implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer dataId;
    private Integer companyId;
    private Integer customerId;
    private Integer objectId;
    private Integer datasetId;
    private String dataCode;
    private String dataName;
    private String dataType;
    private String cDataType;
    private Integer dataSource; // 1 量测指标；3 计算指标
    private String objectValJson;
    private Integer isSet;
    private Integer isCache;
    private Integer readDirection;
    private Integer dataInterval;
    private String dataOcJson;
    private String dynamicJson;
    private Integer lowerControl;
    private String formula;
    private String unit;
    private Integer displayFormat;
    private Integer isPageDisplay;
    private Integer isStorageHis;
    private String defaultValue;
    private Integer isNot;
    private Integer isAbs;
    private String handleBefore;
    private String statisticsType;
    private String statisticsCycle;
    private String storageMethod;
    private String storageParams;
    private Integer storageInterval;
    private Integer sort;
    private Integer isPressure;
    private Double pressOffset;
    private Double pressOffsetRatio;
    private Double maxPressInterval;
    private Double minPressInterval;
    private String borderFormula;
    private Integer rangeTranIf;
    private Integer rangeTranType;
    private Float rangeTranMax;
    private Float rangeTranMin;
    private Float rangeTranCollectMax;
    private Float rangeTranCollectMin;
    private Integer borderIf;
    private Float borderMax;
    private Float borderMin;
    private Double uploadRate;
    private String uploadModel;
    private Integer isCustom;
    private String pointKey;
    private Integer isWarn;
    private Float warnMax;
    private Float warnMin;
    private Integer isError;
    private Float errorMax;
    private Float errorMin;
    private Integer booleanWarnSet;
    private Integer isBelong;
    private Integer createUserId;
    private Date updateTime;
    private Date createTime;
    private Integer pointType;
}
