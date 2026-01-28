package com.ebc.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * 对应 bus_object_info 表的数据结构
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BusObjectInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer objectId;
    private Integer moduleType;
    private Integer parentId;
    private Integer customerId;
    private Integer companyId;
    private Integer projectId;
    private String objectType;
    private String objectCode;
    private String objectName;
    private String deviceType;
    private String deviceUuid;
    private Integer modelId;
    private Integer boxDeviceId;
    private Integer driverId;
    private Integer isCustom;
    private Integer isDisplay;
    private Integer isDataMonitor;
    private Integer isAlarm;
    private Integer isAnalysis;
    private Integer isFault;
    private Integer isCare;
    private Integer isLog;
    private Integer isUnderSet;
    private String tabJson;
    private String hiddenCode;
    private Integer status;
    private String description;
    private Integer createUserId;
    private Date updateTime;
    private Date createTime;
    private String objectAlias;
    private Integer serialNo;
    private Integer assetsStatus;
    private Integer alarm;
    private Integer inverterTotal;
    private String deviceEnergyType;
    private String spacePathCode;
    private Integer spaceId;
    private Integer deviceEnergySources;
    private Integer scenarioType;
    private String userIds;
}
