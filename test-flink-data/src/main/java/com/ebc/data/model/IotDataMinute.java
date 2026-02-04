package com.ebc.data.model;

import lombok.Data;
import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class IotDataMinute implements Serializable {
    private Long dataId;
    private Timestamp dataDate;
    private String dataType;
    private String companyId;
    private String projectId;
    private String gatewayCode;
    private String protocolCode;
    private String productId;
    private String deviceId;
    private String propertyName;
    private String propertyValue;
    private Double propertyNumValue;
    private String messageId;
    private Long timestamp;
    private Timestamp createDate;
    private Integer year;
    private Integer month;
    private Integer day;
    private Integer hour;
}
