package com.ebc.iot.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * IoT Point Data POJO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PointData implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String company_id;
    private String device_id;
    private String project_id;
    private Double property_num_value;
    private String data_type;
    private String property_value;
    private Long create_date;
    private Long data_date;
    private String gateway_code;
    private String property_name;
    private Long timestamp;
}
