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
    
    private Integer company_id;
    private String device_code;
    private String point_code;
    private Double value;
    private Long ts;
}
