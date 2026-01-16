package com.flink.test.iot.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;

/**
 * IoT Point Data POJO
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PointData implements Serializable {
    private Integer company_id;
    private String device_code;
    private String point_code;
    private Double value;
    private Long ts;

    public PointData() {}

    public PointData(Integer company_id, String device_code, String point_code, Double value, Long ts) {
        this.company_id = company_id;
        this.device_code = device_code;
        this.point_code = point_code;
        this.value = value;
        this.ts = ts;
    }

    public Integer getCompany_id() {
        return company_id;
    }

    public void setCompany_id(Integer company_id) {
        this.company_id = company_id;
    }

    public String getDevice_code() {
        return device_code;
    }

    public void setDevice_code(String device_code) {
        this.device_code = device_code;
    }

    public String getPoint_code() {
        return point_code;
    }

    public void setPoint_code(String point_code) {
        this.point_code = point_code;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "PointData{" +
                "company_id=" + company_id +
                ", device_code='" + device_code + '\'' +
                ", point_code='" + point_code + '\'' +
                ", value=" + value +
                ", ts=" + ts +
                '}';
    }
}
