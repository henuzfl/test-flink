package com.flink.test.state;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;

/**
 * Data model for device temperature messages.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceData implements Serializable {
    private String deviceId;
    private String property;
    private Long value;
    private Long ts;

    public DeviceData() {}

    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }

    public String getProperty() { return property; }
    public void setProperty(String property) { this.property = property; }

    public Long getValue() { return value; }
    public void setValue(Long value) { this.value = value; }

    public Long getTs() { return ts; }
    public void setTs(Long ts) { this.ts = ts; }

    @Override
    public String toString() {
        return "DeviceData{" +
                "deviceId='" + deviceId + '\'' +
                ", property='" + property + '\'' +
                ", value=" + value +
                ", ts=" + ts +
                '}';
    }
}
