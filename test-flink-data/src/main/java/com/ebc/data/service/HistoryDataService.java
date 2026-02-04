package com.ebc.data.service;

import java.io.Serializable;

public interface HistoryDataService extends Serializable {
    /**
     * Get the historical value for a point at a specific time (usually the start of a period).
     * @param companyId  Company ID
     * @param deviceId   Device ID
     * @param pointCode  The point code (property_name)
     * @param timestamp  The reference timestamp (usually the start of the period we are interested in)
     * @return The value at (or immediately before) the timestamp, or null if not found.
     */
    Double getHistoryValue(String companyId, String deviceId, String pointCode, long timestamp);

    /**
     * Get the full historical data object.
     */
    com.ebc.data.model.IotDataMinute getLatestHistoryData(String companyId, String deviceId, String pointCode, long timestamp);
}
