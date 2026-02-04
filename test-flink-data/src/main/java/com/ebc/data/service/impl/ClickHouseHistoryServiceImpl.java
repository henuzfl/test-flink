package com.ebc.data.service.impl;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.ebc.data.service.HistoryDataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

@Slf4j
public class ClickHouseHistoryServiceImpl implements HistoryDataService {

    private final String url;
    private final Properties properties;

    public ClickHouseHistoryServiceImpl(ParameterTool params) {
        // e.g. jdbc:clickhouse://10.19.93.240:8123/default
        this.url = params.get("clickhouse.url", "jdbc:clickhouse://10.19.93.240:8123/default");
        this.properties = new Properties();
        this.properties.setProperty("user", params.get("clickhouse.username", "default"));
        this.properties.setProperty("password", params.get("clickhouse.password", ""));
        // socket_timeout
        this.properties.setProperty("socket_timeout", "10000");
    }

    @Override
    public Double getHistoryValue(String companyId, String deviceId, String pointCode, long timestamp) {
        // Table: iot_db.IOT_DATA_MINUTE
        // Columns: COMPANY_ID, DEVICE_ID, PROPERTY_NAME, PROPERTY_NUM_VALUE, TIMESTAMP(UInt64 ms), ...
        String query = "SELECT PROPERTY_NUM_VALUE FROM iot_db.IOT_DATA_MINUTE " +
                "WHERE COMPANY_ID = ? AND DEVICE_ID = ? AND PROPERTY_NAME = ? AND TIMESTAMP <= ? " +
                "ORDER BY TIMESTAMP DESC LIMIT 1";
        
        try (Connection conn = new ClickHouseDataSource(url, properties).getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
             
            stmt.setString(1, companyId);
            stmt.setString(2, deviceId);
            stmt.setString(3, pointCode);
            stmt.setLong(4, timestamp); 
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getDouble(1);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to query ClickHouse for history value of {}/{}/{}: {}", 
                    companyId, deviceId, pointCode, e.getMessage());
        }
        return null;
    }
    @Override
    public com.ebc.data.model.IotDataMinute getLatestHistoryData(String companyId, String deviceId, String pointCode, long timestamp) {
        String query = "SELECT * FROM iot_db.IOT_DATA_MINUTE " +
                "WHERE COMPANY_ID = ? AND DEVICE_ID = ? AND PROPERTY_NAME = ? AND TIMESTAMP <= ? " +
                "ORDER BY TIMESTAMP DESC LIMIT 1";

        try (Connection conn = new ClickHouseDataSource(url, properties).getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setString(1, companyId);
            stmt.setString(2, deviceId);
            stmt.setString(3, pointCode);
            stmt.setLong(4, timestamp);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    com.ebc.data.model.IotDataMinute data = new com.ebc.data.model.IotDataMinute();
                    data.setDataId(rs.getLong("DATA_ID"));
                    data.setDataDate(rs.getTimestamp("DATA_DATE"));
                    data.setDataType(rs.getString("DATA_TYPE"));
                    data.setCompanyId(rs.getString("COMPANY_ID"));
                    data.setProjectId(rs.getString("PROJECT_ID"));
                    data.setGatewayCode(rs.getString("GATEWAY_CODE"));
                    data.setProtocolCode(rs.getString("PROTOCOL_CODE"));
                    data.setProductId(rs.getString("PRODUCT_ID"));
                    data.setDeviceId(rs.getString("DEVICE_ID"));
                    data.setPropertyName(rs.getString("PROPERTY_NAME"));
                    data.setPropertyValue(rs.getString("PROPERTY_VALUE"));
                    data.setPropertyNumValue(rs.getDouble("PROPERTY_NUM_VALUE"));
                    data.setMessageId(rs.getString("MESSAGE_ID"));
                    data.setTimestamp(rs.getLong("TIMESTAMP"));
                    data.setCreateDate(rs.getTimestamp("CREATE_DATE"));
                    data.setYear(rs.getInt("YEAR"));
                    data.setMonth(rs.getInt("MONTH"));
                    data.setDay(rs.getInt("DAY"));
                    data.setHour(rs.getInt("HOUR"));
                    return data;
                }
            }
        } catch (SQLException e) {
            log.error("Failed to query ClickHouse for history data of {}/{}/{}: {}",
                    companyId, deviceId, pointCode, e.getMessage());
        }
        return null;
    }
}
