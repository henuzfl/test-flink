package com.ebc.common.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

/**
 * 全局统一的 Jackson ObjectMapper 工具类
 */
public class JsonMapperUtils {

    private static final ObjectMapper SNAKE_CASE_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);

    /**
     * 获取下划线命名的 ObjectMapper 实例
     */
    public static ObjectMapper getSnakeCaseMapper() {
        return SNAKE_CASE_MAPPER;
    }
}
