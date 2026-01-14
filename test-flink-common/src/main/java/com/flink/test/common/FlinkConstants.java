package com.flink.test.common;

import java.io.Serializable;

/**
 * Common utilities for Flink jobs
 */
public class FlinkConstants implements Serializable {
    public static final String DEFAULT_KAFKA_SERVER = "10.19.93.228:9092";
    public static final String INPUT_TOPIC = "demo-flink-01";
    public static final String OUTPUT_TOPIC = "output-topic";
    public static final String VALUE_AGG_OUTPUT_TOPIC = "value-agg-output";
}
