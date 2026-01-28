package com.ebc.common.event;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 包装三张表的 CDC 事件
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BusLakConfigEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String table;
    private String op;
    private JsonNode data;
}
