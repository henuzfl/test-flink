package com.ebc.common.event;

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
    private String data; // JSON string representation of the data
}
