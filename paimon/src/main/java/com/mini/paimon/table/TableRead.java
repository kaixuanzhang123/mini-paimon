package com.mini.paimon.table;

import com.mini.paimon.schema.Row;

import java.io.IOException;
import java.util.List;

/**
 * Table 读取器
 * 根据扫描计划读取数据
 */
public interface TableRead {
    
    /**
     * 设置投影
     */
    TableRead withProjection(Projection projection);
    
    /**
     * 设置过滤条件
     */
    TableRead withFilter(Predicate predicate);
    
    /**
     * 根据扫描计划读取数据
     */
    List<Row> read(TableScan.Plan plan) throws IOException;
}
