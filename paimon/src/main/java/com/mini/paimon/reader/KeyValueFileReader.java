package com.mini.paimon.reader;

import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.RowKey;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;

import java.io.Closeable;
import java.io.IOException;

/**
 * KeyValue File Reader Interface
 * 参考 Paimon 的 KeyValueFileReader 设计
 * 提供键值文件的读取接口
 */
public interface KeyValueFileReader extends Closeable {
    
    /**
     * 设置过滤条件
     */
    KeyValueFileReader withFilter(Predicate predicate);
    
    /**
     * 设置投影
     */
    KeyValueFileReader withProjection(Projection projection);
    
    /**
     * 点查询 - 根据键获取值
     */
    Row get(RowKey key) throws IOException;
    
    /**
     * 范围查询 - 获取指定范围内的所有行
     */
    RecordReader<Row> readRange(RowKey startKey, RowKey endKey) throws IOException;
    
    /**
     * 全表扫描
     */
    RecordReader<Row> readAll() throws IOException;
}


