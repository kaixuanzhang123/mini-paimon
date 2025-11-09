package com.mini.paimon.reader;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;

import java.io.IOException;

/**
 * Record Reader Factory
 * 参考 Paimon 的 ReaderFactory 设计
 * 
 * 职责：
 * 1. 创建各种类型的 Reader
 * 2. 封装 Reader 创建的复杂性
 * 3. 支持 Reader 的配置和优化
 */
public class RecordReaderFactory {
    
    private final Schema schema;
    
    // 可选的过滤和投影
    private Predicate predicate;
    private Projection projection;
    
    // 范围扫描
    private RowKey startKey;
    private RowKey endKey;
    
    public RecordReaderFactory(Schema schema) {
        this.schema = schema;
    }
    
    /**
     * 设置过滤条件
     */
    public RecordReaderFactory withFilter(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }
    
    /**
     * 设置投影
     */
    public RecordReaderFactory withProjection(Projection projection) {
        this.projection = projection;
        return this;
    }
    
    /**
     * 设置扫描范围
     */
    public RecordReaderFactory withKeyRange(RowKey startKey, RowKey endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
        return this;
    }
    
    /**
     * 创建文件读取器
     */
    public FileRecordReader createFileReader(String filePath) throws IOException {
        SSTableRecordReader reader = new SSTableRecordReader(filePath, schema);
        
        if (predicate != null) {
            reader.withFilter(predicate);
        }
        if (projection != null) {
            reader.withProjection(projection);
        }
        if (startKey != null || endKey != null) {
            reader.withRange(startKey, endKey);
        }
        
        return reader;
    }
    
    /**
     * 创建文件读取器（基于元信息）
     */
    public FileRecordReader createFileReader(DataFileMeta fileMeta) throws IOException {
        return createFileReader(fileMeta.getFileName());
    }
}
