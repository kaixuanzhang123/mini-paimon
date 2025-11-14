package com.mini.paimon.reader;

import com.mini.paimon.metadata.Schema;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;

import java.io.IOException;

/**
 * KeyValue File Reader Factory
 * 参考 Paimon 的 KeyValueFileReaderFactory 设计
 * 负责创建文件读取器,支持谓词和投影下推
 */
public class KeyValueFileReaderFactory {
    
    private final Schema schema;
    private Predicate predicate;
    private Projection projection;
    
    public KeyValueFileReaderFactory(Schema schema) {
        this.schema = schema;
    }
    
    public KeyValueFileReaderFactory withFilter(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }
    
    public KeyValueFileReaderFactory withProjection(Projection projection) {
        this.projection = projection;
        return this;
    }
    
    /**
     * 创建文件读取器
     */
    public KeyValueFileReader createReader(String filePath) throws IOException {
        KeyValueFileReader reader = new SSTableKeyValueReader(filePath, schema);
        
        if (predicate != null) {
            reader.withFilter(predicate);
        }
        
        if (projection != null) {
            reader.withProjection(projection);
        }
        
        return reader;
    }
}


