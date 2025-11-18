package com.mini.paimon.reader;

import com.mini.paimon.format.FileFormat;
import com.mini.paimon.format.FileFormatFactory;
import com.mini.paimon.format.FormatReaderFactory;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * KeyValue File Reader Factory
 * 参考 Paimon 的 KeyValueFileReaderFactory 设计
 * 使用 FileFormat 工厂模式创建文件读取器,支持谓词和投影下推
 * 
 * 支持的格式:
 * - CSV格式 (用于非主键表)
 * - SST格式 (用于主键表)
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
     * 使用 FileFormatFactory 根据文件扩展名选择合适的格式
     */
    public KeyValueFileReader createReader(String filePath) throws IOException {
        // 1. 根据文件扩展名获取文件格式
        FileFormat format = FileFormatFactory.getFormatByFileName(filePath);
        
        // 2. 准备过滤条件列表
        List<Predicate> filters = null;
        if (predicate != null) {
            filters = new ArrayList<>();
            filters.add(predicate);
        }
        
        // 3. 创建读取器工厂
        FormatReaderFactory readerFactory = format.createReaderFactory(
            schema,
            projection,
            filters
        );
        
        // 4. 使用读取器工厂创建读取器
        return readerFactory.createReader(new ReaderContextImpl(filePath));
    }
    
    /**
     * ReaderContext实现
     */
    private static class ReaderContextImpl implements FormatReaderFactory.ReaderContext {
        private final String filePath;
        
        public ReaderContextImpl(String filePath) {
            this.filePath = filePath;
        }
        
        @Override
        public String filePath() {
            return filePath;
        }
    }
}


