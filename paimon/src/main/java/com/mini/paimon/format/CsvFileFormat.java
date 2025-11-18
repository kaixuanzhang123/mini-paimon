package com.mini.paimon.format;

import com.mini.paimon.schema.Schema;
import com.mini.paimon.reader.CsvKeyValueReader;
import com.mini.paimon.reader.KeyValueFileReader;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;
import com.mini.paimon.table.write.CsvKeyValueWriter;
import com.mini.paimon.table.write.KeyValueFileWriter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * CSV文件格式实现
 * 参考 Paimon CsvFileFormat 设计
 * 
 * 用于非主键表（Append-Only表）的数据读写
 */
public class CsvFileFormat extends FileFormat {
    
    public static final String CSV_IDENTIFIER = "csv";
    
    public CsvFileFormat() {
        super(CSV_IDENTIFIER);
    }
    
    @Override
    public FormatReaderFactory createReaderFactory(
            Schema schema,
            @Nullable Projection projection,
            @Nullable List<Predicate> filters) {
        return new CsvReaderFactory(schema, projection, filters);
    }
    
    @Override
    public FormatWriterFactory createWriterFactory(Schema schema) {
        return new CsvWriterFactory(schema);
    }
    
    /**
     * CSV读取器工厂实现
     */
    private static class CsvReaderFactory implements FormatReaderFactory {
        
        private final Schema schema;
        private final Projection projection;
        private final List<Predicate> filters;
        
        public CsvReaderFactory(
                Schema schema,
                @Nullable Projection projection,
                @Nullable List<Predicate> filters) {
            this.schema = schema;
            this.projection = projection;
            this.filters = filters;
        }
        
        @Override
        public KeyValueFileReader createReader(ReaderContext context) throws IOException {
            CsvKeyValueReader reader = new CsvKeyValueReader(context.filePath(), schema);
            
            // 应用投影
            if (projection != null) {
                reader.withProjection(projection);
            }
            
            // 应用过滤条件
            if (filters != null && !filters.isEmpty()) {
                // 目前简化处理，只支持单个Predicate
                // 实际应该合并多个Predicate
                if (filters.size() == 1) {
                    reader.withFilter(filters.get(0));
                }
            }
            
            return reader;
        }
    }
    
    /**
     * CSV写入器工厂实现
     */
    private static class CsvWriterFactory implements FormatWriterFactory {
        
        private final Schema schema;
        
        public CsvWriterFactory(Schema schema) {
            this.schema = schema;
        }
        
        @Override
        public KeyValueFileWriter createWriter(WriterContext context) throws IOException {
            // CSV格式暂不支持压缩，忽略compression参数
            return new CsvKeyValueWriter(context.filePath(), schema);
        }
    }
}


