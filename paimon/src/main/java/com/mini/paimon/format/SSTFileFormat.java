package com.mini.paimon.format;

import com.mini.paimon.metadata.Schema;
import com.mini.paimon.reader.KeyValueFileReader;
import com.mini.paimon.reader.SSTableKeyValueReader;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;
import com.mini.paimon.table.write.KeyValueFileWriter;
import com.mini.paimon.table.write.SSTableKeyValueWriter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * SSTable文件格式实现
 * 参考 Paimon FileFormat 设计
 * 
 * 用于主键表（支持更新删除）的数据读写
 */
public class SSTFileFormat extends FileFormat {
    
    public static final String SST_IDENTIFIER = "sst";
    
    public SSTFileFormat() {
        super(SST_IDENTIFIER);
    }
    
    @Override
    public FormatReaderFactory createReaderFactory(
            Schema schema,
            @Nullable Projection projection,
            @Nullable List<Predicate> filters) {
        return new SSTReaderFactory(schema, projection, filters);
    }
    
    @Override
    public FormatWriterFactory createWriterFactory(Schema schema) {
        return new SSTWriterFactory(schema);
    }
    
    /**
     * SSTable读取器工厂实现
     */
    private static class SSTReaderFactory implements FormatReaderFactory {
        
        private final Schema schema;
        private final Projection projection;
        private final List<Predicate> filters;
        
        public SSTReaderFactory(
                Schema schema,
                @Nullable Projection projection,
                @Nullable List<Predicate> filters) {
            this.schema = schema;
            this.projection = projection;
            this.filters = filters;
        }
        
        @Override
        public KeyValueFileReader createReader(ReaderContext context) throws IOException {
            SSTableKeyValueReader reader = new SSTableKeyValueReader(context.filePath(), schema);
            
            // 应用投影
            if (projection != null) {
                reader.withProjection(projection);
            }
            
            // 应用过滤条件
            if (filters != null && !filters.isEmpty()) {
                // 目前简化处理，只支持单个Predicate
                if (filters.size() == 1) {
                    reader.withFilter(filters.get(0));
                }
            }
            
            return reader;
        }
    }
    
    /**
     * SSTable写入器工厂实现
     */
    private static class SSTWriterFactory implements FormatWriterFactory {
        
        private final Schema schema;
        
        public SSTWriterFactory(Schema schema) {
            this.schema = schema;
        }
        
        @Override
        public KeyValueFileWriter createWriter(WriterContext context) throws IOException {
            // SSTable格式暂不支持压缩，忽略compression参数
            return new SSTableKeyValueWriter(context.filePath(), schema);
        }
    }
}


