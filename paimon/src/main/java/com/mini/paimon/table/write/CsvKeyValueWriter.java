package com.mini.paimon.table.write;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.RowKey;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.storage.CsvWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * CSV KeyValue Writer
 * 参考 Paimon KeyValueFileWriter 设计
 * 
 * 用于非主键表的CSV文件写入
 */
public class CsvKeyValueWriter implements KeyValueFileWriter {
    
    private static final Logger logger = LoggerFactory.getLogger(CsvKeyValueWriter.class);
    
    private final Path filePath;
    private final Schema schema;
    private final CsvWriter csvWriter;
    
    private RowKey minKey;
    private RowKey maxKey;
    
    public CsvKeyValueWriter(Path filePath, Schema schema) throws IOException {
        this.filePath = filePath;
        this.schema = schema;
        this.csvWriter = new CsvWriter(schema, filePath);
        
        logger.debug("Created CsvKeyValueWriter for file: {}", filePath);
    }
    
    @Override
    public void write(Row row) throws IOException {
        csvWriter.write(row);
        
        // 更新min/max key（如果有主键）
        if (schema.hasPrimaryKey()) {
            RowKey rowKey = row.extractKey(schema);
            if (minKey == null || rowKey.compareTo(minKey) < 0) {
                minKey = rowKey;
            }
            if (maxKey == null || rowKey.compareTo(maxKey) > 0) {
                maxKey = rowKey;
            }
        }
    }
    
    @Override
    public void flush() throws IOException {
        csvWriter.flush();
    }
    
    @Override
    public long getRowCount() {
        return csvWriter.getRowCount();
    }
    
    @Override
    public long getFileSize() throws IOException {
        if (Files.exists(filePath)) {
            return Files.size(filePath);
        }
        return 0;
    }
    
    @Override
    public DataFileMeta getFileMeta() throws IOException {
        // 获取相对路径（去掉warehouse路径）
        String fileName = filePath.getFileName().toString();
        String relativePath = "data/" + fileName;
        
        return new DataFileMeta(
            relativePath,
            getFileSize(),
            getRowCount(),
            minKey,
            maxKey,
            0,  // schemaId
            0,  // level
            System.currentTimeMillis()
        );
    }
    
    @Override
    public void close() throws IOException {
        csvWriter.close();
        logger.debug("Closed CsvKeyValueWriter, wrote {} rows to {}", 
                    getRowCount(), filePath);
    }
}

