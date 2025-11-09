package com.mini.paimon.table;

import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.storage.LSMTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Table 写入器
 * 参考 Paimon TableWrite 设计，负责数据写入
 */
public class TableWrite implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TableWrite.class);
    
    private final Schema schema;
    private final String database;
    private final String table;
    private final LSMTree lsmTree;
    private final List<Row> buffer;
    private final int batchSize;
    
    public TableWrite(Table table, int batchSize) throws IOException {
        this.schema = table.schema();
        this.database = table.identifier().getDatabase();
        this.table = table.identifier().getTable();
        this.batchSize = batchSize;
        this.buffer = new ArrayList<>();
        
        // 创建 LSMTree 用于实际写入
        this.lsmTree = new LSMTree(schema, table.pathFactory(), database, this.table);
        
        logger.debug("Created TableWrite for {}.{}", database, this.table);
    }
    
    /**
     * 写入单行数据
     */
    public void write(Row row) throws IOException {
        validateRow(row);
        buffer.add(row);
        
        if (buffer.size() >= batchSize) {
            flush();
        }
    }
    
    /**
     * 批量写入数据
     */
    public void write(List<Row> rows) throws IOException {
        for (Row row : rows) {
            validateRow(row);
            buffer.add(row);
        }
        
        if (buffer.size() >= batchSize) {
            flush();
        }
    }
    
    /**
     * 刷写缓冲区数据
     */
    public void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        
        logger.debug("Flushing {} rows to LSMTree", buffer.size());
        
        for (Row row : buffer) {
            lsmTree.put(row);
        }
        
        buffer.clear();
    }
    
    /**
     * 准备提交（返回提交信息）
     */
    public TableCommitMessage prepareCommit() throws IOException {
        flush();
        return new TableCommitMessage(database, table, schema.getSchemaId());
    }
    
    /**
     * 验证行数据
     */
    private void validateRow(Row row) {
        if (row == null) {
            throw new IllegalArgumentException("Row cannot be null");
        }
        
        if (row.getValues().length != schema.getFields().size()) {
            throw new IllegalArgumentException(
                String.format("Row field count mismatch. Expected: %d, Actual: %d",
                    schema.getFields().size(), row.getValues().length));
        }
    }
    
    @Override
    public void close() throws IOException {
        try {
            flush();
            lsmTree.close();
            logger.debug("Closed TableWrite for {}.{}", database, table);
        } catch (Exception e) {
            logger.error("Error closing TableWrite", e);
            throw new IOException("Failed to close TableWrite", e);
        }
    }
    
    /**
     * Table 提交消息
     */
    public static class TableCommitMessage {
        private final String database;
        private final String table;
        private final int schemaId;
        
        public TableCommitMessage(String database, String table, int schemaId) {
            this.database = database;
            this.table = table;
            this.schemaId = schemaId;
        }
        
        public String getDatabase() {
            return database;
        }
        
        public String getTable() {
            return table;
        }
        
        public int getSchemaId() {
            return schemaId;
        }
    }
}
