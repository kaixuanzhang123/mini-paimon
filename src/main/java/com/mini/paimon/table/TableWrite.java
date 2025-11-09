package com.mini.paimon.table;

import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.partition.PartitionSpec;
import com.mini.paimon.storage.LSMTree;
import com.mini.paimon.storage.PartitionedLSMTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Table 写入器
 * 参考 Paimon TableWrite 设计，负责数据写入
 * 支持分区表写入：每个分区使用独立的 LSMTree
 */
public class TableWrite implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TableWrite.class);
    
    private final Table table;
    private final Schema schema;
    private final String database;
    private final String tableName;
    private final int batchSize;
    private final List<String> partitionKeys;
    
    // 非分区表使用单个 LSMTree
    private LSMTree nonPartitionedLsmTree;
    
    // 分区表：每个分区使用独立的 LSMTree
    private final Map<PartitionSpec, LSMTree> partitionedLsmTrees;
    
    // 缓冲区：按分区组织数据
    private final Map<PartitionSpec, List<Row>> partitionBuffers;
    
    public TableWrite(Table table, int batchSize) throws IOException {
        this.table = table;
        this.schema = table.schema();
        this.database = table.identifier().getDatabase();
        this.tableName = table.identifier().getTable();
        this.batchSize = batchSize;
        this.partitionKeys = schema.getPartitionKeys();
        
        if (partitionKeys.isEmpty()) {
            // 非分区表：使用单个 LSMTree
            this.nonPartitionedLsmTree = new LSMTree(schema, table.pathFactory(), database, tableName);
            this.partitionedLsmTrees = null;
            this.partitionBuffers = null;
            logger.debug("Created TableWrite for non-partitioned table {}.{}", database, tableName);
        } else {
            // 分区表：为每个分区创建独立的 LSMTree
            this.nonPartitionedLsmTree = null;
            this.partitionedLsmTrees = new ConcurrentHashMap<>();
            this.partitionBuffers = new ConcurrentHashMap<>();
            logger.debug("Created TableWrite for partitioned table {}.{}, partition keys: {}", 
                database, tableName, partitionKeys);
        }
    }
    
    /**
     * 写入单行数据
     * 如果是分区表，数据写入到对应分区目录
     */
    public void write(Row row) throws IOException {
        validateRow(row);
        
        if (partitionKeys.isEmpty()) {
            // 非分区表：直接写入
            nonPartitionedLsmTree.put(row);
        } else {
            // 分区表：按分区写入
            PartitionSpec partitionSpec = extractPartitionSpec(row);
            
            // 创建分区目录
            table.partitionManager().createPartition(partitionSpec);
            
            // 获取或创建该分区的 LSMTree
            LSMTree partitionLsmTree = getOrCreatePartitionLsmTree(partitionSpec);
            
            // 写入数据
            partitionLsmTree.put(row);
            
            // 添加到缓冲区用于批量管理
            partitionBuffers.computeIfAbsent(partitionSpec, k -> new ArrayList<>()).add(row);
        }
    }
    
    /**
     * 批量写入数据
     */
    public void write(List<Row> rows) throws IOException {
        for (Row row : rows) {
            write(row);
        }
    }
    
    /**
     * 刷写缓冲区数据
     */
    public void flush() throws IOException {
        if (partitionKeys.isEmpty()) {
            // 非分区表：无需额外操作，LSMTree 内部管理刷写
            return;
        } else {
            // 分区表：清空缓冲区（数据已写入各分区的 LSMTree）
            partitionBuffers.clear();
        }
    }
    
    /**
     * 准备提交（返回提交信息）
     * 注意：必须先刷写数据到磁盘，再让 TableCommit 收集文件
     */
    public TableCommitMessage prepareCommit() throws IOException {
        // 关闭所有 LSMTree，将数据刷写到磁盘
        if (partitionKeys.isEmpty()) {
            if (nonPartitionedLsmTree != null) {
                nonPartitionedLsmTree.close();
            }
        } else {
            for (Map.Entry<PartitionSpec, LSMTree> entry : partitionedLsmTrees.entrySet()) {
                logger.debug("Flushing partition {} to disk", entry.getKey().toPath());
                entry.getValue().close();
            }
        }
        
        return new TableCommitMessage(database, tableName, schema.getSchemaId());
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
            // prepareCommit 已经关闭了 LSMTree，这里不需要再次关闭
            if (partitionKeys.isEmpty()) {
                nonPartitionedLsmTree = null;
            } else {
                partitionedLsmTrees.clear();
                partitionBuffers.clear();
            }
            
            logger.debug("Closed TableWrite for {}.{}", database, tableName);
        } catch (Exception e) {
            logger.error("Error closing TableWrite", e);
            throw new IOException("Failed to close TableWrite", e);
        }
    }
    
    /**
     * 从行数据中提取分区规范
     */
    private PartitionSpec extractPartitionSpec(Row row) {
        Map<String, String> partitionValues = new LinkedHashMap<>();
        
        for (String partitionKey : partitionKeys) {
            int fieldIndex = schema.getFieldIndex(partitionKey);
            if (fieldIndex < 0) {
                throw new IllegalArgumentException("Partition key not found: " + partitionKey);
            }
            
            Object value = row.getValue(fieldIndex);
            if (value == null) {
                throw new IllegalArgumentException("Partition key cannot be null: " + partitionKey);
            }
            
            partitionValues.put(partitionKey, value.toString());
        }
        
        return new PartitionSpec(partitionValues);
    }
    
    /**
     * 获取或创建分区的 LSMTree
     * 每个分区使用独立的数据目录
     */
    private LSMTree getOrCreatePartitionLsmTree(PartitionSpec partitionSpec) throws IOException {
        return partitionedLsmTrees.computeIfAbsent(partitionSpec, spec -> {
            try {
                // 创建分区专属的 LSMTree
                // 使用分区路径作为子目录
                return new PartitionedLSMTree(schema, table.pathFactory(), 
                    database, tableName, spec);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create LSMTree for partition " + spec, e);
            }
        });
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
