package com.mini.paimon.table;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.partition.PartitionSpec;
import com.mini.paimon.storage.LSMTree;
import com.mini.paimon.storage.PartitionedLSMTree;
import com.mini.paimon.storage.SSTable;
import com.mini.paimon.storage.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Table 写入器
 * 参考 Paimon TableWrite 设计，负责数据写入
 * 支持分区表写入：每个分区使用独立的 LSMTree
 * 
 * 两阶段提交流程：
 * 1. write() - 写入数据到 MemTable
 * 2. prepareCommit() - 刷盘并收集文件元信息（Prepare 阶段）
 * 3. TableCommit.commit() - 原子性提交 Snapshot（Commit 阶段）
 */
public class TableWrite implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TableWrite.class);
    
    /** 提交标识符生成器 */
    private static final AtomicLong COMMIT_IDENTIFIER_GENERATOR = new AtomicLong(System.currentTimeMillis());
    
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
    
    // 提交状态管理
    private volatile boolean prepared = false;
    private volatile boolean committed = false;
    private volatile TableCommitMessage lastCommitMessage = null;
    
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
     * 准备提交（两阶段提交的 Prepare 阶段）
     * 
     * Prepare 阶段职责：
     * 1. 刷写所有 MemTable 到磁盘（生成 SSTable 文件）
     * 2. 收集所有数据文件的元信息（DataFileMeta）
     * 3. 生成 ManifestEntry 列表
     * 4. 创建 CommitMessage 返回给 Commit 阶段
     * 
     * 此阶段不修改任何元数据，保证可回滚
     * 
     * @return 提交消息，包含所有需要提交的文件信息
     * @throws IOException 刷盘失败
     * @throws IllegalStateException 如果已经执行过 prepareCommit
     */
    public synchronized TableCommitMessage prepareCommit() throws IOException {
        if (prepared) {
            throw new IllegalStateException("prepareCommit has already been called. " +
                "Create a new TableWrite for next commit.");
        }
        
        logger.info("Starting prepare commit for table {}.{}", database, tableName);
        long startTime = System.currentTimeMillis();
        
        List<ManifestEntry> newFiles = new ArrayList<>();
        
        try {
            // 1. 刷写数据到磁盘并收集文件信息
            if (partitionKeys.isEmpty()) {
                // 非分区表
                if (nonPartitionedLsmTree != null) {
                    logger.debug("Flushing non-partitioned table data");
                    collectFilesFromLSMTree(nonPartitionedLsmTree, null, newFiles);
                    nonPartitionedLsmTree.close();
                }
            } else {
                // 分区表
                for (Map.Entry<PartitionSpec, LSMTree> entry : partitionedLsmTrees.entrySet()) {
                    PartitionSpec partitionSpec = entry.getKey();
                    LSMTree lsmTree = entry.getValue();
                    
                    logger.debug("Flushing partition {} to disk", partitionSpec.toPath());
                    collectFilesFromLSMTree(lsmTree, partitionSpec, newFiles);
                    lsmTree.close();
                }
            }
            
            // 2. 生成提交标识符（用于去重和追踪）
            long commitIdentifier = COMMIT_IDENTIFIER_GENERATOR.incrementAndGet();
            
            // 3. 创建提交消息
            lastCommitMessage = new TableCommitMessage(
                database,
                tableName,
                schema.getSchemaId(),
                commitIdentifier,
                newFiles
            );
            
            prepared = true;
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Prepare commit completed for table {}.{}: {} files, took {}ms",
                database, tableName, newFiles.size(), duration);
            
            return lastCommitMessage;
            
        } catch (IOException e) {
            logger.error("Failed to prepare commit for table {}.{}", database, tableName, e);
            // 准备失败，尝试回滚（关闭所有 LSMTree）
            try {
                abort();
            } catch (Exception abortException) {
                logger.warn("Failed to abort after prepare failure", abortException);
            }
            throw e;
        }
    }
    
    /**
     * 从 LSMTree 收集已刷盘的 SSTable 文件信息
     * 
     * @param lsmTree LSM Tree 实例
     * @param partitionSpec 分区规范（非分区表为 null）
     * @param manifestEntries 输出的 Manifest 条目列表
     */
    private void collectFilesFromLSMTree(
            LSMTree lsmTree, 
            PartitionSpec partitionSpec, 
            List<ManifestEntry> manifestEntries) throws IOException {
        
        // 扫描数据目录，获取所有 SSTable 文件
        Path tableDir = table.pathFactory().getTablePath(database, tableName);
        Path scanDir = partitionSpec == null ? tableDir : tableDir.resolve(partitionSpec.toPath());
        
        if (!Files.exists(scanDir)) {
            logger.debug("Data directory does not exist: {}", scanDir);
            return;
        }
        
        // SSTableReader 用于读取文件元信息
        SSTableReader reader = new SSTableReader();
        
        // 递归查找所有 .sst 文件
        try (java.util.stream.Stream<Path> stream = Files.walk(scanDir)) {
            stream.filter(path -> path.toString().endsWith(".sst"))
                .forEach(sstPath -> {
                    try {
                        // 计算相对路径
                        String relativePath = tableDir.relativize(sstPath).toString();
                        
                        // 获取文件大小
                        long fileSize = Files.size(sstPath);
                        
                        // 从文件名中解析 level（如：data-0-123.sst）
                        int level = parseLevelFromFilename(sstPath.getFileName().toString());
                        
                        // 使用 SSTableReader 扫描文件获取统计信息
                        // 简化实现：直接扫描文件获取 min/max key 和 row count
                        List<Row> rows = reader.scan(sstPath.toString());
                        
                        RowKey minKey = null;
                        RowKey maxKey = null;
                        long rowCount = rows.size();
                        
                        // 计算 min/max key
                        if (!rows.isEmpty() && schema.hasPrimaryKey()) {
                            for (Row row : rows) {
                                RowKey rowKey = RowKey.fromRow(row, schema);
                                if (minKey == null || rowKey.compareTo(minKey) < 0) {
                                    minKey = rowKey;
                                }
                                if (maxKey == null || rowKey.compareTo(maxKey) > 0) {
                                    maxKey = rowKey;
                                }
                            }
                        }
                        
                        // 创建 DataFileMeta
                        DataFileMeta fileMeta = new DataFileMeta(
                            relativePath,
                            fileSize,
                            rowCount,
                            minKey,
                            maxKey,
                            schema.getSchemaId(),
                            level,
                            System.currentTimeMillis()
                        );
                        
                        // 创建 ManifestEntry（ADD 类型）
                        ManifestEntry entry = new ManifestEntry(
                            ManifestEntry.FileKind.ADD,
                            0,  // bucket（简化实现，默认为 0）
                            fileMeta
                        );
                        
                        manifestEntries.add(entry);
                        
                        logger.debug("Collected file: {}, size: {}, rows: {}, level: {}",
                            relativePath, fileSize, rowCount, level);
                            
                    } catch (IOException e) {
                        logger.warn("Failed to process SSTable file: {}", sstPath, e);
                    }
                });
        }
    }
    
    /**
     * 从文件名中解析层级
     * 文件名格式：data-{level}-{sequence}.sst
     */
    private int parseLevelFromFilename(String filename) {
        try {
            // 移除 .sst 后缀
            String nameWithoutExt = filename.substring(0, filename.lastIndexOf('.'));
            // 分割 "data-0-123" 为 ["data", "0", "123"]
            String[] parts = nameWithoutExt.split("-");
            if (parts.length >= 2) {
                return Integer.parseInt(parts[1]);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse level from filename: {}", filename, e);
        }
        return 0; // 默认为 Level 0
    }
    
    /**
     * 中止提交（回滚 Prepare 阶段的操作）
     * 注意：此方法只能在 Commit 之前调用
     */
    public synchronized void abort() throws IOException {
        if (committed) {
            throw new IllegalStateException("Cannot abort after commit");
        }
        
        logger.info("Aborting write operation for table {}.{}", database, tableName);
        
        try {
            // 关闭所有 LSMTree
            if (partitionKeys.isEmpty()) {
                if (nonPartitionedLsmTree != null) {
                    nonPartitionedLsmTree.close();
                }
            } else {
                for (LSMTree lsmTree : partitionedLsmTrees.values()) {
                    try {
                        lsmTree.close();
                    } catch (Exception e) {
                        logger.warn("Failed to close LSMTree during abort", e);
                    }
                }
            }
            
            // 清理状态
            prepared = false;
            lastCommitMessage = null;
            
            logger.info("Write operation aborted for table {}.{}", database, tableName);
            
        } catch (Exception e) {
            logger.error("Error during abort for table {}.{}", database, tableName, e);
            throw new IOException("Failed to abort write operation", e);
        }
    }
    
    /**
     * 标记为已提交（由 TableCommit 调用）
     * 内部方法，不对外暴露
     */
    void markCommitted() {
        this.committed = true;
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
            // 如果没有 commit，但已经 prepare，需要中止
            if (prepared && !committed) {
                logger.warn("Closing TableWrite without commit, aborting prepared changes");
                abort();
            }
            
            // 清理资源
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
     * 
     * 包含两阶段提交 Prepare 阶段的所有信息：
     * - 数据库和表名
     * - Schema 版本
     * - 提交标识符（用于去重）
     * - 新增文件的 Manifest 条目列表
     */
    public static class TableCommitMessage {
        private final String database;
        private final String table;
        private final int schemaId;
        private final long commitIdentifier;
        private final List<ManifestEntry> newFiles;
        
        public TableCommitMessage(
                String database, 
                String table, 
                int schemaId, 
                long commitIdentifier,
                List<ManifestEntry> newFiles) {
            this.database = Objects.requireNonNull(database, "Database cannot be null");
            this.table = Objects.requireNonNull(table, "Table cannot be null");
            this.schemaId = schemaId;
            this.commitIdentifier = commitIdentifier;
            this.newFiles = Collections.unmodifiableList(
                new ArrayList<>(Objects.requireNonNull(newFiles, "New files cannot be null")));
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
        
        public long getCommitIdentifier() {
            return commitIdentifier;
        }
        
        public List<ManifestEntry> getNewFiles() {
            return newFiles;
        }
        
        @Override
        public String toString() {
            return "TableCommitMessage{" +
                    "database='" + database + '\'' +
                    ", table='" + table + '\'' +
                    ", schemaId=" + schemaId +
                    ", commitIdentifier=" + commitIdentifier +
                    ", newFiles=" + newFiles.size() +
                    '}';
        }
    }
}
