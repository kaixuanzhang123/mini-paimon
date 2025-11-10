package com.mini.paimon.table;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.partition.BucketSpec;
import com.mini.paimon.partition.PartitionSpec;
import com.mini.paimon.storage.BucketedLSMTree;
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
    
    /** 全局 SSTable 序列号生成器（跨 TableWrite 实例共享，避免文件名冲突） */
    private static final AtomicLong GLOBAL_SEQUENCE_GENERATOR = new AtomicLong(System.currentTimeMillis());
    
    private final Table table;
    private final Schema schema;
    private final String database;
    private final String tableName;
    private final int batchSize;
    private final List<String> partitionKeys;
    
    // Bucket 配置（默认 4 个 bucket）
    private final int totalBuckets;
    
    // 非分区表使用单个 LSMTree
    private LSMTree nonPartitionedLsmTree;
    
    // 分区表 + Bucket：每个（分区，Bucket）组合使用独立的 LSMTree
    private final Map<String, LSMTree> bucketedLsmTrees;  // Key: partition_path + "#bucket-" + bucket_id
    
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
        this.totalBuckets = 4;  // 默认 4 个 bucket
        
        if (partitionKeys.isEmpty()) {
            // 非分区表：使用单个 LSMTree
            this.nonPartitionedLsmTree = new LSMTree(schema, table.pathFactory(), database, tableName);
            this.bucketedLsmTrees = null;
            this.partitionBuffers = null;
            logger.debug("Created TableWrite for non-partitioned table {}.{}", database, tableName);
        } else {
            // 分区表 + Bucket：为每个（分区，Bucket）组合创建独立的 LSMTree
            this.nonPartitionedLsmTree = null;
            this.bucketedLsmTrees = new ConcurrentHashMap<>();
            this.partitionBuffers = new ConcurrentHashMap<>();
            logger.debug("Created TableWrite for partitioned table {}.{}, partition keys: {}, buckets: {}", 
                database, tableName, partitionKeys, totalBuckets);
        }
    }
    
    /**
     * 写入单行数据
     * 分区表自动根据主键计算 Bucket，实现并行写入
     */
    public void write(Row row) throws IOException {
        validateRow(row);
        
        if (partitionKeys.isEmpty()) {
            // 非分区表：直接写入
            nonPartitionedLsmTree.put(row);
        } else {
            // 分区表：按分区 + Bucket 写入
            PartitionSpec partitionSpec = extractPartitionSpec(row);
            
            // 计算 Bucket：根据主键 hash 分配
            int bucket = computeBucket(row);
            
            // 创建分区目录
            table.partitionManager().createPartition(partitionSpec);
            
            // 获取或创建该（分区，Bucket）的 LSMTree
            LSMTree lsmTree = getOrCreateBucketLsmTree(partitionSpec, bucket);
            
            // 写入数据
            lsmTree.put(row);
            
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
                    // 关键：先 close() 刷盘，再收集文件
                    nonPartitionedLsmTree.close();
                    collectFilesFromLSMTree(nonPartitionedLsmTree, null, newFiles);
                }
            } else {
                // 分区表 + Bucket
                for (Map.Entry<String, LSMTree> entry : bucketedLsmTrees.entrySet()) {
                    String key = entry.getKey();  // 格式：dt=2024-01-01#bucket-0
                    LSMTree lsmTree = entry.getValue();
                    
                    logger.debug("Flushing partition+bucket {} to disk", key);
                    // 关键：先 close() 刷盘，再收集文件
                    lsmTree.close();
                    // 传递完整的 key（包含 bucket 信息）
                    collectFilesFromLSMTree(lsmTree, key, newFiles);
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
     * 性能优化：不再扫描文件目录，直接从 LSMTree 获取写入时维护的元信息
     * 
     * @param lsmTree LSM Tree 实例
     * @param partitionPath 分区路径（非分区表为 null）
     * @param manifestEntries 输出的 Manifest 条目列表
     */
    private void collectFilesFromLSMTree(
            LSMTree lsmTree, 
            String partitionPath, 
            List<ManifestEntry> manifestEntries) throws IOException {
        
        // 性能优化：直接从 LSMTree 获取写入时维护的文件元信息
        // 避免扫描目录和读取文件内容
        List<DataFileMeta> pendingFiles = lsmTree.getPendingFiles();
        
        Path tableDir = table.pathFactory().getTablePath(database, tableName);
        
        for (DataFileMeta fileMeta : pendingFiles) {
            // 计算相对路径：对于分区表，需要加上分区前缀
            // partitionPath 格式如：dt=2024-01-01#bucket-0 或 dt=2024-01-01
            String relativePath;
            if (partitionPath != null && !partitionPath.isEmpty()) {
                // 分区表：路径形如 dt=2024-01-01/bucket-0/data-0-001.sst
                // partitionPath 可能包含 bucket 信息，需要处理
                String actualPath = partitionPath.replace("#", "/");
                relativePath = actualPath + "/" + fileMeta.getFileName();
            } else {
                // 非分区表：路径形如 data/data-0-001.sst
                relativePath = "data/" + fileMeta.getFileName();
            }
            
            // 创建新的 DataFileMeta，使用相对路径
            DataFileMeta relativeFileMeta = new DataFileMeta(
                relativePath,
                fileMeta.getFileSize(),
                fileMeta.getRowCount(),
                fileMeta.getMinKey(),
                fileMeta.getMaxKey(),
                fileMeta.getSchemaId(),
                fileMeta.getLevel(),
                fileMeta.getCreationTime()
            );
            
            // 创建 ManifestEntry（ADD 类型）
            ManifestEntry entry = new ManifestEntry(
                ManifestEntry.FileKind.ADD,
                0,  // bucket（当前默认为 0）
                relativeFileMeta
            );
            
            manifestEntries.add(entry);
            
            logger.debug("Collected file: {}, size: {}, rows: {}, level: {}",
                relativePath, fileMeta.getFileSize(), fileMeta.getRowCount(), fileMeta.getLevel());
        }
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
            // 关键逻辑：
            // 1. 如果已经 prepare 过，LSMTree 已经被 close()，不需要再次关闭
            // 2. 如果没有 prepare 过，需要关闭 LSMTree 以清理资源
            // 3. 但是，由于 prepareCommit() 已经调用了 close()，在 close() 时会刷写数据，
            //    如果再次关闭会导致数据被重复写入
            // 因此，abort() 不应该再次调用 LSMTree.close()
            // 只清理状态即可
            
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
                bucketedLsmTrees.clear();
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
     * 根据行主键计算 Bucket
     * 使用 hash 分配策略，确保相同主键的数据分配到同一个 bucket
     */
    private int computeBucket(Row row) {
        if (!schema.hasPrimaryKey()) {
            // 无主键表：轮询分配到不同 bucket
            return (int)(System.nanoTime() % totalBuckets);
        }
        
        // 有主键表：根据主键 hash 分配
        RowKey rowKey = RowKey.fromRow(row, schema);
        return BucketSpec.computeBucket(rowKey.getBytes(), totalBuckets);
    }
    
    /**
     * 获取或创建（分区 + Bucket）的 LSMTree
     * 每个（分区，Bucket）组合使用独立的数据目录
     */
    private LSMTree getOrCreateBucketLsmTree(PartitionSpec partitionSpec, int bucket) throws IOException {
        // 生成唯一 key：partition_path#bucket-N
        String key = partitionSpec.toPath() + "#bucket-" + bucket;
        
        return bucketedLsmTrees.computeIfAbsent(key, k -> {
            try {
                // 创建 Bucket 专属的 LSMTree
                return new BucketedLSMTree(
                    schema, 
                    table.pathFactory(), 
                    database, 
                    tableName, 
                    partitionSpec,
                    bucket,
                    totalBuckets
                );
            } catch (IOException e) {
                throw new RuntimeException("Failed to create BucketedLSMTree for " + k, e);
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
