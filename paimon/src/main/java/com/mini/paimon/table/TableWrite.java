package com.mini.paimon.table;

import com.mini.paimon.index.IndexType;
import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.metadata.TableMetadata;
import com.mini.paimon.metadata.TableType;
import com.mini.paimon.partition.PartitionSpec;
import com.mini.paimon.storage.AppendOnlyWriter;
import com.mini.paimon.storage.MergeTreeWriter;
import com.mini.paimon.storage.RecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TableWrite - 表写入器
 * 参考 Paimon TableWrite 设计,重构为使用 RecordWriter 架构
 * <p>
 * 根据表类型自动选择写入器:
 * 1. 主键表 (PRIMARY_KEY) -> MergeTreeWriter (使用 LSM Tree)
 * 2. 仅追加表 (APPEND_ONLY) -> AppendOnlyWriter (直接写文件)
 * <p>
 * 两阶段提交流程:
 * 1. write() - 写入数据
 * 2. prepareCommit() - 刷盘并收集文件元信息
 * 3. TableCommit.commit() - 原子性提交 Snapshot
 */
public class TableWrite implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TableWrite.class);

    private static final AtomicLong COMMIT_IDENTIFIER_GENERATOR = new AtomicLong(System.currentTimeMillis());

    private final Table table;
    private final Schema schema;
    private final String database;
    private final String tableName;
    private final TableType tableType;
    private final List<String> partitionKeys;
    private final long writerId;

    // 非分区表: 使用单个 RecordWriter
    private RecordWriter nonPartitionedWriter;

    // 分区表: 每个分区使用独立的 RecordWriter
    private final Map<String, RecordWriter> partitionWriters;

    // 提交状态管理
    private volatile boolean prepared = false;
    private volatile boolean committed = false;
    private volatile TableCommitMessage lastCommitMessage = null;

    public TableWrite(Table table, int batchSize) throws IOException {
        this(table, batchSize, 0L);
    }

    public TableWrite(Table table, int batchSize, long writerId) throws IOException {
        this.table = table;
        this.schema = table.schema();
        this.database = table.identifier().getDatabase();
        this.tableName = table.identifier().getTable();
        this.tableType = TableType.fromSchema(schema);
        this.partitionKeys = schema.getPartitionKeys();
        this.writerId = writerId;

        if (partitionKeys.isEmpty()) {
            // 非分区表: 创建单个 Writer
            this.nonPartitionedWriter = createWriter(database, tableName, writerId);
            this.partitionWriters = null;
            logger.info("Created TableWrite for non-partitioned {} table {}.{} with writerId={}",
                    tableType, database, tableName, writerId);
        } else {
            // 分区表: 按需创建 Writer
            this.nonPartitionedWriter = null;
            this.partitionWriters = new ConcurrentHashMap<>();
            logger.info("Created TableWrite for partitioned {} table {}.{} with writerId={}",
                    tableType, database, tableName, writerId);
        }
    }

    /**
     * 根据表类型创建对应的 RecordWriter
     */
    private RecordWriter createWriter(String database, String tableName, long writerId) throws IOException {
        // 获取索引配置（对所有表类型）
        Map<String, List<IndexType>> indexConfig = getIndexConfig();
        
        if (tableType.isPrimaryKey()) {
            // 主键表: 使用 MergeTreeWriter (现在也支持索引)
            return new MergeTreeWriter(schema, table.pathFactory(), database, tableName, writerId, indexConfig);
        } else {
            // 仅追加表: 使用 AppendOnlyWriter
            return new AppendOnlyWriter(schema, table.pathFactory(), database, tableName, writerId, indexConfig);
        }
    }

    /**
     * 获取索引配置
     */
    private Map<String, List<IndexType>> getIndexConfig() {
        if (table instanceof FileStoreTable) {
            FileStoreTable fileStoreTable = (FileStoreTable) table;
            TableMetadata metadata = fileStoreTable.tableMetadata();
            if (metadata != null) {
                return metadata.getIndexConfig();
            }
        }
        return new HashMap<>();
    }

    /**
     * 写入单行数据
     */
    public void write(Row row) throws IOException {
        validateRow(row);

        if (partitionKeys.isEmpty()) {
            // 非分区表: 直接写入
            nonPartitionedWriter.write(row);
        } else {
            // 分区表: 获取或创建分区的 Writer
            PartitionSpec partitionSpec = extractPartitionSpec(row);

            // 创建分区目录
            table.partitionManager().createPartition(partitionSpec);

            // 获取或创建分区的 Writer
            RecordWriter writer = getOrCreatePartitionWriter(partitionSpec);

            // 写入数据
            writer.write(row);
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
     * 准备提交 (两阶段提交的 Prepare 阶段)
     */
    public synchronized TableCommitMessage prepareCommit() throws IOException {
        if (prepared) {
            throw new IllegalStateException("prepareCommit has already been called. " +
                    "Create a new TableWrite for next commit.");
        }

        logger.info("Starting prepare commit for {} table {}.{}",
                tableType, database, tableName);
        long startTime = System.currentTimeMillis();

        List<ManifestEntry> newFiles = new ArrayList<>();

        try {
            // 刷写所有 Writer 并收集文件信息
            if (partitionKeys.isEmpty()) {
                // 非分区表
                if (nonPartitionedWriter != null) {
                    List<DataFileMeta> files = nonPartitionedWriter.prepareCommit();
                    for (DataFileMeta fileMeta : files) {
                        // 非分区表: 文件路径格式 data/xxx.sst
                        String relativePath = "data/" + fileMeta.getFileName();
                        DataFileMeta relativeFileMeta = new DataFileMeta(
                                relativePath,
                                fileMeta.getFileSize(),
                                fileMeta.getRowCount(),
                                fileMeta.getMinKey(),
                                fileMeta.getMaxKey(),
                                fileMeta.getSchemaId(),
                                fileMeta.getLevel(),
                                fileMeta.getCreationTime(),
                                fileMeta.getIndexMetas()
                        );

                        ManifestEntry entry = new ManifestEntry(
                                ManifestEntry.FileKind.ADD,
                                0,
                                relativeFileMeta
                        );
                        newFiles.add(entry);
                    }
                }
            } else {
                // 分区表
                for (Map.Entry<String, RecordWriter> writerEntry : partitionWriters.entrySet()) {
                    String partitionPath = writerEntry.getKey();
                    RecordWriter writer = writerEntry.getValue();

                    List<DataFileMeta> files = writer.prepareCommit();
                    for (DataFileMeta fileMeta : files) {
                        // 分区表: 文件路径格式 partition_path/xxx.sst
                        String relativePath = partitionPath + "/" + fileMeta.getFileName();
                        DataFileMeta relativeFileMeta = new DataFileMeta(
                                relativePath,
                                fileMeta.getFileSize(),
                                fileMeta.getRowCount(),
                                fileMeta.getMinKey(),
                                fileMeta.getMaxKey(),
                                fileMeta.getSchemaId(),
                                fileMeta.getLevel(),
                                fileMeta.getCreationTime(),
                                fileMeta.getIndexMetas()
                        );

                        ManifestEntry manifestEntry = new ManifestEntry(
                                ManifestEntry.FileKind.ADD,
                                0,
                                relativeFileMeta
                        );
                        newFiles.add(manifestEntry);
                    }
                }
            }

            // 生成提交标识符
            long commitIdentifier = COMMIT_IDENTIFIER_GENERATOR.incrementAndGet();

            // 创建提交消息
            lastCommitMessage = new TableCommitMessage(
                    database,
                    tableName,
                    schema.getSchemaId(),
                    commitIdentifier,
                    newFiles
            );

            prepared = true;

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Prepare commit completed for {} table {}.{}: {} files, took {}ms",
                    tableType, database, tableName, newFiles.size(), duration);

            return lastCommitMessage;

        } catch (IOException e) {
            logger.error("Failed to prepare commit for table {}.{}", database, tableName, e);
            try {
                abort();
            } catch (Exception abortException) {
                logger.warn("Failed to abort after prepare failure", abortException);
            }
            throw e;
        }
    }

    /**
     * 中止提交
     */
    public synchronized void abort() throws IOException {
        if (committed) {
            throw new IllegalStateException("Cannot abort after commit");
        }

        logger.info("Aborting write operation for table {}.{}", database, tableName);

        prepared = false;
        lastCommitMessage = null;

        logger.info("Write operation aborted for table {}.{}", database, tableName);
    }

    /**
     * 标记为已提交 (由 TableCommit 调用)
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
            // 如果没有 commit, 但已经 prepare, 需要中止
            if (prepared && !committed) {
                logger.warn("Closing TableWrite without commit, aborting prepared changes");
                abort();
            }

            // 关闭所有 Writer
            if (partitionKeys.isEmpty()) {
                if (nonPartitionedWriter != null) {
                    nonPartitionedWriter.close();
                }
            } else {
                for (RecordWriter writer : partitionWriters.values()) {
                    writer.close();
                }
                partitionWriters.clear();
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
     * 获取或创建分区的 RecordWriter
     */
    private RecordWriter getOrCreatePartitionWriter(PartitionSpec partitionSpec) throws IOException {
        String partitionPath = partitionSpec.toPath();

        return partitionWriters.computeIfAbsent(partitionPath, key -> {
            try {
                // 为分区创建专属的 Writer
                // 使用 partitionPath 的 hashCode 作为 writerId 的一部分,确保唯一性
                long partitionWriterId = writerId + partitionPath.hashCode();
                return createWriter(database, tableName, partitionWriterId);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create writer for partition " + key, e);
            }
        });
    }

    /**
     * TableCommitMessage - 提交消息
     */
    public static class TableCommitMessage implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

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
