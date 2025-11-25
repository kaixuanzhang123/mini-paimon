package com.mini.paimon.table;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.schema.SchemaManager;
import com.mini.paimon.schema.TableMetadata;
import com.mini.paimon.partition.PartitionManager;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.branch.BranchManager;
import com.mini.paimon.branch.FileSystemBranchManager;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * 基于文件存储的 Table 实现
 * 参考 Paimon FileStoreTable 设计
 */
public class FileStoreTable implements Table {
    private static final Logger logger = LoggerFactory.getLogger(FileStoreTable.class);

    private final Catalog catalog;
    private final Identifier identifier;
    private final Schema schema;
    private final PathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final PartitionManager partitionManager;
    private final TableMetadata tableMetadata;

    public FileStoreTable(Catalog catalog, Identifier identifier, Schema schema, PathFactory pathFactory) {
        this.catalog = catalog;
        this.identifier = identifier;
        this.schema = schema;
        this.pathFactory = pathFactory;
        this.snapshotManager = new SnapshotManager(
                pathFactory,
                identifier.getDatabase(),
                identifier.getTable()
        );
        this.partitionManager = new PartitionManager(
                pathFactory,
                identifier.getDatabase(),
                identifier.getTable(),
                schema.getPartitionKeys()
        );
        
        // 加载表元数据（包含索引配置）
        TableMetadata metadata = null;
        try {
            metadata = catalog.getTableMetadata(identifier);
        } catch (Exception e) {
            logger.warn("Failed to load table metadata for {}, using empty metadata", identifier, e);
        }
        this.tableMetadata = metadata;

        logger.debug("Created FileStoreTable for {}", identifier);
    }
    
    /**
     * 获取表元数据
     */
    public TableMetadata tableMetadata() {
        return tableMetadata;
    }

    @Override
    public Identifier identifier() {
        return identifier;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public TableScan newScan() {
        return new FileStoreTableScan(this);
    }

    @Override
    public TableRead newRead() {
        // 使用 FileStoreTableRead 以支持分区过滤、Manifest缓存、并行读取等优化特性
        return new FileStoreTableRead(
                schema,
                pathFactory,
                identifier.getDatabase(),
                identifier.getTable()
        );
    }


    @Override
    public TableWrite newWrite() {
        try {
            return new TableWrite(this, 1000);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create TableWrite", e);
        }
    }

    @Override
    public TableWrite newWrite(long writerId) {
        try {
            return new TableWrite(this, 1000, writerId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create TableWrite", e);
        }
    }

    @Override
    public TableCommit newCommit() {
        return new TableCommit(catalog, pathFactory, identifier, schema);
    }

    @Override
    public Optional<Snapshot> latestSnapshot() {
        try {
            if (!snapshotManager.hasSnapshot()) {
                return Optional.empty();
            }
            return Optional.of(snapshotManager.latestSnapshot());
        } catch (IOException e) {
            logger.error("Failed to get latest snapshot", e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<Snapshot> snapshot(long snapshotId) {
        try {
            Snapshot snapshot = snapshotManager.snapshot(snapshotId);
            return Optional.ofNullable(snapshot);
        } catch (IOException e) {
            logger.error("Failed to get snapshot {}", snapshotId, e);
            return Optional.empty();
        }
    }

    @Override
    public List<Snapshot> snapshots() throws IOException {
        return snapshotManager.listAllSnapshots();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return snapshotManager;
    }

    @Override
    public PathFactory pathFactory() {
        return pathFactory;
    }

    @Override
    public PartitionManager partitionManager() {
        return partitionManager;
    }
    
    @Override
    public BranchManager branchManager() {
        SchemaManager schemaManager = new SchemaManager(
            pathFactory,
            identifier.getDatabase(),
            identifier.getTable()
        );
        
        return new FileSystemBranchManager(
            pathFactory,
            identifier.getDatabase(),
            identifier.getTable(),
            snapshotManager,
            schemaManager
        );
    }
    
    @Override
    public Table switchToBranch(String branchName) {
        // 规范化分支名
        String currentBranch = BranchManager.normalizeBranch(pathFactory.getBranch());
        String targetBranch = BranchManager.normalizeBranch(branchName);
        
        // 如果已经在目标分支，直接返回
        if (currentBranch.equals(targetBranch)) {
            return this;
        }
        
        // 加载目标分支的 Schema
        SchemaManager branchSchemaManager = new SchemaManager(
            pathFactory.copyWithBranch(targetBranch),
            identifier.getDatabase(),
            identifier.getTable(),
            targetBranch
        );
        
        Schema branchSchema;
        try {
            branchSchema = branchSchemaManager.getCurrentSchema();
        } catch (IOException e) {
            throw new IllegalArgumentException("Branch '" + targetBranch + "' does not exist", e);
        }
        
        // 创建新的 PathFactory 使用目标分支
        PathFactory branchPathFactory = pathFactory.copyWithBranch(targetBranch);
        
        // 创建并返回新的 FileStoreTable 实例
        return new FileStoreTable(catalog, identifier, branchSchema, branchPathFactory);
    }

    public Catalog catalog() {
        return catalog;
    }
}
