package com.mini.paimon.table;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.partition.PartitionManager;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
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
        
        logger.debug("Created FileStoreTable for {}", identifier);
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
        // 使用一个简单的适配器
        return new SimpleTableRead(schema, pathFactory, identifier);
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
    public TableCommit newCommit() {
        return new TableCommit(catalog, pathFactory, identifier);
    }
    
    @Override
    public Optional<Snapshot> latestSnapshot() {
        try {
            if (!snapshotManager.hasSnapshot()) {
                return Optional.empty();
            }
            return Optional.of(snapshotManager.getLatestSnapshot());
        } catch (IOException e) {
            logger.error("Failed to get latest snapshot", e);
            return Optional.empty();
        }
    }
    
    @Override
    public Optional<Snapshot> snapshot(long snapshotId) {
        try {
            Snapshot snapshot = snapshotManager.getSnapshot(snapshotId);
            return Optional.ofNullable(snapshot);
        } catch (IOException e) {
            logger.error("Failed to get snapshot {}", snapshotId, e);
            return Optional.empty();
        }
    }
    
    @Override
    public List<Snapshot> snapshots() throws IOException {
        return snapshotManager.getAllSnapshots();
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
    
    public Catalog catalog() {
        return catalog;
    }
}
