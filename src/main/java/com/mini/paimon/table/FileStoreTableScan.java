package com.mini.paimon.table;

import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.manifest.ManifestFile;
import com.mini.paimon.manifest.ManifestFileMeta;
import com.mini.paimon.manifest.ManifestList;
import com.mini.paimon.partition.PartitionSpec;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * FileStoreTable 扫描器实现
 * 支持分区过滤
 */
public class FileStoreTableScan implements TableScan {
    private static final Logger logger = LoggerFactory.getLogger(FileStoreTableScan.class);
    
    private final FileStoreTable table;
    private Long specifiedSnapshotId;
    private boolean useLatest = true;
    private PartitionSpec partitionFilter;
    
    public FileStoreTableScan(FileStoreTable table) {
        this.table = table;
    }
    
    @Override
    public TableScan withSnapshot(long snapshotId) {
        this.specifiedSnapshotId = snapshotId;
        this.useLatest = false;
        return this;
    }
    
    @Override
    public TableScan withLatestSnapshot() {
        this.useLatest = true;
        this.specifiedSnapshotId = null;
        return this;
    }
    
    @Override
    public TableScan withPartitionFilter(PartitionSpec partitionSpec) {
        this.partitionFilter = partitionSpec;
        return this;
    }
    
    @Override
    public Plan plan() throws IOException {
        SnapshotManager snapshotManager = table.snapshotManager();
        
        // 确定要扫描的快照
        Snapshot snapshot;
        if (specifiedSnapshotId != null) {
            snapshot = snapshotManager.getSnapshot(specifiedSnapshotId);
            if (snapshot == null) {
                throw new IOException("Snapshot not found: " + specifiedSnapshotId);
            }
        } else if (useLatest) {
            if (!snapshotManager.hasSnapshot()) {
                // 没有快照，返回空计划
                return new PlanImpl(null, Collections.emptyList());
            }
            snapshot = snapshotManager.getLatestSnapshot();
        } else {
            throw new IllegalStateException("No snapshot specified");
        }
        
        // 读取 Manifest 文件获取数据文件列表
        List<ManifestEntry> files = readManifestFiles(snapshot);
        
        // 应用分区过滤
        if (partitionFilter != null) {
            files = filterByPartition(files);
        }
        
        logger.debug("Scanned snapshot {} with {} files", 
            snapshot.getSnapshotId(), files.size());
        
        return new PlanImpl(snapshot, files);
    }
    
    private List<ManifestEntry> readManifestFiles(Snapshot snapshot) throws IOException {
        // 从 baseManifestList 读取所有 Manifest 文件
        String manifestListFile = snapshot.getBaseManifestList();
        if (manifestListFile == null || manifestListFile.isEmpty()) {
            return Collections.emptyList();
        }
        
        // 加载 ManifestList
        ManifestList manifestList = ManifestList.load(
            table.pathFactory(),
            table.identifier().getDatabase(),
            table.identifier().getTable(),
            snapshot.getId()
        );
        
        List<ManifestEntry> allEntries = new ArrayList<>();
        for (ManifestFileMeta fileMeta : manifestList.getManifestFiles()) {
            // 加载每个 ManifestFile
            // fileMeta.getFileName() 已经包含 "manifest-" 前缀
            ManifestFile manifestFile = ManifestFile.load(
                table.pathFactory(),
                table.identifier().getDatabase(),
                table.identifier().getTable(),
                fileMeta.getFileName()
            );
            allEntries.addAll(manifestFile.getEntries());
        }
        
        // 过滤掉被删除的文件
        List<ManifestEntry> activeFiles = new ArrayList<>();
        for (ManifestEntry entry : allEntries) {
            if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                activeFiles.add(entry);
            }
        }
        
        return activeFiles;
    }
    
    /**
     * 按分区过滤文件
     */
    private List<ManifestEntry> filterByPartition(List<ManifestEntry> entries) {
        if (partitionFilter == null || partitionFilter.isEmpty()) {
            return entries;
        }
        
        List<ManifestEntry> filtered = new ArrayList<>();
        String partitionPath = partitionFilter.toPath();
        
        for (ManifestEntry entry : entries) {
            // 检查文件路径是否包含分区
            if (entry.getFileName().contains(partitionPath)) {
                filtered.add(entry);
            }
        }
        
        logger.debug("Filtered {} files to {} files for partition {}", 
            entries.size(), filtered.size(), partitionPath);
        
        return filtered;
    }
    
    /**
     * 扫描计划实现
     */
    private static class PlanImpl implements Plan {
        private final Snapshot snapshot;
        private final List<ManifestEntry> files;
        
        public PlanImpl(Snapshot snapshot, List<ManifestEntry> files) {
            this.snapshot = snapshot;
            this.files = files;
        }
        
        @Override
        public Snapshot snapshot() {
            return snapshot;
        }
        
        @Override
        public List<ManifestEntry> files() {
            return files;
        }
    }
}
