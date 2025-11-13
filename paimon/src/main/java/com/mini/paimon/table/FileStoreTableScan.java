package com.mini.paimon.table;

import com.mini.paimon.filter.PartitionFilter;
import com.mini.paimon.filter.PartitionPredicate;
import com.mini.paimon.manifest.ManifestCacheManager;
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
 * 支持分区过滤、Manifest缓存、增量读取
 */
public class FileStoreTableScan implements TableScan {
    private static final Logger logger = LoggerFactory.getLogger(FileStoreTableScan.class);
    
    private final FileStoreTable table;
    private Long specifiedSnapshotId;
    private boolean useLatest = true;
    
    // 分区过滤条件（统一使用 PartitionPredicate ）
    private PartitionPredicate partitionPredicate;
    
    // Manifest缓存管理器
    private static final ManifestCacheManager cacheManager = new ManifestCacheManager();
    
    // 增量读取配置
    private Long baseSnapshotId;
    private boolean useIncrementalRead = false;
    
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
        // 将简单的 PartitionSpec 转换为 PartitionPredicate
        if (partitionSpec != null && !partitionSpec.isEmpty()) {
            PartitionPredicate pred = null;
            for (java.util.Map.Entry<String, String> entry : partitionSpec.getPartitionValues().entrySet()) {
                PartitionPredicate eqPred = PartitionPredicate.equal(entry.getKey(), entry.getValue());
                pred = (pred == null) ? eqPred : pred.and(eqPred);
            }
            this.partitionPredicate = pred;
        }
        return this;
    }
    
    /**
     * 设置分区谓词（支持复杂过滤条件）
     */
    public TableScan withPartitionPredicate(PartitionPredicate predicate) {
        this.partitionPredicate = predicate;
        return this;
    }
    
    /**
     * 启用增量读取模式
     */
    public TableScan withIncrementalRead(long baseSnapshotId) {
        this.baseSnapshotId = baseSnapshotId;
        this.useIncrementalRead = true;
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
        
        // 应用分区过滤（统一使用 PartitionPredicate）
        if (partitionPredicate != null) {
            files = filterByPartitionPredicate(files);
        }
        
        logger.debug("Scanned snapshot {} with {} files", 
            snapshot.getSnapshotId(), files.size());
        
        return new PlanImpl(snapshot, files);
    }
    
    private List<ManifestEntry> readManifestFiles(Snapshot snapshot) throws IOException {
        // 使用增量读取模式
        if (useIncrementalRead && baseSnapshotId != null) {
            return readIncrementalManifest(snapshot);
        }
        
        // 增量模式：合并 base + delta manifest
        return readBasePlusDeltaManifests(snapshot);
    }
    
    /**
     * 读取 base + delta manifests 并合并
     * 这是增量写入模式下的标准读取流程
     */
    private List<ManifestEntry> readBasePlusDeltaManifests(Snapshot snapshot) throws IOException {
        java.util.Map<String, ManifestEntry> fileStateMap = new java.util.LinkedHashMap<>();
        
        // 1. 读取 base manifest（如果存在）
        String baseManifestListName = snapshot.getBaseManifestList();
        if (baseManifestListName != null && !baseManifestListName.isEmpty()) {
            long baseSnapshotId = extractSnapshotIdFromManifestList(baseManifestListName);
            if (baseSnapshotId > 0) {
                ManifestList baseList = cacheManager.getManifestList(
                    table.pathFactory(),
                    table.identifier().getDatabase(),
                    table.identifier().getTable(),
                    baseSnapshotId
                );
                mergeManifestListIntoMap(baseList, fileStateMap);
                logger.debug("Loaded base manifest for snapshot {}, {} files", 
                            baseSnapshotId, fileStateMap.size());
            }
        }
        
        // 2. 读取 delta manifest（如果存在）
        String deltaManifestListName = snapshot.getDeltaManifestList();
        if (deltaManifestListName != null && !deltaManifestListName.isEmpty()) {
            long deltaSnapshotId = snapshot.getId();
            ManifestList deltaList = cacheManager.getManifestList(
            table.pathFactory(),
            table.identifier().getDatabase(),
            table.identifier().getTable(),
                deltaSnapshotId
            );
            mergeManifestListIntoMap(deltaList, fileStateMap);
            logger.debug("Merged delta manifest for snapshot {}, {} files now", 
                        deltaSnapshotId, fileStateMap.size());
        }
        
        // 3. 返回所有活跃文件
        return new ArrayList<>(fileStateMap.values());
    }
    
    /**
     * 将 ManifestList 中的条目合并到文件状态映射中
     */
    private void mergeManifestListIntoMap(ManifestList manifestList, 
                                         java.util.Map<String, ManifestEntry> fileStateMap) 
            throws IOException {
        for (ManifestFileMeta meta : manifestList.getManifestFiles()) {
            String manifestId = meta.getFileName();
            if (manifestId.startsWith("manifest-")) {
                manifestId = manifestId.substring("manifest-".length());
            }
            
            ManifestFile manifestFile = cacheManager.getManifestFile(
                table.pathFactory(),
                table.identifier().getDatabase(),
                table.identifier().getTable(),
                manifestId
            );
            
            for (ManifestEntry entry : manifestFile.getEntries()) {
                String fileName = entry.getFile().getFileName();
                if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                    fileStateMap.put(fileName, entry);
                } else if (entry.getKind() == ManifestEntry.FileKind.DELETE) {
                    fileStateMap.remove(fileName);
                }
            }
        }
    }
    
    /**
     * 从 manifest list 文件名中提取快照 ID
     */
    private long extractSnapshotIdFromManifestList(String manifestListName) {
        try {
            if (manifestListName.startsWith("manifest-list-delta-")) {
                return Long.parseLong(manifestListName.substring("manifest-list-delta-".length()));
            } else if (manifestListName.startsWith("manifest-list-base-")) {
                return Long.parseLong(manifestListName.substring("manifest-list-base-".length()));
            } else if (manifestListName.startsWith("manifest-list-")) {
                return Long.parseLong(manifestListName.substring("manifest-list-".length()));
            }
        } catch (NumberFormatException e) {
            logger.warn("Failed to extract snapshot ID from manifest list: {}", manifestListName, e);
        }
        return -1;
    }
    
    /**
     * 增量读取Manifest变更
     */
    private List<ManifestEntry> readIncrementalManifest(Snapshot snapshot) throws IOException {
        ManifestCacheManager.IncrementalManifest incremental = 
            cacheManager.readIncrementalManifest(
                table.pathFactory(),
                table.identifier().getDatabase(),
                table.identifier().getTable(),
                baseSnapshotId,
                snapshot.getId()
            );
        
        logger.info("Incremental read: {} new entries, {} deleted entries",
                   incremental.getNewEntries().size(),
                   incremental.getDeletedEntries().size());
        
        return incremental.getNewEntries();
    }
    

    
    /**
     * 使用分区谓词过滤文件
     */
    private List<ManifestEntry> filterByPartitionPredicate(List<ManifestEntry> entries) {
        List<ManifestEntry> filtered = new ArrayList<>();
        
        for (ManifestEntry entry : entries) {
            PartitionSpec partition = extractPartitionFromPath(entry.getFileName());
            if (partition != null && partitionPredicate.test(partition)) {
                filtered.add(entry);
            }
        }
        
        logger.info("Partition predicate filter: {} files matched out of {} (predicate: {})",
                   filtered.size(), entries.size(), partitionPredicate);
        
        return filtered;
    }
    
    /**
     * 从文件路径提取分区信息
     */
    private PartitionSpec extractPartitionFromPath(String filePath) {
        try {
            // 解析路径中的分区信息，例如：dt=2024-01-01/hour=10/bucket-0/data-0-000.sst
            String[] parts = filePath.split("/");
            java.util.Map<String, String> partitionValues = new java.util.LinkedHashMap<>();
            
            for (String part : parts) {
                if (part.contains("=") && !part.startsWith("bucket-")) {
                    String[] kv = part.split("=", 2);
                    if (kv.length == 2) {
                        partitionValues.put(kv[0], kv[1]);
                    }
                }
            }
            
            return partitionValues.isEmpty() ? null : new PartitionSpec(partitionValues);
        } catch (Exception e) {
            logger.warn("Failed to extract partition from path: {}", filePath, e);
            return null;
        }
    }
    
    /**
     * 清理Manifest缓存
     */
    public static void clearManifestCache() {
        cacheManager.clearAll();
    }
    
    /**
     * 获取缓存统计信息
     */
    public static ManifestCacheManager.CacheStats getCacheStats() {
        return cacheManager.getStats();
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
