package com.mini.paimon.table;

import com.mini.paimon.filter.PartitionPredicate;
import com.mini.paimon.index.IndexFileManager;
import com.mini.paimon.index.IndexSelector;
import com.mini.paimon.manifest.DataFileMeta;
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
    
    // 行级过滤条件
    private Predicate rowFilter;
    
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
    
    @Override
    public TableScan withFilter(Predicate predicate) {
        this.rowFilter = predicate;
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
            snapshot = snapshotManager.snapshot(specifiedSnapshotId);
            if (snapshot == null) {
                throw new IOException("Snapshot not found: " + specifiedSnapshotId);
            }
        } else if (useLatest) {
            if (!snapshotManager.hasSnapshot()) {
                // 没有快照，返回空计划
                return new PlanImpl(null, Collections.emptyList());
            }
            snapshot = snapshotManager.latestSnapshot();
        } else {
            throw new IllegalStateException("No snapshot specified");
        }
        
        // 读取 Manifest 文件获取数据文件列表
        List<ManifestEntry> files = readManifestFiles(snapshot);
        
        // 应用分区过滤（统一使用 PartitionPredicate）
        if (partitionPredicate != null) {
            files = filterByPartitionPredicate(files);
        }
        
        // 应用行级过滤（使用索引加速）
        if (rowFilter != null) {
            files = filterWithIndex(files);
        }
        
        logger.debug("Scanned snapshot {} with {} files", 
            snapshot.getId(), files.size());
        
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
     * 
     * 在增量模式下：
     * - base manifest list 可能指向一个compacted的base，或者第一个快照的delta
     * - 需要读取从base snapshot到当前snapshot之间的所有delta manifests
     */
    private List<ManifestEntry> readBasePlusDeltaManifests(Snapshot snapshot) throws IOException {
        java.util.Map<String, ManifestEntry> fileStateMap = new java.util.LinkedHashMap<>();
        
        // 1. 读取 base manifest（如果存在）
        String baseManifestListName = snapshot.getBaseManifestList();
        String deltaManifestListName = snapshot.getDeltaManifestList();
        long currentSnapshotId = snapshot.getId();
        
        logger.debug("Reading manifests for snapshot {}: base={}, delta={}", 
                    currentSnapshotId, baseManifestListName, deltaManifestListName);
        
        // 如果 base 和 delta 是同一个 manifest list（首次提交），只需要读取一次
        boolean baseAndDeltaSame = baseManifestListName != null && 
                                   baseManifestListName.equals(deltaManifestListName);
        
        if (baseManifestListName != null && !baseManifestListName.isEmpty() && !baseAndDeltaSame) {
            long baseSnapshotId = extractSnapshotIdFromManifestList(baseManifestListName);
            logger.debug("Extracted base snapshot ID: {} from manifest list name: {}", 
                        baseSnapshotId, baseManifestListName);
            if (baseSnapshotId > 0) {
                try {
                    // 检查base manifest list是否是compacted的base（manifest-list-base-X格式）
                    boolean isCompactedBase = baseManifestListName.startsWith("manifest-list-base-");
                    
                    if (isCompactedBase) {
                        // 如果是compacted base，直接读取base，然后读取从baseSnapshotId+1到currentSnapshotId-1的所有delta manifests
                        List<ManifestFileMeta> baseMetas = cacheManager.getBaseManifestList(
                            table.pathFactory(),
                            table.identifier().getDatabase(),
                            table.identifier().getTable(),
                            baseSnapshotId
                        );
                        ManifestList baseList = new ManifestList(baseMetas);
                        mergeManifestListIntoMap(baseList, fileStateMap);
                        logger.info("Loaded compacted base manifest for snapshot {} (from baseSnapshotId {}), {} files", 
                                  currentSnapshotId, baseSnapshotId, fileStateMap.size());
                        
                        // 读取从baseSnapshotId+1到currentSnapshotId-1的所有delta manifests
                        if (baseSnapshotId < currentSnapshotId - 1) {
                            logger.debug("Reading delta manifests from {} to {} for compacted base", 
                                        baseSnapshotId + 1, currentSnapshotId - 1);
                            for (long snapId = baseSnapshotId + 1; snapId < currentSnapshotId; snapId++) {
                                try {
                                    List<ManifestFileMeta> deltaMetas = cacheManager.getDeltaManifestList(
                                        table.pathFactory(),
                                        table.identifier().getDatabase(),
                                        table.identifier().getTable(),
                                        snapId
                                    );
                                    ManifestList deltaList = new ManifestList(deltaMetas);
                                    mergeManifestListIntoMap(deltaList, fileStateMap);
                                    logger.debug("Merged delta manifest from snapshot {}, {} files now", 
                                               snapId, fileStateMap.size());
                                } catch (IOException e) {
                                    logger.warn("Failed to load delta manifest for snapshot {}: {}", snapId, e.getMessage());
                                    // 继续处理下一个snapshot
                                }
                            }
                            logger.info("Loaded all delta manifests from {} to {} for compacted base, {} files now", 
                                      baseSnapshotId + 1, currentSnapshotId - 1, fileStateMap.size());
                        }
                    } else {
                        // 如果是delta格式的base（manifest-list-delta-X），需要读取从baseSnapshotId到currentSnapshotId-1的所有delta manifests
                        logger.debug("Base is delta format, reading all delta manifests from {} to {}", 
                                    baseSnapshotId, currentSnapshotId - 1);
                        for (long snapId = baseSnapshotId; snapId < currentSnapshotId; snapId++) {
                            try {
                                List<ManifestFileMeta> deltaMetas = cacheManager.getDeltaManifestList(
                                    table.pathFactory(),
                                    table.identifier().getDatabase(),
                                    table.identifier().getTable(),
                                    snapId
                                );
                                ManifestList deltaList = new ManifestList(deltaMetas);
                                mergeManifestListIntoMap(deltaList, fileStateMap);
                                logger.debug("Merged delta manifest from snapshot {}, {} files now", 
                                           snapId, fileStateMap.size());
                            } catch (IOException e) {
                                logger.warn("Failed to load delta manifest for snapshot {}: {}", snapId, e.getMessage());
                                // 继续处理下一个snapshot
                            }
                        }
                        logger.info("Loaded all delta manifests from {} to {} for snapshot {}, {} files", 
                                  baseSnapshotId, currentSnapshotId - 1, currentSnapshotId, fileStateMap.size());
                    }
                } catch (IOException e) {
                    logger.error("Failed to load base manifest list {} (baseSnapshotId={}): {}", 
                                baseManifestListName, baseSnapshotId, e.getMessage(), e);
                    // 如果base manifest加载失败，继续尝试加载delta
                }
            } else {
                logger.warn("Failed to extract base snapshot ID from manifest list name: {}", baseManifestListName);
            }
        } else if (baseAndDeltaSame) {
            logger.debug("Base and delta are the same for snapshot {}, will only read delta", currentSnapshotId);
        }
        
        // 2. 读取当前快照的 delta manifest（如果存在）
        if (deltaManifestListName != null && !deltaManifestListName.isEmpty()) {
            long deltaSnapshotId = currentSnapshotId;
            try {
                List<ManifestFileMeta> deltaMetas = cacheManager.getDeltaManifestList(
                    table.pathFactory(),
                    table.identifier().getDatabase(),
                    table.identifier().getTable(),
                    deltaSnapshotId
                );
                ManifestList deltaList = new ManifestList(deltaMetas);
                mergeManifestListIntoMap(deltaList, fileStateMap);
                logger.info("Merged delta manifest for snapshot {}, {} files now", 
                           deltaSnapshotId, fileStateMap.size());
            } catch (IOException e) {
                logger.error("Failed to load delta manifest list for snapshot {}: {}", 
                            deltaSnapshotId, e.getMessage(), e);
                throw e;
            }
        }
        
        // 3. 返回所有活跃文件
        logger.info("Final file count for snapshot {}: {} files", currentSnapshotId, fileStateMap.size());
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
            // 确保去掉"manifest-"前缀，因为getManifestFile会处理前缀
            if (manifestId.startsWith("manifest-")) {
                manifestId = manifestId.substring("manifest-".length());
            }
            
            try {
                List<ManifestEntry> entries = cacheManager.getManifestFile(
                    table.pathFactory(),
                    table.identifier().getDatabase(),
                    table.identifier().getTable(),
                    manifestId
                );
                
                for (ManifestEntry entry : entries) {
                    String fileName = entry.getFile().getFileName();
                    if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                        fileStateMap.put(fileName, entry);
                    } else if (entry.getKind() == ManifestEntry.FileKind.DELETE) {
                        fileStateMap.remove(fileName);
                    }
                }
            } catch (IOException e) {
                logger.warn("Failed to load manifest file {}: {}, skipping", manifestId, e.getMessage());
                // 如果manifest文件不存在，可能是已经被删除或清理，跳过这个manifest
                // 这在增量模式下是正常的，因为旧的manifest可能已经被compaction清理
                continue;
            }
        }
    }
    
    /**
     * 从 manifest list 文件名中提取快照 ID
     */
    private long extractSnapshotIdFromManifestList(String manifestListName) {
        if (manifestListName == null || manifestListName.isEmpty()) {
            return -1;
        }
        try {
            // 处理 "manifest-list-delta-X" 格式
            if (manifestListName.startsWith("manifest-list-delta-")) {
                String idStr = manifestListName.substring("manifest-list-delta-".length());
                return Long.parseLong(idStr);
            } 
            // 处理 "manifest-list-base-X" 格式
            else if (manifestListName.startsWith("manifest-list-base-")) {
                String idStr = manifestListName.substring("manifest-list-base-".length());
                return Long.parseLong(idStr);
            } 
            // 处理 "manifest-list-X" 格式（兼容旧格式）
            else if (manifestListName.startsWith("manifest-list-")) {
                String idStr = manifestListName.substring("manifest-list-".length());
                return Long.parseLong(idStr);
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
     * 使用索引过滤数据文件
     */
    private List<ManifestEntry> filterWithIndex(List<ManifestEntry> entries) throws IOException {
        if (entries.isEmpty()) {
            return entries;
        }
        
        IndexFileManager indexFileManager = new IndexFileManager(table.pathFactory());
        IndexSelector indexSelector = new IndexSelector(
            indexFileManager,
            table.identifier().getDatabase(),
            table.identifier().getTable()
        );
        
        List<DataFileMeta> dataFileMetas = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            dataFileMetas.add(entry.getFile());
        }
        
        List<DataFileMeta> filtered = indexSelector.filterWithIndex(dataFileMetas, rowFilter);
        
        List<ManifestEntry> result = new ArrayList<>();
        for (DataFileMeta meta : filtered) {
            result.add(new ManifestEntry(ManifestEntry.FileKind.ADD, 0, meta));
        }
        
        logger.info("Index filter: {} files matched out of {} (filter: {})",
                   result.size(), entries.size(), rowFilter);
        
        return result;
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
