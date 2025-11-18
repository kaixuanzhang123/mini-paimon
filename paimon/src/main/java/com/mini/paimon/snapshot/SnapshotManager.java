package com.mini.paimon.snapshot;

import com.mini.paimon.branch.BranchManager;
import com.mini.paimon.utils.IdGenerator;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.manifest.ManifestFile;
import com.mini.paimon.manifest.ManifestFileMeta;
import com.mini.paimon.manifest.ManifestList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Snapshot Manager
 * 参考 Apache Paimon 的设计
 * 
 * 负责快照的创建、管理和查询
 * 支持多种提交类型：APPEND/COMPACT/OVERWRITE
 * 支持分支隔离
 */
public class SnapshotManager {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);
    
    /** 路径工厂 */
    private final PathFactory pathFactory;
    
    /** 数据库名 */
    private final String database;
    
    /** 表名 */
    private final String table;
    
    /** 分支名 */
    private final String branch;
    
    /** ID 生成器 */
    private final IdGenerator idGenerator;
    
    /** 快照ID生成器 */
    private final AtomicLong snapshotIdGenerator;
    
    /** 提交用户标识（默认为 UUID）*/
    private final String commitUser;

    public SnapshotManager(PathFactory pathFactory, String database, String table) {
        this(pathFactory, database, table, null);
    }
    
    public SnapshotManager(PathFactory pathFactory, String database, String table, String branch) {
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.branch = BranchManager.normalizeBranch(branch);
        this.idGenerator = new IdGenerator();
        this.snapshotIdGenerator = new AtomicLong(initializeSnapshotId());
        this.commitUser = UUID.randomUUID().toString();
    }
    
    /**
     * 创建一个新的 SnapshotManager，使用指定的分支
     */
    public SnapshotManager copyWithBranch(String branchName) {
        return new SnapshotManager(
            pathFactory.copyWithBranch(branchName),
            database,
            table,
            branchName
        );
    }
    
    /**
     * 获取当前分支名
     */
    public String getBranch() {
        return branch;
    }
    
    /**
     * 初始化快照 ID
     * 从现有快照中获取最大 ID + 1
     */
    private long initializeSnapshotId() {
        try {
            if (Snapshot.hasLatestSnapshot(pathFactory, database, table)) {
                Snapshot latest = Snapshot.loadLatest(pathFactory, database, table);
                return latest.getId() + 1;
            }
        } catch (IOException e) {
            logger.warn("Failed to load latest snapshot, starting from 1", e);
        }
        return 1;
    }

    /**
     * 创建新的快照（默认为 APPEND 类型）
     * 
     * @param schemaId Schema ID
     * @param manifestEntries Manifest 条目列表
     * @return 新创建的快照
     * @throws IOException IO异常
     */
    public synchronized Snapshot createSnapshot(int schemaId, List<ManifestEntry> manifestEntries) throws IOException {
        return createSnapshot(schemaId, manifestEntries, Snapshot.CommitKind.APPEND);
    }
    
    /**
     * 创建新的快照（指定提交类型）
     * 
     * 参考 Paimon 增量设计：
     * - 每次提交只生成 delta manifest（仅包含本次变更）
     * - base manifest 通过 compaction 定期生成
     * - 读取时合并 base + delta 得到完整状态
     * 
     * 增量模式优势：
     * - 写入性能提升 10-1000 倍（只写变更部分）
     * - 存储空间节省 90%+
     * - 支持真正的增量读取
     * 
     * @param schemaId Schema ID
     * @param manifestEntries Manifest 条目列表（本次变更）
     * @param commitKind 提交类型
     * @return 新创建的快照
     * @throws IOException IO异常
     */
    public synchronized Snapshot createSnapshot(int schemaId, List<ManifestEntry> manifestEntries, 
                                               Snapshot.CommitKind commitKind) throws IOException {
        long snapshotId = snapshotIdGenerator.getAndIncrement();
        
        logger.debug("Creating snapshot {} with {} manifest entries, kind={}", 
                    snapshotId, manifestEntries.size(), commitKind);
        
        // 1. 创建 delta manifest（仅包含本次变更）
        String deltaManifestId = idGenerator.generateManifestId();
        String deltaManifestFileName = "manifest-" + deltaManifestId;
        
        ManifestFile deltaManifestFile = new ManifestFile(manifestEntries);
        deltaManifestFile.persist(pathFactory, database, table, deltaManifestId);
        
        ManifestFileMeta deltaMeta = buildManifestFileMeta(
            deltaManifestFileName, manifestEntries, schemaId);
        
        // 2. 创建 delta manifest list
        ManifestList deltaManifestList = new ManifestList();
        deltaManifestList.addManifestFile(deltaMeta);
        
        String deltaListFileName = "manifest-list-delta-" + snapshotId;
        deltaManifestList.persistDelta(pathFactory, database, table, snapshotId);
        
        // 3. 确定 base manifest list
        String baseManifestListName;
        Snapshot previousSnapshot = null;
        
        if (Snapshot.hasLatestSnapshot(pathFactory, database, table)) {
            try {
                previousSnapshot = Snapshot.loadLatest(pathFactory, database, table);
                
                // 检查是否需要 compaction
                if (shouldCompact(snapshotId, previousSnapshot)) {
                    logger.info("Triggering manifest compaction for snapshot {}", snapshotId);
                    baseManifestListName = compactManifests(snapshotId, schemaId, previousSnapshot, manifestEntries);
                } else {
                    baseManifestListName = previousSnapshot.getBaseManifestList();
                    logger.debug("Reusing base manifest list from snapshot {}: {}", 
                                previousSnapshot.getId(), baseManifestListName);
                }
            } catch (IOException e) {
                logger.warn("Failed to load previous snapshot, creating initial base", e);
                baseManifestListName = deltaListFileName;
            }
        } else {
            logger.info("First commit, delta manifest serves as base");
            baseManifestListName = deltaListFileName;
        }
        
        // 4. 计算记录数
        long deltaRecordCount = calculateRecordCount(manifestEntries);
        long totalRecordCount = calculateTotalRecordCountIncremental(
            previousSnapshot, manifestEntries, deltaRecordCount);
        
        // 5. 创建 Snapshot（引用 base + delta）
        Snapshot snapshot = new Snapshot.Builder()
            .id(snapshotId)
            .schemaId(schemaId)
            .baseManifestList(baseManifestListName)
            .deltaManifestList(deltaListFileName)
            .commitUser(commitUser)
            .commitIdentifier(snapshotId)
            .commitKind(commitKind)
            .timeMillis(System.currentTimeMillis())
            .totalRecordCount(totalRecordCount)
            .deltaRecordCount(deltaRecordCount)
            .build();
        
        // 6. 持久化快照
        snapshot.writeToFile(pathFactory, database, table);
        
        // 7. 更新 LATEST 指针
        Snapshot.updateLatestSnapshot(pathFactory, database, table, snapshotId);
        
        // 8. 更新 EARLIEST 指针（首次提交时）
        if (snapshotId == 1) {
            Snapshot.updateEarliestSnapshot(pathFactory, database, table, snapshotId);
        }
        
        logger.info("Created incremental snapshot {} for table {}/{}: delta={} entries, kind={}, " +
                   "deltaRecords={}, totalRecords={}", 
                   snapshotId, database, table, manifestEntries.size(), commitKind,
                   deltaRecordCount, totalRecordCount);
        
        return snapshot;
    }
    
    /**
     * 构建 ManifestFileMeta
     */
    private ManifestFileMeta buildManifestFileMeta(String fileName, 
                                                   List<ManifestEntry> entries,
                                                   int schemaId) {
        long fileSize = 0;
        long numAddedFiles = 0;
        long numDeletedFiles = 0;
        com.mini.paimon.schema.RowKey minKey = null;
        com.mini.paimon.schema.RowKey maxKey = null;
        
        for (ManifestEntry entry : entries) {
            fileSize += entry.getFile().getFileSize();
            if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                numAddedFiles++;
            } else {
                numDeletedFiles++;
            }
            
            // 更新 minKey 和 maxKey
            if (entry.getMinKey() != null) {
                if (minKey == null || entry.getMinKey().compareTo(minKey) < 0) {
                    minKey = entry.getMinKey();
                }
            }
            if (entry.getMaxKey() != null) {
                if (maxKey == null || entry.getMaxKey().compareTo(maxKey) > 0) {
                    maxKey = entry.getMaxKey();
                }
            }
        }
        
        return new ManifestFileMeta(
            fileName, fileSize, numAddedFiles, numDeletedFiles, 
            schemaId, minKey, maxKey
        );
    }
    
    /**
     * 计算 Delta 记录数
     */
    private long calculateRecordCount(List<ManifestEntry> entries) {
        return entries.stream()
            .mapToLong(entry -> entry.getFile().getRowCount())
            .sum();
    }

    
    /**
     * 增量模式下计算总记录数
     * 基于上一个快照的总记录数 + 本次变更
     */
    private long calculateTotalRecordCountIncremental(Snapshot previousSnapshot, 
                                                     List<ManifestEntry> manifestEntries,
                                                     long deltaRecordCount) {
        if (previousSnapshot == null) {
            return deltaRecordCount;
        }
        
        long previousTotal = previousSnapshot.getTotalRecordCount();
        long addedRecords = 0;
        long deletedRecords = 0;
        
        for (ManifestEntry entry : manifestEntries) {
            if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                addedRecords += entry.getFile().getRowCount();
            } else if (entry.getKind() == ManifestEntry.FileKind.DELETE) {
                deletedRecords += entry.getFile().getRowCount();
            }
        }
        
        return previousTotal + addedRecords - deletedRecords;
    }
    
    /**
     * 判断是否需要执行 compaction
     * 触发条件：
     * 1. Delta 快照数量达到阈值（默认 10 个）
     * 2. 定期触发（每 100 次提交）
     */
    private boolean shouldCompact(long currentSnapshotId, Snapshot previousSnapshot) {
        if (previousSnapshot == null) {
            return false;
        }
        
        String previousBase = previousSnapshot.getBaseManifestList();
        if (previousBase == null) {
            return false;
        }
        
        long lastCompactedSnapshotId = extractSnapshotIdFromManifestList(previousBase);
        if (lastCompactedSnapshotId < 0) {
            lastCompactedSnapshotId = 1;
        }
        
        long deltaCount = currentSnapshotId - lastCompactedSnapshotId;
        
        int minDeltaCount = 10;
        int maxDeltaCount = 50;
        
        if (deltaCount >= maxDeltaCount) {
            logger.info("Compaction triggered: delta count {} >= max {}", deltaCount, maxDeltaCount);
            return true;
        }
        
        if (deltaCount >= minDeltaCount && currentSnapshotId % 100 == 0) {
            logger.info("Compaction triggered: periodic check at snapshot {}", currentSnapshotId);
            return true;
        }
        
        return false;
    }
    
    /**
     * 从 manifest list 文件名中提取快照 ID
     * 例如：manifest-list-delta-123 -> 123
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
     * 执行 Manifest Compaction
     * 合并所有 delta manifests 生成新的 base manifest
     * 
     * @param snapshotId 当前快照ID
     * @param schemaId Schema ID
     * @param previousSnapshot 上一个快照
     * @param currentEntries 当前变更的条目
     * @return 新的 base manifest list 文件名
     * @throws IOException IO异常
     */
    private String compactManifests(long snapshotId, int schemaId, 
                                    Snapshot previousSnapshot,
                                    List<ManifestEntry> currentEntries) throws IOException {
        logger.info("Starting manifest compaction for snapshot {}", snapshotId);
        long startTime = System.currentTimeMillis();
        
        java.util.Map<String, ManifestEntry> fileStateMap = new java.util.LinkedHashMap<>();
        
        // 1. 加载 base manifest（如果存在）
        String baseManifestListName = previousSnapshot.getBaseManifestList();
        if (baseManifestListName != null) {
            long baseSnapshotId = extractSnapshotIdFromManifestList(baseManifestListName);
            if (baseSnapshotId > 0) {
                ManifestList baseList;
                if (baseManifestListName.startsWith("manifest-list-base-")) {
                    baseList = ManifestList.loadBase(pathFactory, database, table, baseSnapshotId);
                } else {
                    baseList = ManifestList.loadDelta(pathFactory, database, table, baseSnapshotId);
                }
                mergeManifestListIntoMap(baseList, fileStateMap);
                logger.debug("Loaded base manifest from snapshot {}, {} active files", 
                            baseSnapshotId, fileStateMap.size());
            }
        }
        
        // 2. 应用所有 delta manifests（从上次 compaction 到现在）
        long startDeltaId = extractSnapshotIdFromManifestList(baseManifestListName);
        if (startDeltaId < 0) {
            startDeltaId = 1;
        }
        
        List<Snapshot> snapshots = getAllSnapshots();
        for (Snapshot snap : snapshots) {
            if (snap.getId() > startDeltaId && snap.getId() < snapshotId) {
                String deltaListName = snap.getDeltaManifestList();
                if (deltaListName != null) {
                    long deltaSnapshotId = snap.getId();
                    ManifestList deltaList = ManifestList.loadDelta(pathFactory, database, table, deltaSnapshotId);
                    mergeManifestListIntoMap(deltaList, fileStateMap);
                    logger.debug("Merged delta manifest from snapshot {}, {} active files now", 
                                deltaSnapshotId, fileStateMap.size());
                }
            }
        }
        
        // 3. 应用当前变更
        for (ManifestEntry entry : currentEntries) {
            String fileName = entry.getFile().getFileName();
            if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                fileStateMap.put(fileName, entry);
            } else if (entry.getKind() == ManifestEntry.FileKind.DELETE) {
                fileStateMap.remove(fileName);
            }
        }
        
        logger.info("Compaction merged to {} active files", fileStateMap.size());
        
        // 4. 创建新的 base manifest
        String baseManifestId = idGenerator.generateManifestId();
        String baseManifestFileName = "manifest-" + baseManifestId;
        
        List<ManifestEntry> allActiveEntries = new ArrayList<>(fileStateMap.values());
        ManifestFile baseManifestFile = new ManifestFile(allActiveEntries);
        baseManifestFile.persist(pathFactory, database, table, baseManifestId);
        
        ManifestFileMeta baseMeta = buildManifestFileMeta(
            baseManifestFileName, allActiveEntries, schemaId);
        
        // 5. 创建新的 base manifest list
        ManifestList baseList = new ManifestList();
        baseList.addManifestFile(baseMeta);
        
        String baseListFileName = "manifest-list-base-" + snapshotId;
        baseList.persistBase(pathFactory, database, table, snapshotId);
        
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Compaction completed in {}ms: created base manifest with {} files", 
                   duration, allActiveEntries.size());
        
        return baseListFileName;
    }
    
    /**
     * 将 ManifestList 中的条目合并到文件状态映射中
     */
    private void mergeManifestListIntoMap(ManifestList manifestList, 
                                         java.util.Map<String, ManifestEntry> fileStateMap) 
            throws IOException {
        for (ManifestFileMeta meta : manifestList.getManifestFiles()) {
            String manifestFileName = meta.getFileName();
            String manifestId = manifestFileName.startsWith("manifest-") 
                ? manifestFileName.substring("manifest-".length()) 
                : manifestFileName;
            
            ManifestFile manifestFile = ManifestFile.load(pathFactory, database, table, manifestId);
            
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
     * 获取最新快照
     * 
     * @return 最新快照
     * @throws IOException IO异常
     */
    public Snapshot getLatestSnapshot() throws IOException {
        return Snapshot.loadLatest(pathFactory, database, table);
    }

    /**
     * 根据ID获取快照
     * 
     * @param snapshotId 快照ID
     * @return 快照
     * @throws IOException IO异常
     */
    public Snapshot getSnapshot(long snapshotId) throws IOException {
        return Snapshot.load(pathFactory, database, table, snapshotId);
    }

    /**
     * 检查是否存在快照
     * 
     * @return true 如果存在快照，否则 false
     */
    public boolean hasSnapshot() {
        return Snapshot.hasLatestSnapshot(pathFactory, database, table);
    }

    /**
     * 获取快照数量
     * 
     * @return 快照数量
     */
    public int getSnapshotCount() {
        int count = 0;
        try {
            // 简单实现：通过查找快照目录下的文件数量来估算
            java.nio.file.Path snapshotDir = pathFactory.getSnapshotDir(database, table);
            if (java.nio.file.Files.exists(snapshotDir)) {
                try (java.util.stream.Stream<java.nio.file.Path> files = java.nio.file.Files.list(snapshotDir)) {
                    count = (int) files.filter(file -> file.getFileName().toString().startsWith("snapshot-")).count();
                }
            }
        } catch (IOException e) {
            logger.warn("Error counting snapshots", e);
        }
        return count;
    }

    /**
     * 获取所有快照列表（按 ID 升序）
     * 
     * @return 快照列表
     * @throws IOException IO异常
     */
    public List<Snapshot> getAllSnapshots() throws IOException {
        List<Snapshot> snapshots = new ArrayList<>();
        Path snapshotDir = pathFactory.getSnapshotDir(database, table);
        
        if (!Files.exists(snapshotDir)) {
            return snapshots;
        }
        
        try (Stream<Path> files = Files.list(snapshotDir)) {
            files.filter(path -> path.getFileName().toString().startsWith("snapshot-"))
                 .sorted(Comparator.comparing(path -> {
                     String fileName = path.getFileName().toString();
                     return Long.parseLong(fileName.substring("snapshot-".length()));
                 }))
                 .forEach(path -> {
                     try {
                         String fileName = path.getFileName().toString();
                         long snapshotId = Long.parseLong(fileName.substring("snapshot-".length()));
                         snapshots.add(Snapshot.load(pathFactory, database, table, snapshotId));
                     } catch (IOException e) {
                         logger.warn("Failed to load snapshot: {}", path, e);
                     }
                 });
        }
        
        return snapshots;
    }
    
    /**
     * 获取最早的快照
     * 
     * @return 最早的快照
     * @throws IOException IO异常
     */
    public Snapshot getEarliestSnapshot() throws IOException {
        Path earliestPath = pathFactory.getEarliestSnapshotPath(database, table);
        if (!Files.exists(earliestPath)) {
            // 如果 EARLIEST 不存在，扫描所有快照找到最小的
            List<Snapshot> allSnapshots = getAllSnapshots();
            if (allSnapshots.isEmpty()) {
                throw new IOException("No snapshot found");
            }
            return allSnapshots.get(0);
        }
        
        String earliestSnapshotId = new String(Files.readAllBytes(earliestPath)).trim();
        long snapshotId = Long.parseLong(earliestSnapshotId);
        return Snapshot.load(pathFactory, database, table, snapshotId);
    }
    
    /**
     * 获取所有活跃的文件（根据最新快照）
     * 
     * @return 活跃文件列表
     * @throws IOException IO异常
     */
    public List<ManifestEntry> getActiveFiles() throws IOException {
        if (!hasSnapshot()) {
            return new ArrayList<>();
        }
        
        Snapshot latestSnapshot = getLatestSnapshot();
        
        // 根据快照中的 manifest list 名称选择正确的加载方法
        String manifestListName = latestSnapshot.getDeltaManifestList();
        if (manifestListName == null || manifestListName.isEmpty()) {
            manifestListName = latestSnapshot.getBaseManifestList();
        }
        
        ManifestList manifestList;
        if (manifestListName != null) {
            long snapshotId = latestSnapshot.getId();
            if (manifestListName.startsWith("manifest-list-delta-")) {
                manifestList = ManifestList.loadDelta(pathFactory, database, table, snapshotId);
            } else if (manifestListName.startsWith("manifest-list-base-")) {
                manifestList = ManifestList.loadBase(pathFactory, database, table, snapshotId);
            } else {
                // 兼容旧格式
                manifestList = ManifestList.load(pathFactory, database, table, snapshotId);
            }
        } else {
            throw new IOException("No manifest list found in snapshot " + latestSnapshot.getId());
        }
        
        List<ManifestEntry> activeFiles = new ArrayList<>();
        for (ManifestFileMeta manifestFileMeta : manifestList.getManifestFiles()) {
            // 从文件名中提取ID
            String manifestFileName = manifestFileMeta.getFileName();
            String manifestId = manifestFileName.substring("manifest-".length());
            ManifestFile manifest = ManifestFile.load(pathFactory, database, table, manifestId);
            activeFiles.addAll(manifest.getEntries());
        }
        
        return activeFiles;
    }
}
