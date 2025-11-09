package com.mini.paimon.snapshot;

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
 */
public class SnapshotManager {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);
    
    /** 路径工厂 */
    private final PathFactory pathFactory;
    
    /** 数据库名 */
    private final String database;
    
    /** 表名 */
    private final String table;
    
    /** ID 生成器 */
    private final IdGenerator idGenerator;
    
    /** 快照ID生成器 */
    private final AtomicLong snapshotIdGenerator;
    
    /** 提交用户标识（默认为 UUID）*/
    private final String commitUser;

    public SnapshotManager(PathFactory pathFactory, String database, String table) {
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.idGenerator = new IdGenerator();
        this.snapshotIdGenerator = new AtomicLong(initializeSnapshotId());
        this.commitUser = UUID.randomUUID().toString();
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
     * @param schemaId Schema ID
     * @param manifestEntries Manifest 条目列表
     * @param commitKind 提交类型
     * @return 新创建的快照
     * @throws IOException IO异常
     */
    public synchronized Snapshot createSnapshot(int schemaId, List<ManifestEntry> manifestEntries, 
                                               Snapshot.CommitKind commitKind) throws IOException {
        // 1. 生成新的快照 ID
        long snapshotId = snapshotIdGenerator.getAndIncrement();
        
        // 2. 生成 Manifest ID
        String manifestId = idGenerator.generateManifestId();
        
        // 3. 创建 Delta Manifest 文件（本次变更）
        ManifestFile deltaManifestFile = new ManifestFile(manifestEntries);
        deltaManifestFile.persist(pathFactory, database, table, manifestId);
        
        // 4. 计算 Delta Manifest 的元信息
        ManifestFileMeta deltaManifestMeta = buildManifestFileMeta(
            "manifest-" + manifestId, manifestEntries, schemaId);
        
        // 5. 创建 Delta Manifest List
        ManifestList deltaManifestList = new ManifestList();
        deltaManifestList.addManifestFile(deltaManifestMeta);
        String deltaManifestListName = "manifest-list-delta-" + snapshotId;
        deltaManifestList.persist(pathFactory, database, table, snapshotId);
        
        // 6. 创建 Base Manifest List（包含历史 + 本次变更）
        ManifestList baseManifestList = new ManifestList();
        baseManifestList.addManifestFile(deltaManifestMeta);
        
        // 如果存在之前的快照，合并其 Base Manifest List
        if (Snapshot.hasLatestSnapshot(pathFactory, database, table)) {
            try {
                Snapshot previousSnapshot = Snapshot.loadLatest(pathFactory, database, table);
                ManifestList previousBaseList = ManifestList.load(
                    pathFactory, database, table, previousSnapshot.getId());
                for (ManifestFileMeta prevManifestFile : previousBaseList.getManifestFiles()) {
                    baseManifestList.addManifestFile(prevManifestFile);
                }
            } catch (IOException e) {
                logger.warn("Failed to load previous snapshot base manifest list", e);
            }
        }
        
        String baseManifestListName = "manifest-list-" + snapshotId;
        
        // 7. 计算记录数
        long deltaRecordCount = calculateRecordCount(manifestEntries);
        long totalRecordCount = calculateTotalRecordCount(baseManifestList);
        
        // 8. 使用 Builder 创建 Snapshot
        Snapshot snapshot = new Snapshot.Builder()
            .id(snapshotId)
            .schemaId(schemaId)
            .baseManifestList(baseManifestListName)
            .deltaManifestList(deltaManifestListName)
            .commitUser(commitUser)
            .commitIdentifier(snapshotId)
            .commitKind(commitKind)
            .timeMillis(System.currentTimeMillis())
            .totalRecordCount(totalRecordCount)
            .deltaRecordCount(deltaRecordCount)
            .build();
        
        // 9. 持久化快照
        snapshot.persist(pathFactory, database, table);
        
        logger.info("Created snapshot {} for table {}/{} with {} manifest entries, kind={}, " +
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
        com.mini.paimon.metadata.RowKey minKey = null;
        com.mini.paimon.metadata.RowKey maxKey = null;
        
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
     * 计算总记录数（从 ManifestList）
     */
    private long calculateTotalRecordCount(ManifestList manifestList) {
        
        return manifestList.getManifestFiles().stream()
            .mapToLong(meta -> {
                try {
                    String manifestFileName = meta.getFileName();
                    String manifestId = manifestFileName.substring("manifest-".length());
                    ManifestFile manifest = ManifestFile.load(pathFactory, database, table, manifestId);
                    return manifest.getEntries().stream()
                        .filter(e -> e.getKind() == ManifestEntry.FileKind.ADD)
                        .mapToLong(e -> e.getFile().getRowCount())
                        .sum();
                } catch (IOException e) {
                    logger.warn("Failed to load manifest file: {}", meta.getFileName(), e);
                    return 0;
                }
            })
            .sum();
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
        ManifestList manifestList = ManifestList.load(
            pathFactory, database, table, latestSnapshot.getId());
        
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
