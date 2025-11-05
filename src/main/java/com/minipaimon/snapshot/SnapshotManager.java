package com.minipaimon.snapshot;

import com.minipaimon.manifest.ManifestEntry;
import com.minipaimon.manifest.ManifestFile;
import com.minipaimon.manifest.ManifestList;
import com.minipaimon.utils.IdGenerator;
import com.minipaimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 快照管理器
 * 负责快照的创建、管理和查询
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

    public SnapshotManager(PathFactory pathFactory, String database, String table) {
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.idGenerator = new IdGenerator();
        this.snapshotIdGenerator = new AtomicLong(0);
    }

    /**
     * 创建新的快照
     * 
     * @param schemaId Schema ID
     * @param manifestEntries Manifest 条目列表
     * @return 新创建的快照
     * @throws IOException IO异常
     */
    public synchronized Snapshot createSnapshot(int schemaId, List<ManifestEntry> manifestEntries) throws IOException {
        // 生成新的快照ID
        long snapshotId = snapshotIdGenerator.getAndIncrement();
        
        // 生成 Manifest ID
        String manifestId = idGenerator.generateManifestId();
        
        // 创建 Manifest 文件
        ManifestFile manifestFile = new ManifestFile(manifestEntries);
        manifestFile.persist(pathFactory, database, table, manifestId);
        
        // 创建 Manifest List
        ManifestList manifestList = new ManifestList();
        manifestList.addManifestFile("manifest-" + manifestId);
        
        // 如果存在之前的快照，将其 Manifest 文件也加入到列表中
        if (Snapshot.hasLatestSnapshot(pathFactory, database, table)) {
            try {
                Snapshot previousSnapshot = Snapshot.loadLatest(pathFactory, database, table);
                ManifestList previousManifestList = ManifestList.load(
                    pathFactory, database, table, previousSnapshot.getSnapshotId());
                for (String prevManifestFile : previousManifestList.getManifestFiles()) {
                    manifestList.addManifestFile(prevManifestFile);
                }
            } catch (IOException e) {
                logger.warn("Failed to load previous snapshot manifest list", e);
            }
        }
        
        // 持久化 Manifest List
        manifestList.persist(pathFactory, database, table, snapshotId);
        
        // 创建快照
        Snapshot snapshot = new Snapshot(
            snapshotId,
            schemaId,
            java.time.Instant.now(),
            "manifest/manifest-list-" + snapshotId
        );
        
        // 持久化快照
        snapshot.persist(pathFactory, database, table);
        
        logger.info("Created snapshot {} for table {}/{} with {} manifest entries", 
                   snapshotId, database, table, manifestEntries.size());
        
        return snapshot;
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
            pathFactory, database, table, latestSnapshot.getSnapshotId());
        
        List<ManifestEntry> activeFiles = new ArrayList<>();
        for (String manifestFileName : manifestList.getManifestFiles()) {
            // 从文件名中提取ID
            String manifestId = manifestFileName.substring("manifest-".length());
            ManifestFile manifest = ManifestFile.load(pathFactory, database, table, manifestId);
            activeFiles.addAll(manifest.getEntries());
        }
        
        return activeFiles;
    }
}
