package com.mini.paimon.manifest;

import com.mini.paimon.io.FileEntry;
import com.mini.paimon.io.ManifestFileIO;
import com.mini.paimon.io.ManifestListIO;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.utils.IdGenerator;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Manifest File Merger
 * 参考 Apache Paimon 的 ManifestFileMerger 设计

 * 负责合并多个小的 manifest 文件，减少文件数量，提升查询性能
 */
public class ManifestFileMerger {
    private static final Logger logger = LoggerFactory.getLogger(ManifestFileMerger.class);
    
    private final PathFactory pathFactory;
    private final String database;
    private final String table;
    private final IdGenerator idGenerator;
    private final int schemaId;
    
    // 新增：使用专门的 I/O 类（对齐 Paimon 设计）
    private final ManifestFileIO manifestFileWriter;
    private final ManifestListIO manifestListWriter;
    
    /**
     * 默认的manifest合并阈值
     * 当delta数量超过此值时，触发base manifest重新生成
     */
    public static final int DEFAULT_MERGE_THRESHOLD = 30;
    
    public ManifestFileMerger(
            PathFactory pathFactory,
            String database,
            String table,
            int schemaId) {
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.schemaId = schemaId;
        this.idGenerator = new IdGenerator();
        
        // 初始化 Manifest I/O 类（对齐 Paimon 设计）
        this.manifestFileWriter = new ManifestFileIO(pathFactory, database, table);
        this.manifestListWriter = new ManifestListIO(pathFactory, database, table);
    }
    
    /**
     * 判断是否需要压缩manifests
     * 
     * @param latestSnapshot 最新的snapshot
     * @param currentSnapshotId 当前要提交的snapshot ID
     * @param mergeThreshold 合并阈值（delta数量）
     * @return true 如果需要合并
     */
    public boolean shouldCompact(Snapshot latestSnapshot, long currentSnapshotId, int mergeThreshold) {
        if (latestSnapshot == null) {
            return false;
        }
        
        String baseListName = latestSnapshot.getBaseManifestList();
        if (baseListName == null) {
            return false;
        }
        
        long lastCompactedId = extractSnapshotId(baseListName);
        if (lastCompactedId < 0) {
            return false;
        }
        
        long deltaCount = currentSnapshotId - lastCompactedId;
        boolean shouldCompact = deltaCount >= mergeThreshold;
        
        if (shouldCompact) {
            logger.info("Should compact manifests: deltaCount={}, threshold={}, lastCompactedId={}, currentId={}", 
                       deltaCount, mergeThreshold, lastCompactedId, currentSnapshotId);
        }
        
        return shouldCompact;
    }
    
    /**
     * 判断是否需要压缩manifests（使用默认阈值）
     */
    public boolean shouldCompact(Snapshot latestSnapshot, long currentSnapshotId) {
        return shouldCompact(latestSnapshot, currentSnapshotId, DEFAULT_MERGE_THRESHOLD);
    }
    
    /**
     * 合并manifests，生成新的base manifest
     * 
     * @param snapshotId 新的snapshot ID
     * @param latestSnapshot 最新的snapshot
     * @param currentEntries 当前提交的entries
     * @param baseEntries 已加载的base entries（可选，用于优化）
     * @return 新的base manifest list名称
     * @throws IOException 合并失败
     */
    public String compactManifests(
            long snapshotId,
            Snapshot latestSnapshot,
            List<ManifestEntry> currentEntries,
            List<FileEntry.SimpleFileEntry> baseEntries) throws IOException {
        
        logger.info("Starting manifest compaction for snapshot {}", snapshotId);
        
        // 1. 读取所有活跃文件（如果baseEntries为空）
        List<ManifestEntry> allActiveFiles;
        if (baseEntries != null && !baseEntries.isEmpty()) {
            allActiveFiles = readAllActiveFilesFromEntries(latestSnapshot, baseEntries);
        } else {
            allActiveFiles = readAllActiveFiles(latestSnapshot);
        }
        
        // 2. 应用当前变更，构建最终状态
        Map<FileEntry.Identifier, ManifestEntry> fileStateMap = new LinkedHashMap<>();
        
        // 先应用base中的活跃文件
        for (ManifestEntry entry : allActiveFiles) {
            if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                fileStateMap.put(FileEntry.Identifier.fromEntry(entry), entry);
            }
        }
        
        // 再应用当前变更
        for (ManifestEntry entry : currentEntries) {
            FileEntry.Identifier id = FileEntry.Identifier.fromEntry(entry);
            if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                fileStateMap.put(id, entry);
            } else {
                fileStateMap.remove(id);
            }
        }
        
        // 3. 生成新的 base manifest（使用 ManifestFileIO）
        List<ManifestEntry> allActive = new ArrayList<>(fileStateMap.values());
        String baseManifestId = idGenerator.generateManifestId();
        String manifestFileName = manifestFileWriter.writeManifest(allActive, baseManifestId);
        
        // 4. 创建 ManifestFileMeta
        ManifestFileMeta baseMeta = ManifestFileMetaBuilder.build(
            manifestFileName, allActive, schemaId);
        
        // 5. 创建 base manifest list（使用 ManifestListIO）
        String baseListName = manifestListWriter.writeBaseManifestList(
            Collections.singletonList(baseMeta), snapshotId);
        
        logger.info("Manifest compaction completed: {} active files in new base", allActive.size());
        
        return baseListName;
    }
    
    /**
     * 读取所有活跃文件（从snapshot）
     */
    private List<ManifestEntry> readAllActiveFiles(Snapshot snapshot) throws IOException {
        List<FileEntry.SimpleFileEntry> entries = readAllEntriesFromSnapshot(snapshot);
        
        // 合并得到活跃文件
        Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> merged = 
            FileEntry.mergeEntries(entries);
        
        // 转换回ManifestEntry（只保留ADD类型）
        List<ManifestEntry> result = new ArrayList<>();
        for (FileEntry.SimpleFileEntry entry : merged.values()) {
            if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                result.add(reconstructManifestEntry(entry));
            }
        }
        
        return result;
    }
    
    /**
     * 从已加载的entries读取活跃文件
     */
    private List<ManifestEntry> readAllActiveFilesFromEntries(
            Snapshot snapshot,
            List<FileEntry.SimpleFileEntry> baseEntries) throws IOException {
        
        // 合并得到活跃文件
        Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> merged = 
            FileEntry.mergeEntries(baseEntries);
        
        // 转换回ManifestEntry（只保留ADD类型）
        List<ManifestEntry> result = new ArrayList<>();
        for (FileEntry.SimpleFileEntry entry : merged.values()) {
            if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                result.add(reconstructManifestEntry(entry));
            }
        }
        
        return result;
    }
    
    /**
     * 读取 snapshot 的所有文件条目（使用 ManifestListIO 和 ManifestFileIO）
     */
    private List<FileEntry.SimpleFileEntry> readAllEntriesFromSnapshot(Snapshot snapshot) throws IOException {
        List<FileEntry.SimpleFileEntry> entries = new ArrayList<>();
        
        // 读取 base manifest list
        String baseListName = snapshot.getBaseManifestList();
        if (baseListName != null) {
            long snapshotId = extractSnapshotId(baseListName);
            List<ManifestFileMeta> manifestMetas = loadManifestList(baseListName, snapshotId);
            
            // 读取所有 manifest files
            for (ManifestFileMeta meta : manifestMetas) {
                String manifestId = extractManifestId(meta.getFileName());
                List<ManifestEntry> manifestEntries = manifestFileWriter.readManifestById(manifestId);
                entries.addAll(FileEntry.fromManifestEntries(manifestEntries));
            }
        }
        
        return entries;
    }
    
    /**
     * 加载 manifest list（根据名称自动判断类型，使用 ManifestListIO）
     */
    private List<ManifestFileMeta> loadManifestList(String listName, long snapshotId) throws IOException {
        if (listName.startsWith("manifest-list-base-")) {
            return manifestListWriter.readBaseManifestList(snapshotId);
        } else if (listName.startsWith("manifest-list-delta-")) {
            return manifestListWriter.readDeltaManifestList(snapshotId);
        } else {
            return manifestListWriter.readManifestList(listName);
        }
    }
    
    /**
     * 从SimpleFileEntry重构ManifestEntry
     * 注意：这是简化版本，部分字段可能丢失
     */
    private ManifestEntry reconstructManifestEntry(FileEntry.SimpleFileEntry entry) {
        return new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            0, // bucket默认为0
            new DataFileMeta(
                entry.getFileName(),
                0, // fileSize未知
                0, // rowCount未知
                entry.getMinKey(),
                entry.getMaxKey(),
                schemaId,
                entry.getLevel(),
                System.currentTimeMillis()
            )
        );
    }
    
    /**
     * 从manifest list名称提取snapshot ID
     */
    private long extractSnapshotId(String manifestListName) {
        try {
            if (manifestListName.startsWith("manifest-list-delta-")) {
                return Long.parseLong(manifestListName.substring("manifest-list-delta-".length()));
            } else if (manifestListName.startsWith("manifest-list-base-")) {
                return Long.parseLong(manifestListName.substring("manifest-list-base-".length()));
            } else if (manifestListName.startsWith("manifest-list-")) {
                return Long.parseLong(manifestListName.substring("manifest-list-".length()));
            }
        } catch (NumberFormatException e) {
            logger.warn("Failed to extract snapshot ID from: {}", manifestListName);
        }
        return -1;
    }
    
    /**
     * 从manifest文件名提取ID
     */
    private String extractManifestId(String manifestFileName) {
        if (manifestFileName.startsWith("manifest-")) {
            return manifestFileName.substring("manifest-".length());
        }
        return manifestFileName;
    }
}

