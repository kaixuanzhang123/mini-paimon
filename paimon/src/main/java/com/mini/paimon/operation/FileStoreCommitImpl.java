package com.mini.paimon.operation;

import com.mini.paimon.io.FileEntry;
import com.mini.paimon.io.ManifestFileIO;
import com.mini.paimon.io.ManifestListIO;
import com.mini.paimon.manifest.*;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.IdGenerator;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * FileStore Commit Implementation
 * 实现两阶段提交、MVCC并发控制、冲突检测等核心功能
 */
public class FileStoreCommitImpl {
    private static final Logger logger = LoggerFactory.getLogger(FileStoreCommitImpl.class);
    
    private final PathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final SnapshotCommit snapshotCommit;
    private final Schema schema;
    private final String database;
    private final String table;
    private final String branch;
    private final CommitOptions options;
    private final IdGenerator idGenerator;
    private final ManifestFileMerger manifestMerger;
    
    // 新增：专门的 I/O 类（对齐 Paimon 设计）
    private final ManifestFileIO manifestFileWriter;
    private final ManifestListIO manifestListWriter;
    
    public FileStoreCommitImpl(
            PathFactory pathFactory,
            SnapshotManager snapshotManager,
            Schema schema,
            String database,
            String table,
            String branch) {
        this(pathFactory, snapshotManager, schema, database, table, branch, new CommitOptions());
    }
    
    public FileStoreCommitImpl(
            PathFactory pathFactory,
            SnapshotManager snapshotManager,
            Schema schema,
            String database,
            String table,
            String branch,
            CommitOptions options) {
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.snapshotCommit = new AtomicRenameSnapshotCommit(
            pathFactory, snapshotManager, database, table);
        this.schema = schema;
        this.database = database;
        this.table = table;
        this.branch = branch != null ? branch : "main";
        this.options = options;
        this.idGenerator = new IdGenerator();
        this.manifestMerger = new ManifestFileMerger(
            pathFactory, database, table, schema.getSchemaId());
        
        // 初始化 Manifest I/O 类（对齐 Paimon 设计）
        this.manifestFileWriter = new ManifestFileIO(pathFactory, database, table);
        this.manifestListWriter = new ManifestListIO(pathFactory, database, table);
    }
    
    /**
     * 提交ManifestCommittable（主入口）
     * 
     * @param committable 提交数据
     * @return 提交生成的snapshot数量
     * @throws IOException 提交失败
     */
    public int commit(ManifestCommittable committable) throws IOException {
        logger.info("Starting commit for table {}.{}, committable: {}", 
                   database, table, committable);
        
        int generatedSnapshots = 0;
        
        // 1. 提交追加文件（如果有）
        if (!committable.getAppendTableFiles().isEmpty()) {
            tryCommit(
                committable.getCommitIdentifier(),
                committable.getCommitUser(),
                committable.getAppendTableFiles(),
                committable.getCommitKind(),
                committable.getWatermark()
            );
            generatedSnapshots++;
        }
        
        // 2. 提交压缩文件（如果有）
        if (!committable.getCompactTableFiles().isEmpty()) {
            tryCommit(
                committable.getCommitIdentifier(),
                committable.getCommitUser(),
                committable.getCompactTableFiles(),
                Snapshot.CommitKind.COMPACT,
                committable.getWatermark()
            );
            generatedSnapshots++;
        }
        
        logger.info("Commit completed for table {}.{}, generated {} snapshots",
                   database, table, generatedSnapshots);
        
        return generatedSnapshots;
    }
    
    /**
     * 尝试提交（带重试机制）
     */
    private void tryCommit(
            long commitIdentifier,
            String commitUser,
            List<ManifestEntry> entries,
            Snapshot.CommitKind commitKind,
            Long watermark) throws IOException {
        
        int attempts = 0;
        long startMillis = System.currentTimeMillis();
        Snapshot latestSnapshot = null;
        List<FileEntry.SimpleFileEntry> baseEntries = new ArrayList<>();
        
        while (true) {
            try {
                // 1. 读取最新snapshot
                latestSnapshot = snapshotManager.latestSnapshot();
                
                // 2. 尝试提交一次
                CommitResult result = tryCommitOnce(
                    commitIdentifier,
                    commitUser,
                    entries,
                    commitKind,
                    watermark,
                    latestSnapshot,
                    baseEntries
                );
                
                if (result.isSuccess()) {
                    logger.info("Commit succeeded after {} attempts", attempts + 1);
                    return;
                }
                
                // 3. 检查超时和重试次数
                long elapsed = System.currentTimeMillis() - startMillis;
                if (elapsed > options.getCommitTimeout()) {
                    throw new IOException("Commit timeout after " + elapsed + "ms");
                }
                
                if (attempts >= options.getCommitMaxRetries()) {
                    throw new IOException("Commit failed after " + attempts + " retries");
                }
                
                // 4. 指数退避重试
                commitRetryWait(attempts);
                attempts++;
                
                logger.info("Retrying commit, attempt {}", attempts + 1);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Commit interrupted", e);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("Commit failed", e);
            }
        }
    }
    
    /**
     * 单次提交尝试
     */
    private CommitResult tryCommitOnce(
            long commitIdentifier,
            String commitUser,
            List<ManifestEntry> deltaEntries,
            Snapshot.CommitKind commitKind,
            Long watermark,
            Snapshot latestSnapshot,
            List<FileEntry.SimpleFileEntry> baseEntries) throws Exception {
        
        // 1. 去重检查：检查是否已经提交过
        if (latestSnapshot != null) {
            if (isDuplicate(latestSnapshot, commitUser, commitIdentifier, commitKind)) {
                logger.info("Commit already exists (duplicate detection), returning success");
                return CommitResult.success();
            }
        }
        
        // 2. 计算新的snapshot ID
        long newSnapshotId = latestSnapshot != null ? latestSnapshot.getId() + 1 : 1;
        
        // 3. 冲突检查
        if (latestSnapshot != null && !baseEntries.isEmpty()) {
            noConflictsOrFail(latestSnapshot, baseEntries, deltaEntries);
        } else if (latestSnapshot != null) {
            // 第一次尝试，读取base文件
            baseEntries.addAll(readAllEntriesFromLatestSnapshot(latestSnapshot));
        }
        
        // 4. 写入 Delta Manifest（使用 ManifestFileIO）
        String deltaManifestId = idGenerator.generateManifestId();
        String manifestFileName = manifestFileWriter.writeManifest(deltaEntries, deltaManifestId);
        
        ManifestFileMeta deltaMeta = ManifestFileMetaBuilder.build(
            manifestFileName, deltaEntries, schema.getSchemaId());
        
        // 5. 写入 Delta Manifest List（使用 ManifestListIO）
        String deltaManifestListName = manifestListWriter.writeDeltaManifestList(
            Collections.singletonList(deltaMeta), newSnapshotId);
        
        // 6. 确定Base Manifest List
        String baseManifestListName;
        if (latestSnapshot != null) {
            // 检查是否需要重新生成base
            if (shouldCompactManifests(latestSnapshot, newSnapshotId)) {
                logger.info("Compacting manifests for snapshot {}", newSnapshotId);
                baseManifestListName = compactManifests(
                    newSnapshotId, latestSnapshot, deltaEntries, baseEntries);
            } else {
                // 复用上一个base
                baseManifestListName = latestSnapshot.getBaseManifestList();
            }
        } else {
            // 首次提交，delta即为base
            baseManifestListName = deltaManifestListName;
        }
        
        // 7. 计算记录数
        long deltaRecordCount = calculateRecordCount(deltaEntries);
        long totalRecordCount = calculateTotalRecordCount(
            latestSnapshot, deltaEntries, deltaRecordCount);
        
        // 8. 创建新的Snapshot
        Snapshot newSnapshot = new Snapshot(
            3, // version
            newSnapshotId,
            schema.getSchemaId(),
            baseManifestListName,
            deltaManifestListName,
            commitUser,
            commitIdentifier,
            commitKind,
            System.currentTimeMillis(),
            totalRecordCount,
            deltaRecordCount
        );
        
        // 9. 原子提交snapshot
        boolean committed = snapshotCommit.commit(newSnapshot, branch);
        
        if (!committed) {
            logger.warn("Snapshot {} commit failed (ID conflict), will retry", newSnapshotId);
            return CommitResult.retry();
        }
        
        logger.info("Successfully committed snapshot {}", newSnapshotId);
        return CommitResult.success();
    }
    
    /**
     * 去重检查：检查snapshot是否已包含此提交
     */
    private boolean isDuplicate(Snapshot snapshot, String commitUser, 
                               long commitIdentifier, Snapshot.CommitKind commitKind) {
        return snapshot.getCommitUser().equals(commitUser) &&
               snapshot.getCommitIdentifier() == commitIdentifier &&
               snapshot.getCommitKind() == commitKind;
    }
    
    /**
     * 冲突检测
     */
    private void noConflictsOrFail(
            Snapshot latestSnapshot,
            List<FileEntry.SimpleFileEntry> baseEntries,
            List<ManifestEntry> deltaEntries) throws IOException {
        
        // 1. 合并base + delta
        List<FileEntry.SimpleFileEntry> allEntries = new ArrayList<>(baseEntries);
        allEntries.addAll(FileEntry.fromManifestEntries(deltaEntries));
        
        // 2. 检查文件删除冲突
        Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> mergedEntries;
        try {
            mergedEntries = FileEntry.mergeEntries(allEntries);
        } catch (IllegalStateException e) {
            throw new IOException("File conflict detected: " + e.getMessage(), e);
        }
        
        // 3. 检查是否有DELETE标记（说明要删除的文件不存在）
        try {
            FileEntry.checkNoDeleteMarks(mergedEntries);
        } catch (IllegalStateException e) {
            throw new IOException("File deletion conflict: " + e.getMessage(), e);
        }
        
        // 4. 对于主键表，检查LSM键范围冲突
        if (!schema.getPrimaryKeys().isEmpty()) {
            checkLSMConflicts(mergedEntries);
        }
    }
    
    /**
     * 检查LSM键范围冲突（主键表）
     */
    private void checkLSMConflicts(Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> entries) 
            throws IOException {
        
        // 按partition, bucket, level分组
        Map<String, List<FileEntry.SimpleFileEntry>> levelGroups = new HashMap<>();
        
        for (FileEntry.SimpleFileEntry entry : entries.values()) {
            if (entry.getKind() != ManifestEntry.FileKind.ADD) {
                continue;
            }
            
            // 跳过Level 0（允许重叠）
            if (entry.getLevel() == 0) {
                continue;
            }
            
            String key = entry.getIdentifier().toString() + "-level-" + entry.getLevel();
            levelGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(entry);
        }
        
        // 检查每个level的键范围
        for (Map.Entry<String, List<FileEntry.SimpleFileEntry>> group : levelGroups.entrySet()) {
            List<FileEntry.SimpleFileEntry> files = group.getValue();
            if (files.size() <= 1) {
                continue;
            }
            
            // 按minKey排序
            files.sort((a, b) -> {
                if (a.getMinKey() == null || b.getMinKey() == null) {
                    return 0;
                }
                return a.getMinKey().compareTo(b.getMinKey());
            });
            
            // 检查相邻文件的键范围是否重叠
            for (int i = 0; i < files.size() - 1; i++) {
                FileEntry.SimpleFileEntry current = files.get(i);
                FileEntry.SimpleFileEntry next = files.get(i + 1);
                
                if (current.getMaxKey() != null && next.getMinKey() != null) {
                    if (current.getMaxKey().compareTo(next.getMinKey()) >= 0) {
                        throw new IOException(
                            "LSM key range conflict detected: " +
                            "file " + current.getFileName() + " [" + current.getMinKey() + 
                            ", " + current.getMaxKey() + "] overlaps with " +
                            "file " + next.getFileName() + " [" + next.getMinKey() + 
                            ", " + next.getMaxKey() + "]");
                    }
                }
            }
        }
    }
    
    /**
     * 读取最新snapshot的所有文件条目
     */
    private List<FileEntry.SimpleFileEntry> readAllEntriesFromLatestSnapshot(Snapshot snapshot) 
            throws IOException {
        List<FileEntry.SimpleFileEntry> entries = new ArrayList<>();
        
        // 读取 base manifest list（使用 ManifestListIO）
        String baseListName = snapshot.getBaseManifestList();
        if (baseListName != null) {
            long snapshotId = extractSnapshotId(baseListName);
            List<ManifestFileMeta> manifestMetas;
            
            if (baseListName.startsWith("manifest-list-base-")) {
                manifestMetas = manifestListWriter.readBaseManifestList(snapshotId);
            } else if (baseListName.startsWith("manifest-list-delta-")) {
                manifestMetas = manifestListWriter.readDeltaManifestList(snapshotId);
            } else {
                manifestMetas = manifestListWriter.readManifestList(baseListName);
            }
            
            // 读取所有 manifest files（使用 ManifestFileIO）
            for (ManifestFileMeta meta : manifestMetas) {
                String manifestId = extractManifestId(meta.getFileName());
                List<ManifestEntry> manifestEntries = manifestFileWriter.readManifestById(manifestId);
                entries.addAll(FileEntry.fromManifestEntries(manifestEntries));
            }
        }
        
        return entries;
    }
    
    /**
     * 判断是否需要压缩manifests
     */
    private boolean shouldCompactManifests(Snapshot latestSnapshot, long currentSnapshotId) {
        return manifestMerger.shouldCompact(
            latestSnapshot, currentSnapshotId, options.getManifestMergeMinCount());
    }
    
    /**
     * 压缩manifests（委托给ManifestFileMerger）
     */
    private String compactManifests(
            long snapshotId,
            Snapshot latestSnapshot,
            List<ManifestEntry> currentEntries,
            List<FileEntry.SimpleFileEntry> baseEntries) throws IOException {
        
        return manifestMerger.compactManifests(
            snapshotId, latestSnapshot, currentEntries, baseEntries);
    }
    
    
    /**
     * 计算delta记录数
     */
    private long calculateRecordCount(List<ManifestEntry> entries) {
        return entries.stream()
            .mapToLong(e -> e.getFile().getRowCount())
            .sum();
    }
    
    /**
     * 计算总记录数
     */
    private long calculateTotalRecordCount(
            Snapshot previousSnapshot,
            List<ManifestEntry> deltaEntries,
            long deltaRecordCount) {
        
        if (previousSnapshot == null) {
            return deltaRecordCount;
        }
        
        long previousTotal = previousSnapshot.getTotalRecordCount();
        long addedRecords = 0;
        long deletedRecords = 0;
        
        for (ManifestEntry entry : deltaEntries) {
            if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                addedRecords += entry.getFile().getRowCount();
            } else {
                deletedRecords += entry.getFile().getRowCount();
            }
        }
        
        return previousTotal + addedRecords - deletedRecords;
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
    
    /**
     * 重试等待（指数退避）
     */
    private void commitRetryWait(int retryCount) throws InterruptedException {
        long waitMs = Math.min(
            options.getCommitRetryMinWait() * (1L << retryCount),
            options.getCommitRetryMaxWait()
        );
        
        // 添加随机抖动（±20%）
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long jitter = random.nextLong(Math.max(1, (long)(waitMs * 0.2)));
        waitMs = waitMs + jitter - (long)(waitMs * 0.1);
        
        logger.info("Waiting {}ms before retry", waitMs);
        TimeUnit.MILLISECONDS.sleep(waitMs);
    }
    
    /**
     * 提交结果
     */
    private static class CommitResult {
        private final boolean success;
        
        private CommitResult(boolean success) {
            this.success = success;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public static CommitResult success() {
            return new CommitResult(true);
        }
        
        public static CommitResult retry() {
            return new CommitResult(false);
        }
    }
}

