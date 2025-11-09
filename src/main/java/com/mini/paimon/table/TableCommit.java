package com.mini.paimon.table;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.exception.CatalogException;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.manifest.ManifestFile;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.IdGenerator;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Table 提交器
 * 参考 Paimon TableCommit 设计，负责提交快照
 * 
 * 两阶段提交流程：
 * 1. TableWrite.prepareCommit() - 生成 CommitMessage（Prepare 阶段）
 * 2. TableCommit.commit() - 原子性提交 Snapshot（Commit 阶段）
 * 
 * 并发控制：
 * - 使用锁保证同一时刻只有一个提交操作
 * 
 * 注意：
 * - 幂等性由 Snapshot ID 本身保证（严格递增）
 * - commitIdentifier 用于应用层追踪，不是幂等性的关键
 */
public class TableCommit {
    private static final Logger logger = LoggerFactory.getLogger(TableCommit.class);
    
    private final Catalog catalog;
    private final PathFactory pathFactory;
    private final Identifier identifier;
    private final SnapshotManager snapshotManager;
    private final IdGenerator idGenerator;
    
    // 并发控制锁（保证原子性）
    private final ReentrantLock commitLock = new ReentrantLock();
    
    // 记录最后成功提交的 commitIdentifier（用于幂等性检查）
    // 注意：每个 TableCommit 实例独立维护，不跨实例共享
    private volatile long lastCommittedIdentifier = 0L;
    
    public TableCommit(Catalog catalog, PathFactory pathFactory, Identifier identifier) {
        this.catalog = catalog;
        this.pathFactory = pathFactory;
        this.identifier = identifier;
        this.snapshotManager = new SnapshotManager(
            pathFactory, identifier.getDatabase(), identifier.getTable());
        this.idGenerator = new IdGenerator();
    }
    
    /**
     * 提交写入操作（两阶段提交的 Commit 阶段）
     * 
     * Commit 阶段职责：
     * 1. 检查幂等性（避免重复提交）
     * 2. 创建 Manifest 文件
     * 3. 创建 Snapshot
     * 4. 原子性提交到 Catalog
     * 5. 更新提交状态
     * 
     * @param commitMessage Prepare 阶段生成的提交消息
     * @throws IOException 提交失败
     */
    public void commit(TableWrite.TableCommitMessage commitMessage) throws IOException {
        commit(commitMessage, Snapshot.CommitKind.APPEND);
    }
    
    /**
     * 提交写入操作（指定提交类型）
     * 
     * @param commitMessage 提交消息
     * @param commitKind 提交类型（APPEND/COMPACT/OVERWRITE）
     * @throws IOException 提交失败
     */
    public void commit(TableWrite.TableCommitMessage commitMessage, Snapshot.CommitKind commitKind) 
            throws IOException {
        
        // 获取提交锁，保证原子性
        commitLock.lock();
        try {
            logger.info("Starting commit for table {}, commitIdentifier={}, kind={}",
                identifier, commitMessage.getCommitIdentifier(), commitKind);
            
            long startTime = System.currentTimeMillis();
            
            // 1. 幂等性检查：如果已经提交过，直接返回
            if (commitMessage.getCommitIdentifier() <= lastCommittedIdentifier) {
                logger.info("CommitIdentifier {} already committed, skipping (idempotent)",
                    commitMessage.getCommitIdentifier());
                return;
            }
            
            // 2. 验证提交消息
            validateCommitMessage(commitMessage);
            
            // 3. 获取新文件列表
            List<ManifestEntry> manifestEntries = commitMessage.getNewFiles();
            
            // 4. 对于 OVERWRITE 提交，即使没有数据文件也要创建快照（表示数据被清空）
            if (manifestEntries.isEmpty() && commitKind != Snapshot.CommitKind.OVERWRITE) {
                logger.warn("No data files to commit for table {}", identifier);
                return;
            }
            
            // 5. 创建 Snapshot
            Snapshot snapshot = snapshotManager.createSnapshot(
                commitMessage.getSchemaId(), 
                manifestEntries, 
                commitKind
            );
            
            // 6. 原子性提交到 Catalog
            try {
                catalog.commitSnapshot(identifier, snapshot);
                
                // 7. 更新成功提交的 commitIdentifier
                lastCommittedIdentifier = commitMessage.getCommitIdentifier();
                
                long duration = System.currentTimeMillis() - startTime;
                logger.info("Successfully committed snapshot {} to table {}, took {}ms, files={}",
                    snapshot.getId(), identifier, duration, manifestEntries.size());
                    
            } catch (CatalogException e) {
                // 提交失败，记录错误并抛出异常
                logger.error("Failed to commit snapshot {} to table {}",
                    snapshot.getId(), identifier, e);
                throw new IOException("Failed to commit snapshot", e);
            }
            
        } finally {
            commitLock.unlock();
        }
    }
    
    /**
     * 验证提交消息
     */
    private void validateCommitMessage(TableWrite.TableCommitMessage commitMessage) {
        if (commitMessage == null) {
            throw new IllegalArgumentException("CommitMessage cannot be null");
        }
        
        if (!commitMessage.getDatabase().equals(identifier.getDatabase()) ||
            !commitMessage.getTable().equals(identifier.getTable())) {
            throw new IllegalArgumentException(
                String.format("CommitMessage table mismatch. Expected: %s, Actual: %s.%s",
                    identifier, commitMessage.getDatabase(), commitMessage.getTable()));
        }
        
        if (commitMessage.getCommitIdentifier() <= 0) {
            throw new IllegalArgumentException(
                "CommitIdentifier must be positive: " + commitMessage.getCommitIdentifier());
        }
    }
    
    /**
     * 收集 Manifest 条目
     * 支持分区表：扫描表目录下的所有分区子目录（与 snapshot、manifest 同级）
     */
    private List<ManifestEntry> collectManifestEntries() throws IOException {
        List<ManifestEntry> entries = new ArrayList<>();
        
        Path tableDir = pathFactory.getTablePath(identifier.getDatabase(), identifier.getTable());
        if (!Files.exists(tableDir)) {
            return entries;
        }
        
        // 递归扫描表目录，查找所有 .sst 文件（包括分区目录）
        collectSSTFiles(tableDir, tableDir, entries);
        
        return entries;
    }
    
    /**
     * 递归收集 SSTable 文件
     * @param scanDir 要扫描的目录
     * @param baseDir 基准目录（用于计算相对路径）
     * @param entries 收集的条目列表
     */
    private void collectSSTFiles(Path scanDir, Path baseDir, List<ManifestEntry> entries) throws IOException {
        try (java.util.stream.Stream<Path> stream = Files.walk(scanDir)) {
            stream.filter(path -> path.toString().endsWith(".sst"))
                .forEach(sstPath -> {
                    try {
                        long fileSize = Files.size(sstPath);
                        // 计算相对于表目录的相对路径
                        String relativePath = baseDir.relativize(sstPath).toString();
                        
                        ManifestEntry entry = ManifestEntry.addFile(
                            relativePath,  // 使用相对路径，如：dt=2024-01-01/data-0-000.sst
                            fileSize,
                            0,
                            null,
                            null,
                            0,
                            0
                        );
                        entries.add(entry);
                    } catch (IOException e) {
                        logger.warn("Failed to create manifest entry for {}", sstPath, e);
                    }
                });
        }
    }
    
    /**
     * 中止提交（清理临时文件）
     * 
     * 此方法用于清理在 Prepare 阶段生成的临时文件，
     * 但由于我们的实现中 Prepare 直接生成正式文件，
     * 此方法仅做记录。
     * 
     * 注意：
     * - 生产环境中应该使用临时目录和原子重命名
     * - 当前实现是简化版本
     */
    public void abort() {
        logger.info("Aborting commit for table {} (cleanup temporary resources)", identifier);
        // TODO: 在完整实现中，这里应该清理临时文件
    }
}
