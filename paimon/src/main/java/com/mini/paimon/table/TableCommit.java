package com.mini.paimon.table;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.exception.CatalogException;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.manifest.ManifestFile;
import com.mini.paimon.manifest.ManifestFileMeta;
import com.mini.paimon.manifest.ManifestList;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.transaction.Transaction;
import com.mini.paimon.transaction.TransactionManager;
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
    
    // 事务管理器（可选）
    private TransactionManager transactionManager;
    
    public TableCommit(Catalog catalog, PathFactory pathFactory, Identifier identifier) {
        this.catalog = catalog;
        this.pathFactory = pathFactory;
        this.identifier = identifier;
        this.snapshotManager = new SnapshotManager(
            pathFactory, identifier.getDatabase(), identifier.getTable());
        this.idGenerator = new IdGenerator();
        this.transactionManager = null;
    }
    
    /**
     * 构造函数（支持事务管理器）
     */
    public TableCommit(Catalog catalog, PathFactory pathFactory, Identifier identifier, 
                      TransactionManager transactionManager) {
        this.catalog = catalog;
        this.pathFactory = pathFactory;
        this.identifier = identifier;
        this.snapshotManager = new SnapshotManager(
            pathFactory, identifier.getDatabase(), identifier.getTable());
        this.idGenerator = new IdGenerator();
        this.transactionManager = transactionManager;
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
        commit(commitMessage, Snapshot.CommitKind.APPEND, null);
    }
    
    /**
     * 提交写入操作（支持事务）
     */
    public void commit(TableWrite.TableCommitMessage commitMessage, String transactionId) throws IOException {
        commit(commitMessage, Snapshot.CommitKind.APPEND, transactionId);
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
        commit(commitMessage, commitKind, null);
    }
    
    /**
     * 提交写入操作（完整版本，支持事务）
     * 
     * @param commitMessage 提交消息
     * @param commitKind 提交类型
     * @param transactionId 事务ID（可选）
     * @throws IOException 提交失败
     */
    public void commit(TableWrite.TableCommitMessage commitMessage, 
                      Snapshot.CommitKind commitKind,
                      String transactionId) throws IOException {
        
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
            List<ManifestEntry> manifestEntries = new ArrayList<>(commitMessage.getNewFiles());
            
            // 4. 对于 OVERWRITE 提交，需要标记所有旧文件为已删除
            if (commitKind == Snapshot.CommitKind.OVERWRITE) {
                // 获取上一个快照的所有文件
                if (Snapshot.hasLatestSnapshot(pathFactory, identifier.getDatabase(), identifier.getTable())) {
                    try {
                        Snapshot previousSnapshot = Snapshot.loadLatest(pathFactory, 
                            identifier.getDatabase(), identifier.getTable());
                        
                        // 读取上一个快照的 manifest list
                        String manifestListName = previousSnapshot.getDeltaManifestList();
                        if (manifestListName == null || manifestListName.isEmpty()) {
                            manifestListName = previousSnapshot.getBaseManifestList();
                        }
                        
                        ManifestList previousManifestList;
                        if (manifestListName != null) {
                            long snapshotId = previousSnapshot.getId();
                            if (manifestListName.startsWith("manifest-list-delta-")) {
                                previousManifestList = ManifestList.loadDelta(pathFactory, 
                                    identifier.getDatabase(), identifier.getTable(), snapshotId);
                            } else if (manifestListName.startsWith("manifest-list-base-")) {
                                previousManifestList = ManifestList.loadBase(pathFactory, 
                                    identifier.getDatabase(), identifier.getTable(), snapshotId);
                            } else {
                                // 兼容旧格式
                                previousManifestList = ManifestList.load(pathFactory, 
                                    identifier.getDatabase(), identifier.getTable(), snapshotId);
                            }
                        } else {
                            throw new IOException("No manifest list found in previous snapshot " + previousSnapshot.getId());
                        }
                        
                        // 遍历所有 manifest files，收集所有的 ADD 文件
                        for (ManifestFileMeta manifestMeta : previousManifestList.getManifestFiles()) {
                            ManifestFile manifestFile = ManifestFile.load(
                                pathFactory, identifier.getDatabase(), identifier.getTable(), 
                                manifestMeta.getFileName());
                            
                            for (ManifestEntry oldEntry : manifestFile.getEntries()) {
                                // 只处理之前是 ADD 的文件，标记为 DELETE
                                if (oldEntry.getKind() == ManifestEntry.FileKind.ADD) {
                                    ManifestEntry deleteEntry = ManifestEntry.deleteFile(
                                        oldEntry.getFile().getFileName(),
                                        oldEntry.getFile().getFileSize(),
                                        oldEntry.getFile().getSchemaId(),
                                        oldEntry.getMinKey(),
                                        oldEntry.getMaxKey(),
                                        oldEntry.getFile().getRowCount(),
                                        oldEntry.getFile().getLevel()
                                    );
                                    manifestEntries.add(deleteEntry);
                                }
                            }
                        }
                        
                        logger.info("OVERWRITE mode: marked {} old files as DELETE", 
                            manifestEntries.size() - commitMessage.getNewFiles().size());
                        
                    } catch (IOException e) {
                        logger.warn("Failed to load previous snapshot for OVERWRITE", e);
                    }
                }
            }
            
            // 5. 对于 OVERWRITE 提交，即使没有数据文件也要创建快照（表示数据被清空）
            if (manifestEntries.isEmpty() && commitKind != Snapshot.CommitKind.OVERWRITE) {
                logger.warn("No data files to commit for table {}", identifier);
                return;
            }
            
            // 6. 创建 Snapshot（通过 Manifest 控制数据文件的可见性）
            Snapshot snapshot = snapshotManager.createSnapshot(
                commitMessage.getSchemaId(), 
                manifestEntries, 
                commitKind
            );
            
            // 7. 原子性提交到 Catalog
            try {
                catalog.commitSnapshot(identifier, snapshot);
                
                // 8. 更新成功提交的 commitIdentifier
                lastCommittedIdentifier = commitMessage.getCommitIdentifier();
                
                // 9. 如果有事务管理器，提交事务
                if (transactionManager != null && transactionId != null) {
                    transactionManager.commitTransaction(transactionId, snapshot.getId());
                }
                
                // 10. 清理 WAL 文件和目录（提交成功后数据已持久化）
                cleanupWALDirectory();
                
                long duration = System.currentTimeMillis() - startTime;
                logger.info("Successfully committed snapshot {} to table {}, took {}ms, files={}",
                    snapshot.getId(), identifier, duration, manifestEntries.size());
                    
            } catch (CatalogException e) {
                // 提交失败，记录错误并抛出异常
                logger.error("Failed to commit snapshot {} to table {}",
                    snapshot.getId(), identifier, e);
                
                // 如果有事务管理器，回滚事务
                if (transactionManager != null && transactionId != null) {
                    try {
                        transactionManager.abortTransaction(transactionId);
                    } catch (Exception abortEx) {
                        logger.error("Failed to abort transaction " + transactionId, abortEx);
                    }
                }
                
                throw new IOException("Failed to commit snapshot", e);
            } catch (Exception e) {
                // 其他异常
                logger.error("Unexpected error during commit for table {}", identifier, e);
                
                // 如果有事务管理器，回滚事务
                if (transactionManager != null && transactionId != null) {
                    try {
                        transactionManager.abortTransaction(transactionId);
                    } catch (Exception abortEx) {
                        logger.error("Failed to abort transaction " + transactionId, abortEx);
                    }
                }
                
                throw new IOException("Commit failed", e);
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
     * 清理 WAL 目录及其所有文件
     * 在提交成功后调用，此时数据已持久化到 SSTable，WAL 不再需要
     */
    private void cleanupWALDirectory() {
        try {
            String database = identifier.getDatabase();
            String table = identifier.getTable();
            Path walDir = pathFactory.getWalDir(database, table);
            
            if (!Files.exists(walDir)) {
                logger.debug("WAL directory does not exist: {}", walDir);
                return;
            }
            
            // 删除 WAL 目录下的所有文件
            try (java.util.stream.Stream<Path> stream = Files.list(walDir)) {
                stream.forEach(walFile -> {
                    try {
                        Files.deleteIfExists(walFile);
                        logger.debug("Deleted WAL file: {}", walFile);
                    } catch (IOException e) {
                        logger.warn("Failed to delete WAL file {}: {}", walFile, e.getMessage());
                    }
                });
            }
            
            // 删除 WAL 目录本身（如果为空）
            try {
                Files.delete(walDir);
                logger.info("Deleted WAL directory: {}", walDir);
            } catch (java.nio.file.DirectoryNotEmptyException e) {
                // 目录不为空，可能有其他并发写入的 WAL，保留目录
                logger.debug("WAL directory not empty, keeping it: {}", walDir);
            } catch (IOException e) {
                logger.warn("Failed to delete WAL directory {}: {}", walDir, e.getMessage());
            }
            
        } catch (IOException e) {
            // WAL 清理失败不影响提交结果，只记录警告
            logger.warn("Failed to cleanup WAL directory for table {}: {}", identifier, e.getMessage());
        }
    }
    
    /**
     * 中止提交
     * 
     * 注意：
     * - Paimon 中数据文件已在 prepareCommit 时写入最终目录
     * - 通过 Manifest 的可见性控制保证原子性
     * - abort 不需要删除数据文件，未被 Snapshot 引用的文件对外不可见
     */
    public void abort() {
        logger.info("Aborting commit for table {}", identifier);
    }
}
