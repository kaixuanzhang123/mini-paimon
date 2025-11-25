package com.mini.paimon.operation;

import com.mini.paimon.io.SnapshotFile;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 使用原子 rename 实现的 Snapshot 提交
 * 参考 Apache Paimon 的 RenamingSnapshotCommit
 * 
 * 流程：
 * 1. 使用 SnapshotFile 写入临时文件
 * 2. 原子 rename 到目标位置
 * 3. 更新 latest hint
 * 
 * 适用于 HDFS 和本地文件系统（支持原子 rename）
 * 对于对象存储（S3/OSS），需要使用外部锁
 * 
 * @see SnapshotFile 负责 Snapshot 的 I/O 操作
 */
public class AtomicRenameSnapshotCommit implements SnapshotCommit {
    private static final Logger logger = LoggerFactory.getLogger(AtomicRenameSnapshotCommit.class);
    
    private final PathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final String database;
    private final String table;
    
    // 新增：使用 SnapshotFile 进行 I/O 操作（对齐 Paimon 设计）
    private final SnapshotFile snapshotFile;
    
    public AtomicRenameSnapshotCommit(PathFactory pathFactory, SnapshotManager snapshotManager,
                                     String database, String table) {
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.database = database;
        this.table = table;
        
        // 初始化 SnapshotFile（对齐 Paimon 设计）
        this.snapshotFile = new SnapshotFile(pathFactory, database, table);
    }
    
    @Override
    public boolean commit(Snapshot snapshot, String branch) throws Exception {
        long snapshotId = snapshot.getId();
        
        // 1. 使用 SnapshotFile 写入临时文件
        Path tempPath = snapshotFile.writeTempSnapshot(snapshot, snapshotId);
        Path targetPath = snapshotFile.getSnapshotPath(snapshotId);
        
        try {
            // 2. 原子 rename（如果目标文件已存在则失败）
            if (Files.exists(targetPath)) {
                // Snapshot ID 已被占用，删除临时文件
                Files.deleteIfExists(tempPath);
                logger.warn("Snapshot {} already exists, commit failed", snapshotId);
                return false;
            }
            
            // 执行原子 rename
            Files.move(tempPath, targetPath, StandardCopyOption.ATOMIC_MOVE);
            
            // 3. 更新 latest hint
            snapshotManager.commitLatestHint(snapshotId);
            
            logger.info("Successfully committed snapshot {} using atomic rename", snapshotId);
            return true;
            
        } catch (IOException e) {
            // 清理临时文件
            try {
                Files.deleteIfExists(tempPath);
            } catch (IOException cleanupEx) {
                logger.warn("Failed to cleanup temp file: {}", tempPath, cleanupEx);
            }
            
            // 检查是否是因为文件已存在
            if (Files.exists(targetPath)) {
                logger.warn("Snapshot {} already exists after exception, assuming conflict", snapshotId);
                return false;
            }
            
            throw e;
        }
    }
}
