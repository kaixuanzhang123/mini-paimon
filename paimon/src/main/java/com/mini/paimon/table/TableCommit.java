package com.mini.paimon.table;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.operation.FileStoreCommitImpl;
import com.mini.paimon.operation.ManifestCommittable;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * Table Commit
 * 参考Apache Paimon的TableCommit设计
 * 
 * 简化版本，委托给FileStoreCommitImpl处理复杂的提交逻辑
 * 负责：
 * 1. 接收TableWrite的CommitMessage
 * 2. 转换为ManifestCommittable
 * 3. 调用FileStoreCommitImpl进行提交
 */
public class TableCommit {
    private static final Logger logger = LoggerFactory.getLogger(TableCommit.class);
    
    private final Catalog catalog;
    private final PathFactory pathFactory;
    private final Identifier identifier;
    private final Schema schema;
    private final FileStoreCommitImpl fileStoreCommit;
    private final String commitUser;
    
    public TableCommit(Catalog catalog, PathFactory pathFactory, Identifier identifier, Schema schema) {
        this.catalog = catalog;
        this.pathFactory = pathFactory;
        this.identifier = identifier;
        this.schema = schema;
        
        // 创建SnapshotManager
        SnapshotManager snapshotManager = new SnapshotManager(
            pathFactory,
            identifier.getDatabase(),
            identifier.getTable(),
            null
        );
        
        // 创建FileStoreCommitImpl
        this.fileStoreCommit = new FileStoreCommitImpl(
            pathFactory,
            snapshotManager,
            schema,
            identifier.getDatabase(),
            identifier.getTable(),
            "main"
        );
        
        // 生成commitUser（使用UUID）
        this.commitUser = UUID.randomUUID().toString();
        
        logger.info("Created TableCommit for table {}, commitUser: {}", identifier, commitUser);
    }
    
    /**
     * 提交写入操作
     * 
     * @param commitMessage TableWrite的准备提交消息
     * @throws IOException 提交失败
     */
    public void commit(TableWrite.TableCommitMessage commitMessage) throws IOException {
        commit(commitMessage, Snapshot.CommitKind.APPEND);
    }
    
    /**
     * 提交写入操作（指定提交类型）
     * 
     * @param commitMessage 提交消息
     * @param commitKind 提交类型
     * @throws IOException 提交失败
     */
    public void commit(TableWrite.TableCommitMessage commitMessage, Snapshot.CommitKind commitKind) 
            throws IOException {
        
        logger.info("Starting commit for table {}, commitIdentifier={}, kind={}, files={}",
                   identifier, commitMessage.getCommitIdentifier(), commitKind,
                   commitMessage.getNewFiles().size());
        
        try {
            // 1. 验证提交消息
            validateCommitMessage(commitMessage);
            
            // 2. 创建ManifestCommittable
            ManifestCommittable committable = ManifestCommittable.builder()
                .commitIdentifier(commitMessage.getCommitIdentifier())
                .commitUser(commitUser)
                .appendTableFiles(commitMessage.getNewFiles())
                .commitKind(commitKind)
                .build();
            
            // 3. 委托给FileStoreCommitImpl进行提交
            int generatedSnapshots = fileStoreCommit.commit(committable);
            
            logger.info("Commit completed for table {}, generated {} snapshots",
                       identifier, generatedSnapshots);
            
        } catch (IOException e) {
            logger.error("Commit failed for table {}", identifier, e);
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error during commit for table {}", identifier, e);
            throw new IOException("Commit failed", e);
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
     * 中止提交
     * 
     * 在新的设计中，abort不需要做任何事情
     * 因为数据文件已在prepareCommit时写入，未被snapshot引用的文件对外不可见
     */
    public void abort() {
        logger.info("Aborting commit for table {} (no-op in new design)", identifier);
    }
}
