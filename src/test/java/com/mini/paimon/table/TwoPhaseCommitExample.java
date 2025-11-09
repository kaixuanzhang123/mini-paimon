package com.mini.paimon.table;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.snapshot.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 两阶段提交示例
 * 演示 Mini Paimon 的两阶段提交机制
 * 
 * 两阶段提交流程：
 * 1. Prepare 阶段：TableWrite.prepareCommit() - 刷盘并生成 CommitMessage
 * 2. Commit 阶段：TableCommit.commit() - 原子性提交 Snapshot
 * 
 * 核心设计特点：
 * - 幂等性：支持重复提交同一个 CommitMessage
 * - 原子性：使用锁保证同一时刻只有一个提交操作
 * - 可靠性：Prepare 阶段数据已刷盘，Commit 失败可重试
 */
public class TwoPhaseCommitExample {
    private static final Logger logger = LoggerFactory.getLogger(TwoPhaseCommitExample.class);
    
    public static void main(String[] args) {
        try {
            logger.info("=== Two-Phase Commit Example ===");
            
            // 1. 创建 Catalog
            CatalogContext context = CatalogContext.builder()
                .warehouse("./warehouse-2pc-example")
                .build();
            
            Catalog catalog = new FileSystemCatalog("test_catalog", "default", context);
            
            // 2. 创建数据库和表
            catalog.createDatabase("test_db", true);
            
            Identifier tableId = new Identifier("test_db", "order_table");
            Schema schema = new Schema(
                0,
                Arrays.asList(
                    new Field("order_id", DataType.STRING, false),
                    new Field("user_id", DataType.INT, false),
                    new Field("amount", DataType.DOUBLE, false),
                    new Field("status", DataType.STRING, false)
                ),
                Collections.singletonList("order_id"),
                Collections.emptyList()
            );
            
            catalog.createTable(tableId, schema, true);
            logger.info("Created table: {}", tableId);
            
            // 3. 获取 Table 实例
            Table table = catalog.getTable(tableId);
            
            // ===== 第一次提交：正常流程 =====
            logger.info("\n=== Commit #1: Normal Flow ===");
            TableWrite.TableCommitMessage commitMsg1 = executeWriteAndPrepare(table, 
                Arrays.asList(
                    new Row(new Object[]{"ORDER001", 1001, 99.99, "PENDING"}),
                    new Row(new Object[]{"ORDER002", 1002, 199.99, "CONFIRMED"})
                )
            );
            
            logger.info("Commit Message #1: {}", commitMsg1);
            
            // 提交
            TableCommit commit1 = table.newCommit();
            commit1.commit(commitMsg1);
            
            // 查询快照
            Snapshot snapshot1 = catalog.getLatestSnapshot(tableId);
            logger.info("Snapshot #1: id={}, commitKind={}, files={}, records={}", 
                snapshot1.getId(), snapshot1.getCommitKind(), 
                snapshot1.getDeltaRecordCount(), snapshot1.getTotalRecordCount());
            
            // ===== 第二次提交：测试幂等性 =====
            logger.info("\n=== Commit #2: Test Idempotency ===");
            TableWrite.TableCommitMessage commitMsg2 = executeWriteAndPrepare(table,
                Arrays.asList(
                    new Row(new Object[]{"ORDER003", 1003, 299.99, "PENDING"}),
                    new Row(new Object[]{"ORDER004", 1004, 399.99, "CONFIRMED"})
                )
            );
            
            logger.info("Commit Message #2: {}", commitMsg2);
            
            // 第一次提交
            TableCommit commit2 = table.newCommit();
            commit2.commit(commitMsg2);
            logger.info("First commit succeeded");
            
            // 重复提交同一个 CommitMessage（测试幂等性）
            TableCommit commit2Retry = table.newCommit();
            commit2Retry.commit(commitMsg2);
            logger.info("Duplicate commit skipped (idempotent)");
            
            // 验证：快照应该只增加一次
            Snapshot snapshot2 = catalog.getLatestSnapshot(tableId);
            logger.info("Snapshot #2: id={}, commitKind={}, files={}, records={}", 
                snapshot2.getId(), snapshot2.getCommitKind(), 
                snapshot2.getDeltaRecordCount(), snapshot2.getTotalRecordCount());
            
            // ===== 第三次提交：测试 Abort =====
            logger.info("\n=== Commit #3: Test Abort ===");
            
            TableWrite writer3 = table.newWrite();
            try {
                writer3.write(new Row(new Object[]{"ORDER005", 1005, 499.99, "PENDING"}));
                
                // 模拟业务异常，中止提交
                logger.info("Business logic failed, aborting...");
                writer3.abort();
                logger.info("Write aborted successfully");
                
            } finally {
                writer3.close();
            }
            
            // 验证：快照数量不应该增加
            Snapshot snapshot3 = catalog.getLatestSnapshot(tableId);
            logger.info("Snapshot after abort: id={}, same as previous: {}", 
                snapshot3.getId(), snapshot3.getId() == snapshot2.getId());
            
            // ===== 第四次提交：Overwrite 模式 =====
            logger.info("\n=== Commit #4: Overwrite Mode ===");
            TableWrite.TableCommitMessage commitMsg4 = executeWriteAndPrepare(table,
                Arrays.asList(
                    new Row(new Object[]{"ORDER006", 1006, 599.99, "CONFIRMED"})
                )
            );
            
            TableCommit commit4 = table.newCommit();
            commit4.commit(commitMsg4, Snapshot.CommitKind.OVERWRITE);
            logger.info("Overwrite commit succeeded");
            
            Snapshot snapshot4 = catalog.getLatestSnapshot(tableId);
            logger.info("Snapshot #4: id={}, commitKind={}, totalRecords={}", 
                snapshot4.getId(), snapshot4.getCommitKind(), snapshot4.getTotalRecordCount());
            
            // ===== 查看所有快照 =====
            logger.info("\n=== All Snapshots ===");
            List<Snapshot> allSnapshots = catalog.listSnapshots(tableId);
            for (Snapshot snapshot : allSnapshots) {
                logger.info("Snapshot {}: kind={}, deltaRecords={}, totalRecords={}, time={}", 
                    snapshot.getId(), snapshot.getCommitKind(), 
                    snapshot.getDeltaRecordCount(), snapshot.getTotalRecordCount(),
                    new java.util.Date(snapshot.getTimeMillis()));
            }
            
            // 清理
            catalog.close();
            logger.info("\n=== Two-Phase Commit Example Completed Successfully ===");
            
        } catch (Exception e) {
            logger.error("Error in two-phase commit example", e);
            System.exit(1);
        }
    }
    
    /**
     * 执行写入并准备提交
     */
    private static TableWrite.TableCommitMessage executeWriteAndPrepare(
            Table table, List<Row> rows) throws Exception {
        
        TableWrite writer = table.newWrite();
        try {
            // 写入数据
            for (Row row : rows) {
                writer.write(row);
            }
            
            // Prepare 阶段：刷盘并生成 CommitMessage
            TableWrite.TableCommitMessage commitMessage = writer.prepareCommit();
            logger.info("Prepared commit: {} files, commitIdentifier={}", 
                commitMessage.getNewFiles().size(), commitMessage.getCommitIdentifier());
            
            return commitMessage;
            
        } finally {
            writer.close();
        }
    }
}
