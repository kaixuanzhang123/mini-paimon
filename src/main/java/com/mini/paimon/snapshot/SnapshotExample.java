package com.mini.paimon.snapshot;

import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.*;
import com.mini.paimon.storage.LSMTree;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Snapshot 示例
 * 演示完善后的 Snapshot 功能
 * 
 * 参考 Apache Paimon 的设计：
 * - 支持多种提交类型（APPEND/COMPACT/OVERWRITE）
 * - 记录详细的提交信息（commitUser, commitIdentifier, timeMillis）
 * - 区分 baseManifestList 和 deltaManifestList
 * - 记录 totalRecordCount 和 deltaRecordCount
 * - 支持 EARLIEST 和 LATEST 快照指针
 */
public class SnapshotExample {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotExample.class);
    
    public static void main(String[] args) throws IOException {
        logger.info("===== Snapshot 完善功能演示 =====\n");
        
        String testDir = "/tmp/mini-paimon-snapshot-demo";
        cleanupTestDir(testDir);
        
        // 1. 创建测试 Schema
        Schema schema = createTestSchema();
        
        // 2. 演示不同类型的 Snapshot
        demonstrateSnapshotTypes(testDir, schema);
        
        // 3. 演示 Snapshot 查询
        demonstrateSnapshotQuery(testDir);
        
        logger.info("\n===== 演示完成 =====");
    }
    
    /**
     * 创建测试 Schema
     */
    private static Schema createTestSchema() {
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, false);
        Field ageField = new Field("age", DataType.INT, true);
        
        List<Field> fields = Arrays.asList(idField, nameField, ageField);
        List<String> primaryKeys = Arrays.asList("id");
        
        return new Schema(1, fields, primaryKeys);
    }
    
    /**
     * 演示不同类型的 Snapshot
     */
    private static void demonstrateSnapshotTypes(String testDir, Schema schema) throws IOException {
        logger.info("--- 1. 演示不同类型的 Snapshot ---\n");
        
        PathFactory pathFactory = new PathFactory(testDir);
        LSMTree lsmTree = new LSMTree(schema, pathFactory, "default", "test");
        SnapshotManager snapshotManager = new SnapshotManager(pathFactory, "default", "test");
        
        // 1.1 APPEND 类型快照（默认）
        logger.info("创建 APPEND 类型快照...");
        for (int i = 1; i <= 10; i++) {
            Object[] values = new Object[]{i, "User" + i, 20 + i};
            lsmTree.put(new Row(values));
        }
        lsmTree.close();
        
        Snapshot appendSnapshot = snapshotManager.getLatestSnapshot();
        logger.info("  Snapshot ID: {}", appendSnapshot.getId());
        logger.info("  Commit Kind: {}", appendSnapshot.getCommitKind());
        logger.info("  Commit User: {}", appendSnapshot.getCommitUser());
        logger.info("  Total Records: {}", appendSnapshot.getTotalRecordCount());
        logger.info("  Delta Records: {}", appendSnapshot.getDeltaRecordCount());
        logger.info("  Time: {}", appendSnapshot.getCommitTime());
        
        // 1.2 继续写入，创建更多快照
        logger.info("\n继续写入数据，创建更多快照...");
        lsmTree = new LSMTree(schema, pathFactory, "default", "test");
        for (int i = 11; i <= 20; i++) {
            Object[] values = new Object[]{i, "User" + i, 20 + i};
            lsmTree.put(new Row(values));
        }
        lsmTree.close();
        
        Snapshot secondSnapshot = snapshotManager.getLatestSnapshot();
        logger.info("  Snapshot ID: {}", secondSnapshot.getId());
        logger.info("  Total Records: {}", secondSnapshot.getTotalRecordCount());
        logger.info("  Delta Records: {}", secondSnapshot.getDeltaRecordCount());
    }
    
    /**
     * 演示 Snapshot 查询功能
     */
    private static void demonstrateSnapshotQuery(String testDir) throws IOException {
        logger.info("\n--- 2. 演示 Snapshot 查询功能 ---\n");
        
        PathFactory pathFactory = new PathFactory(testDir);
        SnapshotManager snapshotManager = new SnapshotManager(pathFactory, "default", "test");
        
        // 2.1 获取所有快照
        logger.info("获取所有快照列表:");
        List<Snapshot> allSnapshots = snapshotManager.getAllSnapshots();
        for (Snapshot snapshot : allSnapshots) {
            logger.info("  - Snapshot {}: kind={}, totalRecords={}, deltaRecords={}, time={}", 
                       snapshot.getId(),
                       snapshot.getCommitKind(),
                       snapshot.getTotalRecordCount(),
                       snapshot.getDeltaRecordCount(),
                       snapshot.getCommitTime());
        }
        
        // 2.2 获取最新快照
        logger.info("\n获取最新快照:");
        Snapshot latest = snapshotManager.getLatestSnapshot();
        logger.info("  Latest Snapshot ID: {}", latest.getId());
        logger.info("  Base Manifest List: {}", latest.getBaseManifestList());
        logger.info("  Delta Manifest List: {}", latest.getDeltaManifestList());
        
        // 2.3 获取最早快照
        logger.info("\n获取最早快照:");
        Snapshot earliest = snapshotManager.getEarliestSnapshot();
        logger.info("  Earliest Snapshot ID: {}", earliest.getId());
        logger.info("  Commit Time: {}", earliest.getCommitTime());
        
        // 2.4 快照数量
        logger.info("\n快照统计:");
        logger.info("  总快照数: {}", snapshotManager.getSnapshotCount());
        
        // 2.5 演示 Snapshot Builder
        logger.info("\n演示 Snapshot Builder:");
        Snapshot customSnapshot = new Snapshot.Builder()
            .id(999)
            .schemaId(1)
            .baseManifestList("manifest-list-999")
            .deltaManifestList("manifest-list-delta-999")
            .commitUser("custom-user")
            .commitIdentifier(999)
            .commitKind(Snapshot.CommitKind.COMPACT)
            .totalRecordCount(1000)
            .deltaRecordCount(100)
            .build();
        
        logger.info("  Custom Snapshot: {}", customSnapshot);
        logger.info("  Version: {}", customSnapshot.getVersion());
        logger.info("  Commit Kind: {}", customSnapshot.getCommitKind());
        
        // 2.6 演示 EARLIEST 和 LATEST 文件
        logger.info("\n检查快照指针文件:");
        Path snapshotDir = pathFactory.getSnapshotDir("default", "test");
        Path earliestFile = pathFactory.getEarliestSnapshotPath("default", "test");
        Path latestFile = pathFactory.getLatestSnapshotPath("default", "test");
        
        if (Files.exists(earliestFile)) {
            String earliestId = new String(Files.readAllBytes(earliestFile)).trim();
            logger.info("  EARLIEST -> snapshot-{}", earliestId);
        }
        
        if (Files.exists(latestFile)) {
            String latestId = new String(Files.readAllBytes(latestFile)).trim();
            logger.info("  LATEST -> snapshot-{}", latestId);
        }
    }
    
    /**
     * 清理测试目录
     */
    private static void cleanupTestDir(String testDir) throws IOException {
        Path testPath = Paths.get(testDir);
        if (Files.exists(testPath)) {
            Files.walk(testPath)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        // ignore
                    }
                });
        }
    }
}
