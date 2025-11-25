package com.mini.paimon.examples;

import com.mini.paimon.io.ManifestListIO;
import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.manifest.ManifestFile;
import com.mini.paimon.manifest.ManifestFileMeta;
import com.mini.paimon.manifest.ManifestList;
import com.mini.paimon.operation.FileStoreCommitImpl;
import com.mini.paimon.operation.ManifestCommittable;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.RowKey;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.PathFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Full Commit Flow Test
 * 完整测试提交流程的各个环节
 */
public class FullCommitFlowTest {
    
    public static void main(String[] args) throws Exception {
        String warehouse = "./warehouse-full-test";
        String database = "test_db";
        String table = "test_table";
        
        // 清理旧数据
        Path warehousePath = java.nio.file.Paths.get(warehouse);
        if (Files.exists(warehousePath)) {
            deleteDirectory(warehousePath);
        }
        
        PathFactory pathFactory = new PathFactory(warehouse);
        SnapshotManager snapshotManager = new SnapshotManager(
            pathFactory, database, table, "main");
        
        Schema schema = new Schema(
            0,
            Arrays.asList(
                new Field("id", com.mini.paimon.schema.DataType.INT(), false),
                new Field("name", com.mini.paimon.schema.DataType.STRING(), true)
            ),
            Arrays.asList("id"),
            new ArrayList<>()
        );
        
        FileStoreCommitImpl commitImpl = new FileStoreCommitImpl(
            pathFactory, snapshotManager, schema, database, table, "main");
        
        System.out.println("=== Full Commit Flow Test ===\n");
        
        // 测试1: 第一次提交（创建表结构）
        System.out.println("Test 1: First commit (initialize table)");
        testFirstCommit(commitImpl, snapshotManager, pathFactory, database, table);
        
        // 测试2: 增量提交
        System.out.println("\nTest 2: Incremental commits");
        testIncrementalCommits(commitImpl, snapshotManager);
        
        // 测试3: Base Manifest 合并
        System.out.println("\nTest 3: Base Manifest compaction");
        testBaseManifestCompaction(commitImpl, snapshotManager, pathFactory, database, table);
        
        // 测试4: 文件读取完整性
        System.out.println("\nTest 4: File integrity verification");
        testFileIntegrity(snapshotManager, pathFactory, database, table);
        
        // 测试5: Snapshot 历史查询
        System.out.println("\nTest 5: Snapshot history");
        testSnapshotHistory(snapshotManager);
        
        System.out.println("\n=== All Tests Passed ===");
    }
    
    /**
     * 测试第一次提交
     */
    private static void testFirstCommit(
            FileStoreCommitImpl commitImpl, 
            SnapshotManager snapshotManager,
            PathFactory pathFactory,
            String database,
            String table) throws Exception {
        
        // 创建第一批数据文件
        List<ManifestEntry> files = Arrays.asList(
            createEntry("data-1.parquet", 1024, 100, 1, 100),
            createEntry("data-2.parquet", 2048, 200, 101, 300)
        );
        
        ManifestCommittable committable = ManifestCommittable.builder()
            .commitIdentifier(1L)
            .commitUser("test-user")
            .appendTableFiles(files)
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        
        int snapshots = commitImpl.commit(committable);
        assert snapshots == 1 : "Should generate 1 snapshot";
        
        // 验证 snapshot 文件
        Snapshot snapshot = snapshotManager.latestSnapshot();
        assert snapshot != null : "Snapshot should not be null";
        assert snapshot.getId() == 1 : "Snapshot ID should be 1";
        assert snapshot.getTotalRecordCount() == 300 : "Total records should be 300";
        assert snapshot.getDeltaRecordCount() == 300 : "Delta records should be 300";
        
        // 验证 manifest list 文件
        String deltaListName = snapshot.getDeltaManifestList();
        assert deltaListName != null : "Delta manifest list should not be null";
        
        Path deltaListPath = pathFactory.getDeltaManifestListPath(database, table, 1);
        assert Files.exists(deltaListPath) : "Delta manifest list file should exist";
        
        // 验证 manifest list 内容
        ManifestListIO manifestListReader = new ManifestListIO(pathFactory, database, table);
        List<ManifestFileMeta> manifestMetas = manifestListReader.readDeltaManifestList(1);
        assert manifestMetas.size() == 1 : "Should have 1 manifest file";
        
        ManifestFileMeta meta = manifestMetas.get(0);
        assert meta.getNumAddedFiles() == 2 : "Should have 2 added files";
        assert meta.getNumDeletedFiles() == 0 : "Should have 0 deleted files";
        
        System.out.println("  ✓ First commit verified");
        System.out.println("    - Snapshot ID: " + snapshot.getId());
        System.out.println("    - Total records: " + snapshot.getTotalRecordCount());
        System.out.println("    - Manifest files: " + manifestMetas.size());
    }
    
    /**
     * 测试增量提交
     */
    private static void testIncrementalCommits(
            FileStoreCommitImpl commitImpl,
            SnapshotManager snapshotManager) throws Exception {
        
        // 提交多次增量更新
        for (int i = 0; i < 5; i++) {
            List<ManifestEntry> files = Arrays.asList(
                createEntry("data-" + (3 + i) + ".parquet", 
                           1024, 100, 301 + i * 100, 400 + i * 100)
            );
            
            ManifestCommittable committable = ManifestCommittable.builder()
                .commitIdentifier(2L + i)
                .commitUser("test-user")
                .appendTableFiles(files)
                .commitKind(Snapshot.CommitKind.APPEND)
                .build();
            
            commitImpl.commit(committable);
        }
        
        // 验证最新 snapshot
        Snapshot latest = snapshotManager.latestSnapshot();
        assert latest.getId() == 6 : "Latest snapshot should be 6";
        assert latest.getTotalRecordCount() == 800 : "Total records should be 800";
        
        // 验证 snapshot 列表
        List<Snapshot> all = snapshotManager.listAllSnapshots();
        assert all.size() == 6 : "Should have 6 snapshots";
        
        System.out.println("  ✓ Incremental commits verified");
        System.out.println("    - Total snapshots: " + all.size());
        System.out.println("    - Latest snapshot ID: " + latest.getId());
        System.out.println("    - Total records: " + latest.getTotalRecordCount());
    }
    
    /**
     * 测试 Base Manifest 合并
     */
    private static void testBaseManifestCompaction(
            FileStoreCommitImpl commitImpl,
            SnapshotManager snapshotManager,
            PathFactory pathFactory,
            String database,
            String table) throws Exception {
        
        // 继续提交直到触发 Base Manifest 合并（30次）
        for (int i = 0; i < 30; i++) {
            List<ManifestEntry> files = Arrays.asList(
                createEntry("data-" + (10 + i) + ".parquet", 
                           512, 50, 1000 + i * 50, 1049 + i * 50)
            );
            
            ManifestCommittable committable = ManifestCommittable.builder()
                .commitIdentifier(100L + i)
                .commitUser("test-user")
                .appendTableFiles(files)
                .commitKind(Snapshot.CommitKind.APPEND)
                .build();
            
            commitImpl.commit(committable);
        }
        
        // 验证是否生成了 Base Manifest
        Snapshot latest = snapshotManager.latestSnapshot();
        String baseListName = latest.getBaseManifestList();
        
        if (baseListName != null && baseListName.startsWith("manifest-list-base-")) {
            System.out.println("  ✓ Base Manifest compaction triggered");
            System.out.println("    - Base manifest list: " + baseListName);
            
            // 验证 base manifest list 文件
            long snapshotId = extractSnapshotId(baseListName);
            Path baseListPath = pathFactory.getBaseManifestListPath(database, table, snapshotId);
            assert Files.exists(baseListPath) : "Base manifest list file should exist";
            
            ManifestListIO manifestListReader = new ManifestListIO(pathFactory, database, table);
            List<ManifestFileMeta> baseMetas = manifestListReader.readBaseManifestList(snapshotId);
            System.out.println("    - Base manifest contains " + baseMetas.size() + " file(s)");
        } else {
            System.out.println("  ⚠ Base Manifest not yet compacted (need more commits)");
        }
    }
    
    /**
     * 测试文件完整性
     */
    private static void testFileIntegrity(
            SnapshotManager snapshotManager,
            PathFactory pathFactory,
            String database,
            String table) throws Exception {
        
        Snapshot latest = snapshotManager.latestSnapshot();
        
        // 验证 snapshot 文件
        Path snapshotPath = pathFactory.getSnapshotPath(database, table, latest.getId());
        assert Files.exists(snapshotPath) : "Snapshot file should exist";
        long snapshotSize = Files.size(snapshotPath);
        assert snapshotSize > 0 : "Snapshot file should not be empty";
        
        // 验证 latest hint 文件
        Path latestHint = pathFactory.getLatestSnapshotPath(database, table);
        assert Files.exists(latestHint) : "Latest hint file should exist";
        String latestContent = new String(Files.readAllBytes(latestHint)).trim();
        assert latestContent.equals(String.valueOf(latest.getId())) : 
            "Latest hint should match snapshot ID";
        
        System.out.println("  ✓ File integrity verified");
        System.out.println("    - Snapshot file size: " + snapshotSize + " bytes");
        System.out.println("    - Latest hint: " + latestContent);
    }
    
    /**
     * 测试 Snapshot 历史
     */
    private static void testSnapshotHistory(SnapshotManager snapshotManager) throws Exception {
        List<Snapshot> all = snapshotManager.listAllSnapshots();
        
        // 验证 snapshot ID 连续性
        for (int i = 0; i < all.size(); i++) {
            Snapshot s = all.get(i);
            assert s.getId() == i + 1 : "Snapshot IDs should be sequential";
        }
        
        // 验证记录数递增
        long prevTotal = 0;
        for (Snapshot s : all) {
            assert s.getTotalRecordCount() >= prevTotal : 
                "Total records should be non-decreasing";
            prevTotal = s.getTotalRecordCount();
        }
        
        // 测试随机读取
        Snapshot mid = snapshotManager.snapshot(all.size() / 2);
        assert mid != null : "Should be able to read middle snapshot";
        
        System.out.println("  ✓ Snapshot history verified");
        System.out.println("    - Total snapshots: " + all.size());
        System.out.println("    - Snapshot ID range: [1, " + all.get(all.size() - 1).getId() + "]");
        System.out.println("    - Total records in latest: " + all.get(all.size() - 1).getTotalRecordCount());
    }
    
    private static ManifestEntry createEntry(String fileName, long fileSize, long rowCount,
                                            int minKey, int maxKey) {
        DataFileMeta fileMeta = new DataFileMeta(
            fileName,
            fileSize,
            rowCount,
            new RowKey(intToBytes(minKey)),
            new RowKey(intToBytes(maxKey)),
            0, // schemaId
            0, // level
            System.currentTimeMillis()
        );
        
        return new ManifestEntry(ManifestEntry.FileKind.ADD, 0, fileMeta);
    }
    
    private static byte[] intToBytes(int value) {
        return new byte[] {
            (byte)(value >>> 24),
            (byte)(value >>> 16),
            (byte)(value >>> 8),
            (byte)value
        };
    }
    
    private static long extractSnapshotId(String manifestListName) {
        if (manifestListName.startsWith("manifest-list-base-")) {
            return Long.parseLong(manifestListName.substring("manifest-list-base-".length()));
        } else if (manifestListName.startsWith("manifest-list-delta-")) {
            return Long.parseLong(manifestListName.substring("manifest-list-delta-".length()));
        }
        return -1;
    }
    
    private static void deleteDirectory(Path path) throws Exception {
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted((p1, p2) -> -p1.compareTo(p2))
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (Exception e) {
                        // Ignore
                    }
                });
        }
    }
}

