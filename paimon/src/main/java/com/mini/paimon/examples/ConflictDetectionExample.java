package com.mini.paimon.examples;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.operation.FileStoreCommitImpl;
import com.mini.paimon.operation.ManifestCommittable;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.RowKey;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.PathFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Conflict Detection Example
 * 演示MVCC并发控制和冲突检测机制
 */
public class ConflictDetectionExample {
    
    public static void main(String[] args) throws Exception {
        String warehouse = "./warehouse-conflict-example";
        String database = "test_db";
        String table = "test_table";
        
        PathFactory pathFactory = new PathFactory(warehouse);
        SnapshotManager snapshotManager = new SnapshotManager(
            pathFactory, database, table, "main");
        
        // 创建带主键的schema（用于LSM冲突检测）
        Schema schema = new Schema(
            0,
            Arrays.asList(
                new Field("id", com.mini.paimon.schema.DataType.INT(), false),
                new Field("value", com.mini.paimon.schema.DataType.STRING(), true)
            ),
            Arrays.asList("id"), // 主键
            Arrays.asList()
        );
        
        FileStoreCommitImpl commitImpl = new FileStoreCommitImpl(
            pathFactory, snapshotManager, schema, database, table, "main");
        
        System.out.println("=== MVCC Conflict Detection Example ===\n");
        
        // 场景1: 成功的顺序提交
        System.out.println("Scenario 1: Sequential commits (should succeed)");
        commitFile("file-1.parquet", 1, 100, commitImpl, "user1");
        commitFile("file-2.parquet", 101, 200, commitImpl, "user2");
        System.out.println("✓ Sequential commits succeeded\n");
        
        // 场景2: 文件删除冲突
        System.out.println("Scenario 2: File deletion conflict");
        try {
            // 提交一个新文件
            commitFile("file-3.parquet", 201, 300, commitImpl, "user3");
            
            // 尝试删除不存在的文件（应该在冲突检测时失败）
            List<ManifestEntry> deleteNonExistentFile = Arrays.asList(
                createEntry("file-999.parquet", 1000, 1, 1, 1, ManifestEntry.FileKind.DELETE)
            );
            
            ManifestCommittable badCommit = ManifestCommittable.builder()
                .commitIdentifier(System.currentTimeMillis())
                .commitUser("user4")
                .appendTableFiles(deleteNonExistentFile)
                .commitKind(Snapshot.CommitKind.APPEND)
                .build();
            
            commitImpl.commit(badCommit);
            System.out.println("✗ Should have failed but didn't!");
            
        } catch (Exception e) {
            System.out.println("✓ File deletion conflict detected: " + e.getMessage());
        }
        System.out.println();
        
        // 场景3: LSM键范围冲突（Level >= 1）
        System.out.println("Scenario 3: LSM key range conflict (primary key table)");
        try {
            // 在Level 1添加一个文件，键范围 [1000, 2000]
            List<ManifestEntry> firstLevelFile = Arrays.asList(
                createEntry("level1-file1.parquet", 2048, 1000, 1000, 2000, 
                           ManifestEntry.FileKind.ADD, 1)
            );
            
            ManifestCommittable commit1 = ManifestCommittable.builder()
                .commitIdentifier(System.currentTimeMillis())
                .commitUser("user5")
                .appendTableFiles(firstLevelFile)
                .commitKind(Snapshot.CommitKind.COMPACT)
                .build();
            
            commitImpl.commit(commit1);
            System.out.println("  - Added Level 1 file with range [1000, 2000]");
            
            // 尝试添加重叠键范围的文件 [1500, 2500]（应该失败）
            List<ManifestEntry> overlappingFile = Arrays.asList(
                createEntry("level1-file2.parquet", 2048, 1000, 1500, 2500,
                           ManifestEntry.FileKind.ADD, 1)
            );
            
            ManifestCommittable commit2 = ManifestCommittable.builder()
                .commitIdentifier(System.currentTimeMillis())
                .commitUser("user6")
                .appendTableFiles(overlappingFile)
                .commitKind(Snapshot.CommitKind.COMPACT)
                .build();
            
            commitImpl.commit(commit2);
            System.out.println("✗ Should have detected LSM conflict but didn't!");
            
        } catch (Exception e) {
            System.out.println("✓ LSM key range conflict detected: " + e.getMessage());
        }
        System.out.println();
        
        // 场景4: 去重检测（相同的commitUser + commitIdentifier）
        System.out.println("Scenario 4: Duplicate detection");
        try {
            long commitId = System.currentTimeMillis();
            String user = "user7";
            
            // 第一次提交
            List<ManifestEntry> files = Arrays.asList(
                createEntry("file-dedup.parquet", 1024, 100, 3000, 3100, ManifestEntry.FileKind.ADD)
            );
            
            ManifestCommittable commit = ManifestCommittable.builder()
                .commitIdentifier(commitId)
                .commitUser(user)
                .appendTableFiles(files)
                .commitKind(Snapshot.CommitKind.APPEND)
                .build();
            
            commitImpl.commit(commit);
            System.out.println("  - First commit with ID=" + commitId);
            
            // 第二次提交（相同的commitIdentifier，应该被去重）
            commitImpl.commit(commit);
            System.out.println("✓ Duplicate commit detected and skipped");
            
        } catch (Exception e) {
            System.out.println("✗ Unexpected error: " + e.getMessage());
        }
        System.out.println();
        
        // 场景5: 指数退避重试机制
        System.out.println("Scenario 5: Retry with exponential backoff");
        System.out.println("  - Simulating concurrent commits from 3 threads...");
        
        Thread[] threads = new Thread[3];
        for (int i = 0; i < 3; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    List<ManifestEntry> files = Arrays.asList(
                        createEntry("concurrent-" + threadId + ".parquet", 1024, 100,
                                   4000 + threadId * 100, 4000 + threadId * 100 + 99,
                                   ManifestEntry.FileKind.ADD)
                    );
                    
                    ManifestCommittable commit = ManifestCommittable.builder()
                        .commitIdentifier(System.currentTimeMillis() + threadId)
                        .commitUser("concurrent-user" + threadId)
                        .appendTableFiles(files)
                        .commitKind(Snapshot.CommitKind.APPEND)
                        .build();
                    
                    long start = System.currentTimeMillis();
                    commitImpl.commit(commit);
                    long elapsed = System.currentTimeMillis() - start;
                    
                    System.out.println("  - Thread " + threadId + " succeeded after " + elapsed + "ms");
                    
                } catch (Exception e) {
                    System.err.println("  - Thread " + threadId + " failed: " + e.getMessage());
                }
            });
        }
        
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        
        System.out.println("✓ All threads completed (some may have retried)\n");
        
        // 最终统计
        Snapshot latest = snapshotManager.latestSnapshot();
        List<Snapshot> all = snapshotManager.listAllSnapshots();
        
        System.out.println("=== Final Statistics ===");
        System.out.println("Total snapshots: " + all.size());
        System.out.println("Latest snapshot ID: " + latest.getId());
        System.out.println("Latest commit kind: " + latest.getCommitKind());
        System.out.println("Total records: " + latest.getTotalRecordCount());
        
        System.out.println("\n✓ Conflict detection example completed!");
    }
    
    private static void commitFile(String fileName, int minKey, int maxKey,
                                   FileStoreCommitImpl commitImpl, String user) throws Exception {
        List<ManifestEntry> files = Arrays.asList(
            createEntry(fileName, 1024, 100, minKey, maxKey, ManifestEntry.FileKind.ADD)
        );
        
        ManifestCommittable commit = ManifestCommittable.builder()
            .commitIdentifier(System.currentTimeMillis())
            .commitUser(user)
            .appendTableFiles(files)
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        
        commitImpl.commit(commit);
    }
    
    private static ManifestEntry createEntry(String fileName, long fileSize, long rowCount,
                                            int minKey, int maxKey, ManifestEntry.FileKind kind) {
        return createEntry(fileName, fileSize, rowCount, minKey, maxKey, kind, 0);
    }
    
    private static ManifestEntry createEntry(String fileName, long fileSize, long rowCount,
                                            int minKey, int maxKey, ManifestEntry.FileKind kind,
                                            int level) {
        DataFileMeta fileMeta = new DataFileMeta(
            fileName,
            fileSize,
            rowCount,
            new RowKey(intToBytes(minKey)),
            new RowKey(intToBytes(maxKey)),
            0, // schemaId
            level,
            System.currentTimeMillis()
        );
        
        return new ManifestEntry(kind, 0, fileMeta);
    }
    
    private static byte[] intToBytes(int value) {
        return new byte[] {
            (byte)(value >>> 24),
            (byte)(value >>> 16),
            (byte)(value >>> 8),
            (byte)value
        };
    }
}

