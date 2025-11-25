package com.mini.paimon.examples;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Commit Example
 * 演示如何使用重构后的FileStoreCommitImpl进行提交
 */
public class CommitExample {
    
    public static void main(String[] args) throws Exception {
        // 1. 创建Catalog
        String warehouse = "./warehouse-commit-example";
        CatalogContext context = CatalogContext.builder()
            .warehouse(warehouse)
            .build();
        Catalog catalog = new FileSystemCatalog("test_catalog", context);
        
        // 2. 创建数据库和表
        String database = "test_db";
        String table = "test_table";
        catalog.createDatabase(database, true);
        
        List<Field> fields = Arrays.asList(
            new Field("id", com.mini.paimon.schema.DataType.INT(), false),
            new Field("name", com.mini.paimon.schema.DataType.STRING(), true),
            new Field("age", com.mini.paimon.schema.DataType.INT(), true)
        );
        
        Schema schema = new Schema(
            0,
            fields,
            Arrays.asList("id"),
            new ArrayList<>()
        );
        
        Identifier identifier = new Identifier(database, table);
        catalog.createTable(identifier, schema, true);
        
        System.out.println("✓ Created database and table");
        
        // 3. 准备提交数据
        PathFactory pathFactory = new PathFactory(warehouse);
        SnapshotManager snapshotManager = new SnapshotManager(
            pathFactory, database, table, "main");
        
        // 4. 创建FileStoreCommitImpl
        FileStoreCommitImpl commitImpl = new FileStoreCommitImpl(
            pathFactory,
            snapshotManager,
            schema,
            database,
            table,
            "main"
        );
        
        System.out.println("✓ Created FileStoreCommitImpl");
        
        // 5. 第一次提交：添加两个文件
        List<ManifestEntry> firstCommitFiles = Arrays.asList(
            createManifestEntry("data-1.parquet", 1024, 100, 1, 100, ManifestEntry.FileKind.ADD),
            createManifestEntry("data-2.parquet", 2048, 200, 101, 300, ManifestEntry.FileKind.ADD)
        );
        
        ManifestCommittable committable1 = ManifestCommittable.builder()
            .commitIdentifier(System.currentTimeMillis())
            .commitUser("user1")
            .appendTableFiles(firstCommitFiles)
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        
        int snapshots1 = commitImpl.commit(committable1);
        System.out.println("✓ First commit completed, generated " + snapshots1 + " snapshot(s)");
        
        // 6. 验证第一次提交
        Snapshot snapshot1 = snapshotManager.latestSnapshot();
        System.out.println("  - Snapshot ID: " + snapshot1.getId());
        System.out.println("  - Total records: " + snapshot1.getTotalRecordCount());
        System.out.println("  - Delta records: " + snapshot1.getDeltaRecordCount());
        
        // 7. 第二次提交：删除一个文件，添加一个新文件
        List<ManifestEntry> secondCommitFiles = Arrays.asList(
            createManifestEntry("data-1.parquet", 1024, 100, 1, 100, ManifestEntry.FileKind.DELETE),
            createManifestEntry("data-3.parquet", 3072, 300, 301, 600, ManifestEntry.FileKind.ADD)
        );
        
        ManifestCommittable committable2 = ManifestCommittable.builder()
            .commitIdentifier(System.currentTimeMillis())
            .commitUser("user1")
            .appendTableFiles(secondCommitFiles)
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        
        int snapshots2 = commitImpl.commit(committable2);
        System.out.println("✓ Second commit completed, generated " + snapshots2 + " snapshot(s)");
        
        // 8. 验证第二次提交
        Snapshot snapshot2 = snapshotManager.latestSnapshot();
        System.out.println("  - Snapshot ID: " + snapshot2.getId());
        System.out.println("  - Total records: " + snapshot2.getTotalRecordCount());
        System.out.println("  - Delta records: " + snapshot2.getDeltaRecordCount());
        
        // 9. 列出所有snapshots
        List<Snapshot> allSnapshots = snapshotManager.listAllSnapshots();
        System.out.println("\n✓ All snapshots:");
        for (Snapshot s : allSnapshots) {
            System.out.println("  - Snapshot " + s.getId() + 
                             ": " + s.getCommitKind() + 
                             ", records=" + s.getTotalRecordCount());
        }
        
        // 10. 模拟并发提交（会触发重试）
        System.out.println("\n✓ Testing concurrent commits...");
        
        Thread t1 = new Thread(() -> {
            try {
                List<ManifestEntry> files = Arrays.asList(
                    createManifestEntry("data-4.parquet", 1024, 100, 601, 700, ManifestEntry.FileKind.ADD)
                );
                
                ManifestCommittable c = ManifestCommittable.builder()
                    .commitIdentifier(System.currentTimeMillis())
                    .commitUser("user2")
                    .appendTableFiles(files)
                    .commitKind(Snapshot.CommitKind.APPEND)
                    .build();
                
                commitImpl.commit(c);
                System.out.println("  - Thread 1 committed successfully");
            } catch (Exception e) {
                System.err.println("  - Thread 1 failed: " + e.getMessage());
            }
        });
        
        Thread t2 = new Thread(() -> {
            try {
                List<ManifestEntry> files = Arrays.asList(
                    createManifestEntry("data-5.parquet", 2048, 200, 701, 900, ManifestEntry.FileKind.ADD)
                );
                
                ManifestCommittable c = ManifestCommittable.builder()
                    .commitIdentifier(System.currentTimeMillis() + 1)
                    .commitUser("user3")
                    .appendTableFiles(files)
                    .commitKind(Snapshot.CommitKind.APPEND)
                    .build();
                
                commitImpl.commit(c);
                System.out.println("  - Thread 2 committed successfully");
            } catch (Exception e) {
                System.err.println("  - Thread 2 failed: " + e.getMessage());
            }
        });
        
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        
        // 11. 最终状态
        Snapshot finalSnapshot = snapshotManager.latestSnapshot();
        System.out.println("\n✓ Final state:");
        System.out.println("  - Latest snapshot ID: " + finalSnapshot.getId());
        System.out.println("  - Total snapshots: " + allSnapshots.size());
        
        catalog.close();
        System.out.println("\n✓ Example completed successfully!");
    }
    
    /**
     * 创建ManifestEntry辅助方法
     */
    private static ManifestEntry createManifestEntry(
            String fileName, long fileSize, long rowCount,
            int minKeyValue, int maxKeyValue, ManifestEntry.FileKind kind) {
        
        DataFileMeta fileMeta = new DataFileMeta(
            fileName,
            fileSize,
            rowCount,
            new RowKey(intToBytes(minKeyValue)),
            new RowKey(intToBytes(maxKeyValue)),
            0, // schemaId
            0, // level
            System.currentTimeMillis()
        );
        
        return new ManifestEntry(kind, 0, fileMeta);
    }
    
    /**
     * 将int转换为字节数组
     */
    private static byte[] intToBytes(int value) {
        return new byte[] {
            (byte)(value >>> 24),
            (byte)(value >>> 16),
            (byte)(value >>> 8),
            (byte)value
        };
    }
}

