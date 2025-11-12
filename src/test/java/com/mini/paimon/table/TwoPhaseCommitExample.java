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
import com.mini.paimon.utils.PathFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

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
    
    public static void main(String[] args) throws Exception {
        String warehousePath = "warehouse_2pc_demo";
        PathFactory pathFactory = new PathFactory(warehousePath);
        
        CatalogContext context = CatalogContext.builder()
            .warehouse(warehousePath)
            .build();
        
        Catalog catalog = new FileSystemCatalog("demo_catalog", "default", context);
        
        try {
            catalog.createDatabase("test_db", true);
            
            Schema schema = new Schema(
                1,
                Arrays.asList(
                    new Field("id", DataType.INT, false),
                    new Field("name", DataType.STRING, false),
                    new Field("age", DataType.INT, false)
                ),
                Arrays.asList("id"),
                Arrays.asList()
            );
            
            Identifier tableId = new Identifier("test_db", "users");
            catalog.createTable(tableId, schema, true);
            
            System.out.println("=== Paimon 两阶段提交示例 ===\n");
            
            Table table = catalog.getTable(tableId);
            TableWrite tableWrite = new TableWrite(table, 100);
            
            System.out.println("1. Write 阶段：写入数据到 MemTable");
            tableWrite.write(new Row(new Object[]{1, "Alice", 25}));
            tableWrite.write(new Row(new Object[]{2, "Bob", 30}));
            tableWrite.write(new Row(new Object[]{3, "Charlie", 35}));
            System.out.println("   - 数据已写入内存表 (MemTable)");
            
            System.out.println("\n2. PrepareCommit 阶段：刷写数据到最终目录");
            TableWrite.TableCommitMessage message = tableWrite.prepareCommit();
            System.out.println("   - 数据文件已写入最终目录: data/");
            System.out.println("   - 收集到 " + message.getNewFiles().size() + " 个数据文件的元信息");
            System.out.println("   - 此时数据文件存在但对外不可见（未被 Snapshot 引用）");
            
            Path dataDir = pathFactory.getDataDir("test_db", "users");
            if (Files.exists(dataDir)) {
                long fileCount = Files.list(dataDir).filter(p -> p.toString().endsWith(".sst")).count();
                System.out.println("   - 实际文件数量: " + fileCount);
            }
            
            System.out.println("\n3. Commit 阶段：通过 Snapshot 使数据可见");
            TableCommit tableCommit = new TableCommit(catalog, pathFactory, tableId);
            tableCommit.commit(message);
            System.out.println("   - 创建 Manifest 文件，记录数据文件变更");
            System.out.println("   - 创建 Snapshot，引用 Manifest");
            System.out.println("   - 原子性提交 Snapshot (更新 LATEST 指针)");
            System.out.println("   - 数据现在对外可见！");
            
            tableWrite.markCommitted();
            
            System.out.println("\n4. 验证提交结果");
            if (Snapshot.hasLatestSnapshot(pathFactory, "test_db", "users")) {
                Snapshot snapshot = Snapshot.loadLatest(pathFactory, "test_db", "users");
                System.out.println("   - Snapshot ID: " + snapshot.getId());
                System.out.println("   - 提交类型: " + snapshot.getCommitKind());
                System.out.println("   - Schema ID: " + snapshot.getSchemaId());
            }
            
            tableWrite.close();
            
            System.out.println("\n=== 关键点总结 ===");
            System.out.println("✓ 数据文件直接写入最终目录 (data/)，而不是临时目录");
            System.out.println("✓ PrepareCommit 只负责刷盘和收集元信息，不修改元数据");
            System.out.println("✓ Commit 通过 Manifest + Snapshot 控制数据可见性");
            System.out.println("✓ 原子性由 Snapshot 的原子提交保证，而非文件重命名");
            System.out.println("✓ 失败的提交不会影响系统（数据文件存在但不可见）");
            
        } finally {
            catalog.close();
            deleteDirectory(new File(warehousePath));
        }
    }
    
    private static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
}
