package com.mini.paimon;

import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.storage.LSMTree;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

/**
 * Mini Paimon 完整示例
 * 演示所有阶段的功能集成
 */
public class MiniPaimonExample {
    public static void main(String[] args) throws IOException {
        System.out.println("=== Mini Paimon 完整示例 ===");
        
        // 阶段一：基础设施搭建
        System.out.println("\n1. 基础设施搭建");
        String warehousePath = Paths.get("./warehouse").toAbsolutePath().toString();
        PathFactory pathFactory = new PathFactory(warehousePath);
        pathFactory.createTableDirectories("example_db", "user_table");
        
        // 创建表结构
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, true);
        Schema schema = new Schema(0, Arrays.asList(idField, nameField), Collections.singletonList("id"));
        
        System.out.println("创建表结构: " + schema);
        
        // 阶段二：LSM Tree 存储引擎
        System.out.println("\n2. LSM Tree 存储引擎");
        LSMTree lsmTree = new LSMTree(schema, pathFactory, "example_db", "user_table");
        
        // 插入数据
        Row row1 = new Row(new Object[]{1, "Alice"});
        Row row2 = new Row(new Object[]{2, "Bob"});
        Row row3 = new Row(new Object[]{3, "Charlie"});
        
        lsmTree.put(row1);
        lsmTree.put(row2);
        lsmTree.put(row3);
        System.out.println("插入数据: " + row1);
        System.out.println("插入数据: " + row2);
        System.out.println("插入数据: " + row3);
        
        // 查询数据
        RowKey key1 = RowKey.fromRow(row1, schema);
        RowKey key2 = RowKey.fromRow(row2, schema);
        Row retrievedRow1 = lsmTree.get(key1);
        Row retrievedRow2 = lsmTree.get(key2);
        System.out.println("查询数据 (id=1): " + retrievedRow1);
        System.out.println("查询数据 (id=2): " + retrievedRow2);
        
        // 阶段三：Snapshot 机制
        System.out.println("\n3. Snapshot 机制");
        SnapshotManager snapshotManager = new SnapshotManager(pathFactory, "example_db", "user_table");
        
        // 创建一些 Manifest 条目来模拟数据文件变更
        DataFileMeta fileMeta1 = new DataFileMeta(
            "./data/data-0-001.sst",
            1024,
            100,
            new RowKey(new byte[]{0, 0, 0, 1}),
            new RowKey(new byte[]{0, 0, 0, 10}),
            0,
            0,
            System.currentTimeMillis()
        );
        
        ManifestEntry entry1 = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            0,
            fileMeta1
        );
        
        DataFileMeta fileMeta2 = new DataFileMeta(
            "./data/data-0-002.sst",
            512,
            50,
            new RowKey(new byte[]{0, 0, 0, 11}),
            new RowKey(new byte[]{0, 0, 0, 20}),
            0,
            0,
            System.currentTimeMillis()
        );
        
        ManifestEntry entry2 = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            0,
            fileMeta2
        );
        
        // 创建快照
        Snapshot snapshot1 = snapshotManager.createSnapshot(schema.getSchemaId(), Arrays.asList(entry1, entry2));
        System.out.println("创建快照 1: " + snapshot1.getSnapshotId());
        
        // 创建更多条目
        DataFileMeta fileMeta3 = new DataFileMeta(
            "./data/data-0-001.sst",
            1024,
            100,
            new RowKey(new byte[]{0, 0, 0, 1}),
            new RowKey(new byte[]{0, 0, 0, 10}),
            0,
            0,
            System.currentTimeMillis()
        );
        
        ManifestEntry entry3 = new ManifestEntry(
            ManifestEntry.FileKind.DELETE,
            0,
            fileMeta3
        );
        
        DataFileMeta fileMeta4 = new DataFileMeta(
            "./data/data-0-003.sst",
            768,
            75,
            new RowKey(new byte[]{0, 0, 0, 21}),
            new RowKey(new byte[]{0, 0, 0, 30}),
            0,
            0,
            System.currentTimeMillis()
        );
        
        ManifestEntry entry4 = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            0,
            fileMeta4
        );
        
        // 创建第二个快照
        Snapshot snapshot2 = snapshotManager.createSnapshot(schema.getSchemaId(), Arrays.asList(entry3, entry4));
        System.out.println("创建快照 2: " + snapshot2.getSnapshotId());
        
        // 获取最新快照
        Snapshot latestSnapshot = snapshotManager.getLatestSnapshot();
        System.out.println("最新快照: " + latestSnapshot.getSnapshotId());
        
        // 获取所有活跃文件
        System.out.println("活跃文件数量: " + snapshotManager.getActiveFiles().size());
        
        // 关闭 LSM Tree（会触发最终的数据刷写和快照创建）
        lsmTree.close();
        System.out.println("关闭 LSM Tree");
        
        System.out.println("\n=== Mini Paimon 示例完成 ===");
        
        System.out.println("\n生成的文件结构:");
        System.out.println(warehousePath + "/");
        System.out.println("└── example_db/");
        System.out.println("    └── user_table/");
        System.out.println("        ├── schema/");
        System.out.println("        │   ├── schema-0");
        System.out.println("        │   └── schema-1");
        System.out.println("        ├── snapshot/");
        System.out.println("        │   ├── snapshot-0");
        System.out.println("        │   ├── snapshot-1");
        System.out.println("        │   └── LATEST");
        System.out.println("        ├── manifest/");
        System.out.println("        │   ├── manifest-list-0");
        System.out.println("        │   ├── manifest-list-1");
        System.out.println("        │   ├── manifest-51c241c5666141d1");
        System.out.println("        │   └── manifest-b6ba67a090b841a9");
        System.out.println("        ├── data/");
        System.out.println("        │   └── data-0-000.sst");
        System.out.println("        └── metadata");
        
        System.out.println("\n要清理测试数据，请手动运行: rm -rf " + warehousePath);
    }
}