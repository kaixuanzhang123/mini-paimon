package com.mini.paimon;

import com.mini.paimon.catalog.*;
import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.table.*;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Mini Paimon 完整示例
 * 演示所有阶段的功能集成
 */
public class MiniPaimonExample {
    public static void main(String[] args) throws IOException {
        System.out.println("=== Mini Paimon 完整示例 ===");
        
        System.out.println("\n1. 基础设施搭建");
        String warehousePath = Paths.get("./warehouse").toAbsolutePath().toString();
        PathFactory pathFactory = new PathFactory(warehousePath);
        
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, true);
        Schema schema = new Schema(0, Arrays.asList(idField, nameField), Collections.singletonList("id"));
        
        System.out.println("创建表结构: " + schema);
        
        CatalogHelper catalogHelper = new CatalogHelper(pathFactory, warehousePath, "example_db");
        catalogHelper.createTableIfNotExists("user_table", schema.getFields(), schema.getPrimaryKeys());
        
        System.out.println("\n2. Table 操作（使用新 API）");
        Table table = catalogHelper.getTable("user_table");
        
        Row row1 = new Row(new Object[]{1, "Alice"});
        Row row2 = new Row(new Object[]{2, "Bob"});
        Row row3 = new Row(new Object[]{3, "Charlie"});
        
        // 使用 newWrite() 写入数据
        try (TableWrite writer = table.newWrite()) {
            writer.write(row1);
            writer.write(row2);
            writer.write(row3);
            System.out.println("插入数据: " + row1);
            System.out.println("插入数据: " + row2);
            System.out.println("插入数据: " + row3);
        }
        
        // 使用 newScan() 和 newRead() 查询数据
        TableScan scan = table.newScan().withLatestSnapshot();
        TableScan.Plan plan = scan.plan();
        TableRead reader = table.newRead();
        List<Row> allRows = reader.read(plan);
        System.out.println("查询所有数据: " + allRows.size() + " 条记录");
        for (Row row : allRows) {
            System.out.println("  - " + row);
        }
        
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
        
        Snapshot latestSnapshot = snapshotManager.getLatestSnapshot();
        System.out.println("最新快照: " + latestSnapshot.getSnapshotId());
        
        System.out.println("活跃文件数量: " + snapshotManager.getActiveFiles().size());
        
        catalogHelper.close();
        System.out.println("关闭 Catalog");
        
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