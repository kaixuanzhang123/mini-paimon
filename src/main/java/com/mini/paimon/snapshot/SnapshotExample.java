package com.mini.paimon.snapshot;

import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Snapshot 机制示例
 * 演示如何使用 Snapshot 和 Manifest 功能
 */
public class SnapshotExample {
    public static void main(String[] args) throws IOException {
        // 创建路径工厂
        PathFactory pathFactory = new PathFactory("./warehouse");
        
        // 创建表目录结构
        pathFactory.createTableDirectories("example_db", "example_table");
        
        // 创建表结构
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, true);
        Schema schema = new Schema(0, Arrays.asList(idField, nameField), Collections.singletonList("id"));
        
        // 创建快照管理器
        SnapshotManager snapshotManager = new SnapshotManager(pathFactory, "example_db", "example_table");
        
        // 创建一些 Manifest 条目
        ManifestEntry entry1 = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            "./data/data-0-001.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, 1}),
            new RowKey(new byte[]{0, 0, 0, 10}),
            100
        );
        
        ManifestEntry entry2 = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            "./data/data-0-002.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, 11}),
            new RowKey(new byte[]{0, 0, 0, 20}),
            50
        );
        
        // 创建快照
        Snapshot snapshot1 = snapshotManager.createSnapshot(schema.getSchemaId(), Arrays.asList(entry1, entry2));
        System.out.println("Created snapshot 1: " + snapshot1);
        
        // 创建更多条目
        ManifestEntry entry3 = new ManifestEntry(
            ManifestEntry.FileKind.DELETE,
            "./data/data-0-001.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, 1}),
            new RowKey(new byte[]{0, 0, 0, 10}),
            100
        );
        
        ManifestEntry entry4 = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            "./data/data-0-003.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, 21}),
            new RowKey(new byte[]{0, 0, 0, 30}),
            75
        );
        
        // 创建第二个快照
        Snapshot snapshot2 = snapshotManager.createSnapshot(schema.getSchemaId(), Arrays.asList(entry3, entry4));
        System.out.println("Created snapshot 2: " + snapshot2);
        
        // 获取最新快照
        Snapshot latestSnapshot = snapshotManager.getLatestSnapshot();
        System.out.println("Latest snapshot: " + latestSnapshot);
        
        // 获取所有活跃文件
        System.out.println("Active files: " + snapshotManager.getActiveFiles());
        
        System.out.println("Snapshot example completed successfully!");
    }
}