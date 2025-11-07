package com.mini.paimon.snapshot;

import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.utils.PathFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Snapshot 测试类
 */
class SnapshotTest {
    private SnapshotManager snapshotManager;
    private PathFactory pathFactory;
    private String testWarehousePath;

    @BeforeEach
    void setUp() throws IOException {
        // 创建临时测试目录
        testWarehousePath = "./test-warehouse-snapshot";
        pathFactory = new PathFactory(testWarehousePath);
        
        // 创建表目录
        pathFactory.createTableDirectories("test_db", "test_table");
        
        // 创建 SnapshotManager
        snapshotManager = new SnapshotManager(pathFactory, "test_db", "test_table");
    }

    @AfterEach
    void tearDown() throws IOException {
        // 清理测试目录
        deleteDirectory(Paths.get(testWarehousePath));
    }

    @Test
    void testCreateSnapshot() throws IOException {
        // 创建 Schema
        Field idField = new Field("id", DataType.INT, false);
        Schema schema = new Schema(0, Collections.singletonList(idField), Collections.singletonList("id"));
        
        // 创建 Manifest 条目
        ManifestEntry entry = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            "./data/data-0-001.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, 1}),
            new RowKey(new byte[]{0, 0, 0, 10}),
            100
        );
        
        // 创建快照
        Snapshot snapshot = snapshotManager.createSnapshot(schema.getSchemaId(), Collections.singletonList(entry));
        
        assertNotNull(snapshot);
        assertEquals(0, snapshot.getSnapshotId());
        assertEquals(0, snapshot.getSchemaId());
        assertNotNull(snapshot.getCommitTime());
        assertNotNull(snapshot.getManifestList());
        
        // 验证文件已创建
        Path snapshotPath = pathFactory.getSnapshotPath("test_db", "test_table", 0);
        assertTrue(Files.exists(snapshotPath));
        
        Path manifestListPath = pathFactory.getManifestListPath("test_db", "test_table", 0);
        assertTrue(Files.exists(manifestListPath));
        
        Path latestPath = pathFactory.getLatestSnapshotPath("test_db", "test_table");
        assertTrue(Files.exists(latestPath));
    }

    @Test
    void testLoadSnapshot() throws IOException {
        // 创建 Schema
        Field idField = new Field("id", DataType.INT, false);
        Schema schema = new Schema(0, Collections.singletonList(idField), Collections.singletonList("id"));
        
        // 创建 Manifest 条目
        ManifestEntry entry = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            "./data/data-0-001.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, 1}),
            new RowKey(new byte[]{0, 0, 0, 10}),
            100
        );
        
        // 创建快照
        Snapshot originalSnapshot = snapshotManager.createSnapshot(schema.getSchemaId(), Collections.singletonList(entry));
        
        // 重新加载快照
        Snapshot loadedSnapshot = snapshotManager.getSnapshot(0);
        
        assertNotNull(loadedSnapshot);
        assertEquals(originalSnapshot, loadedSnapshot);
        assertEquals(0, loadedSnapshot.getSnapshotId());
        assertEquals(0, loadedSnapshot.getSchemaId());
    }

    @Test
    void testGetLatestSnapshot() throws IOException {
        // 创建 Schema
        Field idField = new Field("id", DataType.INT, false);
        Schema schema = new Schema(0, Collections.singletonList(idField), Collections.singletonList("id"));
        
        // 创建多个快照
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
            100
        );
        
        Snapshot snapshot1 = snapshotManager.createSnapshot(schema.getSchemaId(), Collections.singletonList(entry1));
        Snapshot snapshot2 = snapshotManager.createSnapshot(schema.getSchemaId(), Collections.singletonList(entry2));
        
        // 获取最新快照
        Snapshot latestSnapshot = snapshotManager.getLatestSnapshot();
        
        assertNotNull(latestSnapshot);
        assertEquals(1, latestSnapshot.getSnapshotId());
        assertEquals(snapshot2, latestSnapshot);
    }

    @Test
    void testHasSnapshot() throws IOException {
        assertFalse(snapshotManager.hasSnapshot());
        
        // 创建 Schema
        Field idField = new Field("id", DataType.INT, false);
        Schema schema = new Schema(0, Collections.singletonList(idField), Collections.singletonList("id"));
        
        // 创建 Manifest 条目
        ManifestEntry entry = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            "./data/data-0-001.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, 1}),
            new RowKey(new byte[]{0, 0, 0, 10}),
            100
        );
        
        // 创建快照
        snapshotManager.createSnapshot(schema.getSchemaId(), Collections.singletonList(entry));
        
        assertTrue(snapshotManager.hasSnapshot());
    }

    @Test
    void testGetSnapshotCount() throws IOException {
        assertEquals(0, snapshotManager.getSnapshotCount());
        
        // 创建 Schema
        Field idField = new Field("id", DataType.INT, false);
        Schema schema = new Schema(0, Collections.singletonList(idField), Collections.singletonList("id"));
        
        // 创建 Manifest 条目
        ManifestEntry entry = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            "./data/data-0-001.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, 1}),
            new RowKey(new byte[]{0, 0, 0, 10}),
            100
        );
        
        // 创建快照
        snapshotManager.createSnapshot(schema.getSchemaId(), Collections.singletonList(entry));
        
        assertEquals(1, snapshotManager.getSnapshotCount());
    }

    /**
     * 递归删除目录
     */
    private void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted(java.util.Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(java.io.File::delete);
        }
    }
}
