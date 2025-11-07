package com.mini.paimon.manifest;

import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.utils.PathFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Manifest 测试类
 */
class ManifestTest {
    private PathFactory pathFactory;
    private String testWarehousePath;

    @BeforeEach
    void setUp() throws IOException {
        // 创建临时测试目录
        testWarehousePath = "./test-warehouse-manifest";
        pathFactory = new PathFactory(testWarehousePath);
        
        // 创建表目录
        pathFactory.createTableDirectories("test_db", "test_table");
    }

    @AfterEach
    void tearDown() throws IOException {
        // 清理测试目录
        deleteDirectory(Paths.get(testWarehousePath));
    }

    @Test
    void testManifestEntry() {
        ManifestEntry entry = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            "./data/data-0-001.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, 1}),
            new RowKey(new byte[]{0, 0, 0, (byte) 10}),
            100
        );
        
        assertEquals(ManifestEntry.FileKind.ADD, entry.getKind());
        assertEquals("./data/data-0-001.sst", entry.getFile());
        assertEquals(0, entry.getLevel());
        Assertions.assertEquals(new RowKey(new byte[]{0, 0, 0, 1}), entry.getMinKey());
        Assertions.assertEquals(new RowKey(new byte[]{0, 0, 0, (byte) 10}), entry.getMaxKey());
        assertEquals(100, entry.getRowCount());
    }

    @Test
    void testManifestFile() throws IOException {
        // 创建 Manifest 条目
        ManifestEntry entry1 = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            "./data/data-0-001.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, 1}),
            new RowKey(new byte[]{0, 0, 0, (byte) 10}),
            100
        );
        
        ManifestEntry entry2 = new ManifestEntry(
            ManifestEntry.FileKind.DELETE,
            "./data/data-0-002.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, (byte) 11}),
            new RowKey(new byte[]{0, 0, 0, (byte) 20}),
            50
        );
        
        // 创建 Manifest 文件
        ManifestFile manifestFile = new ManifestFile(Arrays.asList(entry1, entry2));
        
        assertEquals(2, manifestFile.size());
        assertFalse(manifestFile.isEmpty());
        assertEquals(2, manifestFile.getEntries().size());
        
        // 持久化
        manifestFile.persist(pathFactory, "test_db", "test_table", "abc123");
        
        // 验证文件已创建
        Path manifestPath = pathFactory.getManifestPath("test_db", "test_table", "abc123");
        assertTrue(Files.exists(manifestPath));
        
        // 重新加载
        ManifestFile loadedManifest = ManifestFile.load(pathFactory, "test_db", "test_table", "abc123");
        
        assertNotNull(loadedManifest);
        assertEquals(manifestFile, loadedManifest);
        assertEquals(2, loadedManifest.size());
    }

    @Test
    void testManifestList() throws IOException {
        // 创建 Manifest List
        ManifestList manifestList = new ManifestList();
        manifestList.addManifestFile("manifest-abc123");
        manifestList.addManifestFile("manifest-def456");
        
        assertEquals(2, manifestList.size());
        assertFalse(manifestList.isEmpty());
        assertEquals(2, manifestList.getManifestFiles().size());
        
        // 持久化
        manifestList.persist(pathFactory, "test_db", "test_table", 1);
        
        // 验证文件已创建
        Path manifestListPath = pathFactory.getManifestListPath("test_db", "test_table", 1);
        assertTrue(Files.exists(manifestListPath));
        
        // 重新加载
        ManifestList loadedManifestList = ManifestList.load(pathFactory, "test_db", "test_table", 1);
        
        assertNotNull(loadedManifestList);
        assertEquals(manifestList, loadedManifestList);
        assertEquals(2, loadedManifestList.size());
    }

    @Test
    void testManifestFileExists() {
        assertFalse(ManifestFile.exists(pathFactory, "test_db", "test_table", "nonexistent"));
        
        // 创建一个 Manifest 文件来测试
        ManifestEntry entry = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            "./data/data-0-001.sst",
            0,
            new RowKey(new byte[]{0, 0, 0, 1}),
            new RowKey(new byte[]{0, 0, 0, (byte) 10}),
            100
        );
        
        ManifestFile manifestFile = new ManifestFile(Collections.singletonList(entry));
        assertFalse(ManifestFile.exists(pathFactory, "test_db", "test_table", "test"));
    }

    @Test
    void testManifestListExists() {
        assertFalse(ManifestList.exists(pathFactory, "test_db", "test_table", 999));
        
        // 创建一个 Manifest List 来测试
        ManifestList manifestList = new ManifestList();
        assertFalse(ManifestList.exists(pathFactory, "test_db", "test_table", 999));
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