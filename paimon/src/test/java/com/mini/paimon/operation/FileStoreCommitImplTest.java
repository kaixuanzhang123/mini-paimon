package com.mini.paimon.operation;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.RowKey;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.PathFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * FileStoreCommitImpl 单元测试
 * 测试两阶段提交、MVCC并发控制、冲突检测等核心功能
 */
public class FileStoreCommitImplTest {
    
    private Path testWarehouse;
    private PathFactory pathFactory;
    private SnapshotManager snapshotManager;
    private Schema schema;
    private FileStoreCommitImpl commitImpl;
    
    private static final String DATABASE = "test_db";
    private static final String TABLE = "test_table";
    private static final String BRANCH = "main";
    
    @Before
    public void setUp() throws IOException {
        // 创建临时测试目录
        testWarehouse = Paths.get("./test-warehouse-" + System.currentTimeMillis());
        Files.createDirectories(testWarehouse);
        
        pathFactory = new PathFactory(testWarehouse.toString());
        snapshotManager = new SnapshotManager(pathFactory, DATABASE, TABLE);
        
        // 创建测试schema（主键表）
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("id", DataType.INT(), false));
        fields.add(new Field("name", DataType.STRING(), true));
        schema = new Schema(1, fields, Arrays.asList("id"), new ArrayList<>());
        
        // 创建必要的目录
        Files.createDirectories(pathFactory.getSnapshotDir(DATABASE, TABLE));
        Files.createDirectories(pathFactory.getManifestDir(DATABASE, TABLE));
        
        commitImpl = new FileStoreCommitImpl(
            pathFactory, snapshotManager, schema, DATABASE, TABLE, BRANCH);
    }
    
    @After
    public void tearDown() throws IOException {
        // 清理测试目录
        if (Files.exists(testWarehouse)) {
            Files.walk(testWarehouse)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
        }
    }
    
    /**
     * 测试基本的提交流程
     */
    @Test
    public void testBasicCommit() throws IOException {
        // 1. 创建第一次提交
        List<ManifestEntry> entries1 = createTestEntries(1, 2);
        ManifestCommittable committable1 = ManifestCommittable.builder()
            .commitIdentifier(1001L)
            .commitUser("test-user")
            .appendTableFiles(entries1)
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        
        int snapshots = commitImpl.commit(committable1);
        
        // 验证
        assertEquals("Should generate 1 snapshot", 1, snapshots);
        
        Snapshot snapshot1 = snapshotManager.latestSnapshot();
        assertNotNull("Snapshot should exist", snapshot1);
        assertEquals("Snapshot ID should be 1", 1L, snapshot1.getId());
        assertEquals("Commit kind should be APPEND", Snapshot.CommitKind.APPEND, snapshot1.getCommitKind());
        assertEquals("Commit user should match", "test-user", snapshot1.getCommitUser());
        assertEquals("Commit identifier should match", 1001L, snapshot1.getCommitIdentifier());
    }
    
    /**
     * 测试增量提交
     */
    @Test
    public void testIncrementalCommit() throws IOException {
        // 1. 第一次提交
        List<ManifestEntry> entries1 = createTestEntries(1, 2);
        ManifestCommittable committable1 = ManifestCommittable.builder()
            .commitIdentifier(1001L)
            .commitUser("test-user")
            .appendTableFiles(entries1)
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        commitImpl.commit(committable1);
        
        // 2. 第二次提交
        List<ManifestEntry> entries2 = createTestEntries(3, 4);
        ManifestCommittable committable2 = ManifestCommittable.builder()
            .commitIdentifier(1002L)
            .commitUser("test-user")
            .appendTableFiles(entries2)
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        commitImpl.commit(committable2);
        
        // 验证
        Snapshot snapshot2 = snapshotManager.latestSnapshot();
        assertNotNull("Snapshot should exist", snapshot2);
        assertEquals("Snapshot ID should be 2", 2L, snapshot2.getId());
        assertEquals("Commit identifier should match", 1002L, snapshot2.getCommitIdentifier());
        
        // 验证可以读取所有snapshots
        Snapshot snapshot1 = snapshotManager.snapshot(1L);
        assertNotNull("First snapshot should exist", snapshot1);
        assertEquals("First snapshot ID should be 1", 1L, snapshot1.getId());
    }
    
    /**
     * 测试去重机制
     */
    @Test
    public void testDuplicateDetection() throws IOException {
        // 1. 第一次提交
        List<ManifestEntry> entries = createTestEntries(1, 2);
        ManifestCommittable committable = ManifestCommittable.builder()
            .commitIdentifier(1001L)
            .commitUser("test-user")
            .appendTableFiles(entries)
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        
        int snapshots1 = commitImpl.commit(committable);
        assertEquals("Should generate 1 snapshot", 1, snapshots1);
        
        // 2. 重复提交（相同的commitUser, commitIdentifier, commitKind）
        int snapshots2 = commitImpl.commit(committable);
        assertEquals("Should not generate new snapshot (duplicate)", 1, snapshots2);
        
        // 验证：只有一个snapshot
        Snapshot latest = snapshotManager.latestSnapshot();
        assertNotNull("Snapshot should exist", latest);
        assertEquals("Should still be snapshot 1", 1L, latest.getId());
    }
    
    /**
     * 测试Base Manifest合并
     */
    @Test
    public void testManifestCompaction() throws IOException {
        String commitUser = "test-user";
        
        // 提交31次，触发manifest合并（默认阈值30）
        for (int i = 1; i <= 31; i++) {
            List<ManifestEntry> entries = createTestEntries(i * 10, i * 10 + 1);
            ManifestCommittable committable = ManifestCommittable.builder()
                .commitIdentifier(1000L + i)
                .commitUser(commitUser)
                .appendTableFiles(entries)
                .commitKind(Snapshot.CommitKind.APPEND)
                .build();
            
            commitImpl.commit(committable);
        }
        
        // 验证最新snapshot
        Snapshot latest = snapshotManager.latestSnapshot();
        assertNotNull("Latest snapshot should exist", latest);
        assertEquals("Should have 31 snapshots", 31L, latest.getId());
        
        // 验证第31个snapshot应该有base manifest（因为触发了合并）
        String baseManifestList = latest.getBaseManifestList();
        assertNotNull("Base manifest list should exist", baseManifestList);
        assertTrue("Base manifest should be new", 
                  baseManifestList.contains("manifest-list-base-31"));
    }
    
    /**
     * 测试Compaction提交
     */
    @Test
    public void testCompactCommit() throws IOException {
        // 1. 先提交一些普通数据
        List<ManifestEntry> entries1 = createTestEntries(1, 5);
        ManifestCommittable committable1 = ManifestCommittable.builder()
            .commitIdentifier(1001L)
            .commitUser("test-user")
            .appendTableFiles(entries1)
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        commitImpl.commit(committable1);
        
        // 2. 提交compaction结果（包含append和compact）
        List<ManifestEntry> appendEntries = createTestEntries(6, 7);
        List<ManifestEntry> compactEntries = createTestEntries(100, 101);
        
        ManifestCommittable committable2 = ManifestCommittable.builder()
            .commitIdentifier(1002L)
            .commitUser("test-user")
            .appendTableFiles(appendEntries)
            .compactTableFiles(compactEntries)
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        
        int snapshots = commitImpl.commit(committable2);
        
        // 应该生成2个snapshot（一个append，一个compact）
        assertEquals("Should generate 2 snapshots", 2, snapshots);
        
        Snapshot latest = snapshotManager.latestSnapshot();
        assertNotNull("Latest snapshot should exist", latest);
        assertEquals("Latest snapshot ID should be 3", 3L, latest.getId());
        assertEquals("Latest commit kind should be COMPACT", 
                    Snapshot.CommitKind.COMPACT, latest.getCommitKind());
    }
    
    /**
     * 测试空提交
     */
    @Test
    public void testEmptyCommit() throws IOException {
        // 创建空的committable
        ManifestCommittable committable = ManifestCommittable.builder()
            .commitIdentifier(1001L)
            .commitUser("test-user")
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        
        int snapshots = commitImpl.commit(committable);
        
        // 空提交不应该生成snapshot
        assertEquals("Should not generate snapshot for empty commit", 0, snapshots);
        
        Snapshot latest = snapshotManager.latestSnapshot();
        assertNull("No snapshot should exist", latest);
    }
    
    /**
     * 测试OVERWRITE提交
     */
    @Test
    public void testOverwriteCommit() throws IOException {
        // 1. 先提交一些数据
        List<ManifestEntry> entries1 = createTestEntries(1, 3);
        ManifestCommittable committable1 = ManifestCommittable.builder()
            .commitIdentifier(1001L)
            .commitUser("test-user")
            .appendTableFiles(entries1)
            .commitKind(Snapshot.CommitKind.APPEND)
            .build();
        commitImpl.commit(committable1);
        
        // 2. OVERWRITE提交
        List<ManifestEntry> entries2 = createTestEntries(10, 12);
        ManifestCommittable committable2 = ManifestCommittable.builder()
            .commitIdentifier(1002L)
            .commitUser("test-user")
            .appendTableFiles(entries2)
            .commitKind(Snapshot.CommitKind.OVERWRITE)
            .build();
        commitImpl.commit(committable2);
        
        // 验证
        Snapshot latest = snapshotManager.latestSnapshot();
        assertNotNull("Latest snapshot should exist", latest);
        assertEquals("Should have 2 snapshots", 2L, latest.getId());
        assertEquals("Commit kind should be OVERWRITE", 
                    Snapshot.CommitKind.OVERWRITE, latest.getCommitKind());
    }
    
    /**
     * 辅助方法：创建测试用的ManifestEntry列表
     */
    private List<ManifestEntry> createTestEntries(int startId, int endId) {
        List<ManifestEntry> entries = new ArrayList<>();
        
        for (int i = startId; i <= endId; i++) {
            RowKey minKey = RowKey.of(i);
            RowKey maxKey = RowKey.of(i);
            
            DataFileMeta fileMeta = new DataFileMeta(
                "data-" + i + ".parquet",
                1024L,  // fileSize
                100L,   // rowCount
                minKey,
                maxKey,
                schema.getSchemaId(),
                0,      // level
                System.currentTimeMillis()
            );
            
            entries.add(new ManifestEntry(ManifestEntry.FileKind.ADD, 0, fileMeta));
        }
        
        return entries;
    }
    
    /**
     * 辅助方法：创建DELETE类型的ManifestEntry
     */
    private ManifestEntry createDeleteEntry(int id) {
        RowKey minKey = RowKey.of(id);
        RowKey maxKey = RowKey.of(id);
        
        DataFileMeta fileMeta = new DataFileMeta(
            "data-" + id + ".parquet",
            1024L,
            100L,
            minKey,
            maxKey,
            schema.getSchemaId(),
            0,
            System.currentTimeMillis()
        );
        
        return new ManifestEntry(ManifestEntry.FileKind.DELETE, 0, fileMeta);
    }
}

