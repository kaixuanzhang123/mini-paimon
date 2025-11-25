package com.mini.paimon.io;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.schema.RowKey;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * FileEntry 单元测试
 * 测试文件合并、冲突检测等功能
 */
public class FileEntryTest {
    
    /**
     * 测试Identifier的相等性
     */
    @Test
    public void testIdentifierEquals() {
        FileEntry.Identifier id1 = new FileEntry.Identifier("partition1", 0, 1, "file1.parquet");
        FileEntry.Identifier id2 = new FileEntry.Identifier("partition1", 0, 1, "file1.parquet");
        FileEntry.Identifier id3 = new FileEntry.Identifier("partition1", 0, 2, "file1.parquet");
        FileEntry.Identifier id4 = new FileEntry.Identifier("partition2", 0, 1, "file1.parquet");
        
        assertEquals("Same identifiers should be equal", id1, id2);
        assertEquals("Hash codes should be equal", id1.hashCode(), id2.hashCode());
        
        assertNotEquals("Different level should not be equal", id1, id3);
        assertNotEquals("Different partition should not be equal", id1, id4);
    }
    
    /**
     * 测试Identifier从ManifestEntry创建
     */
    @Test
    public void testIdentifierFromEntry() {
        ManifestEntry entry = createTestEntry("data-1.parquet", 1, 0);
        FileEntry.Identifier id = FileEntry.Identifier.fromEntry(entry);
        
        assertNotNull("Identifier should not be null", id);
        assertEquals("File name should match", "data-1.parquet", id.toString().contains("data-1.parquet"));
    }
    
    /**
     * 测试基本的ADD合并
     */
    @Test
    public void testMergeAddEntries() {
        List<FileEntry.SimpleFileEntry> entries = new ArrayList<>();
        
        // 添加两个ADD类型的文件
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        entries.add(createSimpleEntry("file2.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        
        Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> merged = FileEntry.mergeEntries(entries);
        
        assertEquals("Should have 2 files", 2, merged.size());
        
        // 验证都是ADD类型
        for (FileEntry.SimpleFileEntry entry : merged.values()) {
            assertEquals("All should be ADD", ManifestEntry.FileKind.ADD, entry.getKind());
        }
    }
    
    /**
     * 测试ADD和DELETE抵消
     */
    @Test
    public void testMergeAddAndDelete() {
        List<FileEntry.SimpleFileEntry> entries = new ArrayList<>();
        
        // 先ADD
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        // 再DELETE同一个文件
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.DELETE, 0, 0));
        
        Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> merged = FileEntry.mergeEntries(entries);
        
        // ADD和DELETE应该抵消
        assertEquals("ADD and DELETE should cancel out", 0, merged.size());
    }
    
    /**
     * 测试DELETE保留（文件在之前的manifest中）
     */
    @Test
    public void testMergeDeleteOnly() {
        List<FileEntry.SimpleFileEntry> entries = new ArrayList<>();
        
        // 只有DELETE，没有对应的ADD
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.DELETE, 0, 0));
        
        Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> merged = FileEntry.mergeEntries(entries);
        
        // DELETE标记应该被保留
        assertEquals("DELETE mark should be kept", 1, merged.size());
        
        FileEntry.SimpleFileEntry entry = merged.values().iterator().next();
        assertEquals("Should be DELETE", ManifestEntry.FileKind.DELETE, entry.getKind());
    }
    
    /**
     * 测试重复ADD冲突检测
     */
    @Test
    public void testMergeDuplicateAddConflict() {
        List<FileEntry.SimpleFileEntry> entries = new ArrayList<>();
        
        // 添加两个相同的ADD文件
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        
        try {
            FileEntry.mergeEntries(entries);
            fail("Should throw IllegalStateException for duplicate ADD");
        } catch (IllegalStateException e) {
            assertTrue("Error message should mention conflict", 
                      e.getMessage().contains("already added"));
        }
    }
    
    /**
     * 测试DELETE标记检查
     */
    @Test
    public void testCheckNoDeleteMarks() {
        List<FileEntry.SimpleFileEntry> entries = new ArrayList<>();
        
        // 只有DELETE，没有对应的ADD
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.DELETE, 0, 0));
        
        Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> merged = FileEntry.mergeEntries(entries);
        
        try {
            FileEntry.checkNoDeleteMarks(merged);
            fail("Should throw IllegalStateException for DELETE mark");
        } catch (IllegalStateException e) {
            assertTrue("Error message should mention deletion conflict", 
                      e.getMessage().contains("was not added"));
        }
    }
    
    /**
     * 测试正常情况下的DELETE标记检查（无冲突）
     */
    @Test
    public void testCheckNoDeleteMarksSuccess() {
        List<FileEntry.SimpleFileEntry> entries = new ArrayList<>();
        
        // ADD两个文件
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        entries.add(createSimpleEntry("file2.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        
        Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> merged = FileEntry.mergeEntries(entries);
        
        // 不应该抛出异常
        FileEntry.checkNoDeleteMarks(merged);
    }
    
    /**
     * 测试复杂的合并场景
     */
    @Test
    public void testComplexMergeScenario() {
        List<FileEntry.SimpleFileEntry> entries = new ArrayList<>();
        
        // 场景：
        // 1. ADD file1, file2, file3
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        entries.add(createSimpleEntry("file2.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        entries.add(createSimpleEntry("file3.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        
        // 2. DELETE file2（抵消）
        entries.add(createSimpleEntry("file2.parquet", ManifestEntry.FileKind.DELETE, 0, 0));
        
        // 3. ADD file4
        entries.add(createSimpleEntry("file4.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        
        Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> merged = FileEntry.mergeEntries(entries);
        
        // 结果应该有3个文件：file1, file3, file4
        assertEquals("Should have 3 files", 3, merged.size());
        
        // 验证都是ADD类型
        for (FileEntry.SimpleFileEntry entry : merged.values()) {
            assertEquals("All should be ADD", ManifestEntry.FileKind.ADD, entry.getKind());
        }
    }
    
    /**
     * 测试fromManifestEntries转换
     */
    @Test
    public void testFromManifestEntries() {
        List<ManifestEntry> manifestEntries = new ArrayList<>();
        manifestEntries.add(createTestEntry("file1.parquet", 1, 0));
        manifestEntries.add(createTestEntry("file2.parquet", 2, 0));
        manifestEntries.add(createTestEntry("file3.parquet", 3, 0));
        
        List<FileEntry.SimpleFileEntry> simpleEntries = 
            FileEntry.fromManifestEntries(manifestEntries);
        
        assertEquals("Should have 3 entries", 3, simpleEntries.size());
        
        // 验证转换正确
        for (int i = 0; i < manifestEntries.size(); i++) {
            assertEquals("Kind should match", 
                        manifestEntries.get(i).getKind(), 
                        simpleEntries.get(i).getKind());
            assertEquals("Level should match", 
                        manifestEntries.get(i).getLevel(), 
                        simpleEntries.get(i).getLevel());
        }
    }
    
    /**
     * 测试不同bucket的文件不冲突
     */
    @Test
    public void testDifferentBucketsNoConflict() {
        List<FileEntry.SimpleFileEntry> entries = new ArrayList<>();
        
        // 相同文件名，但不同bucket
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.ADD, 1, 0));
        
        Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> merged = FileEntry.mergeEntries(entries);
        
        // 不同bucket应该被视为不同文件
        assertEquals("Different buckets should be different files", 2, merged.size());
    }
    
    /**
     * 测试不同level的文件不冲突
     */
    @Test
    public void testDifferentLevelsNoConflict() {
        List<FileEntry.SimpleFileEntry> entries = new ArrayList<>();
        
        // 相同文件名，但不同level
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.ADD, 0, 0));
        entries.add(createSimpleEntry("file1.parquet", ManifestEntry.FileKind.ADD, 0, 1));
        
        Map<FileEntry.Identifier, FileEntry.SimpleFileEntry> merged = FileEntry.mergeEntries(entries);
        
        // 不同level应该被视为不同文件
        assertEquals("Different levels should be different files", 2, merged.size());
    }
    
    /**
     * 辅助方法：创建SimpleFileEntry
     */
    private FileEntry.SimpleFileEntry createSimpleEntry(
            String fileName, 
            ManifestEntry.FileKind kind, 
            int bucket,
            int level) {
        ManifestEntry entry = createTestEntry(fileName, bucket, level);
        if (kind == ManifestEntry.FileKind.DELETE) {
            entry = new ManifestEntry(ManifestEntry.FileKind.DELETE, bucket, entry.getFile());
        }
        return new FileEntry.SimpleFileEntry(entry);
    }
    
    /**
     * 辅助方法：创建ManifestEntry
     */
    private ManifestEntry createTestEntry(String fileName, int bucket, int level) {
        RowKey minKey = RowKey.of(1);
        RowKey maxKey = RowKey.of(100);
        
        DataFileMeta fileMeta = new DataFileMeta(
            fileName,
            1024L,
            100L,
            minKey,
            maxKey,
            1,
            level,
            System.currentTimeMillis()
        );
        
        return new ManifestEntry(ManifestEntry.FileKind.ADD, bucket, fileMeta);
    }
}

