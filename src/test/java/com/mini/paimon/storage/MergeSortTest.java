package com.mini.paimon.storage;

import com.mini.paimon.metadata.*;
import com.mini.paimon.utils.PathFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 归并排序测试
 * 验证：
 * 1. 多路归并排序的正确性
 * 2. 重复 key 的去重逻辑
 * 3. minKey/maxKey 的准确性
 */
public class MergeSortTest {
    
    @TempDir
    Path tempDir;
    
    private PathFactory pathFactory;
    private Schema schema;
    private SSTableWriter writer;
    private SSTableReader reader;
    
    @BeforeEach
    void setUp() {
        pathFactory = new PathFactory(tempDir.toString());
        
        // 创建测试 Schema（有主键）
        List<Field> fields = Arrays.asList(
            new Field("id", DataType.INT, false),
            new Field("name", DataType.STRING, false),
            new Field("age", DataType.INT, false)
        );
        schema = new Schema(1, fields, Arrays.asList("id"), Collections.emptyList());
        
        writer = new SSTableWriter();
        reader = new SSTableReader();
    }
    
    @Test
    void testMultiWayMergeSort() throws IOException {
        // 创建 3 个有序的 SSTable 文件
        // File 1: keys [1, 3, 5, 7, 9]
        // File 2: keys [2, 4, 6, 8, 10]
        // File 3: keys [1, 2, 3, 11, 12]  (包含重复 key)
        
        String file1 = createSSTableWithKeys(Arrays.asList(1, 3, 5, 7, 9), "file1.sst");
        String file2 = createSSTableWithKeys(Arrays.asList(2, 4, 6, 8, 10), "file2.sst");
        String file3 = createSSTableWithKeys(Arrays.asList(1, 2, 3, 11, 12), "file3.sst");
        
        // 执行多路归并
        List<String> files = Arrays.asList(file1, file2, file3);
        List<Row> merged = MergeSortedReader.readAll(schema, files);
        
        // 验证：
        // 1. 数据有序
        // 2. 重复的 key (1, 2, 3) 只保留 file3 的数据（最新的）
        // 3. 最终应该有 12 条不重复的记录
        
        assertEquals(12, merged.size(), "Should have 12 unique records");
        
        // 验证顺序
        for (int i = 0; i < merged.size(); i++) {
            Row row = merged.get(i);
            int expectedId = i + 1;
            assertEquals(expectedId, row.getValue(0), "Row " + i + " should have id=" + expectedId);
        }
        
        // 验证重复 key 使用了最新数据（file3）
        Row row1 = merged.get(0);  // id=1
        Row row2 = merged.get(1);  // id=2
        Row row3 = merged.get(2);  // id=3
        
        assertEquals("name-1-file3", row1.getValue(1), "Duplicate key 1 should use data from file3");
        assertEquals("name-2-file3", row2.getValue(1), "Duplicate key 2 should use data from file3");
        assertEquals("name-3-file3", row3.getValue(1), "Duplicate key 3 should use data from file3");
    }
    
    @Test
    void testMinMaxKeyCorrectness() throws IOException {
        // 创建一个 MemTable 并刷写
        MemTable memTable = new MemTable(schema, 1);
        
        // 插入乱序数据
        memTable.put(createRow(5, "Alice", 25));
        memTable.put(createRow(2, "Bob", 30));
        memTable.put(createRow(8, "Charlie", 35));
        memTable.put(createRow(1, "David", 40));
        memTable.put(createRow(10, "Eve", 45));
        
        // 刷写到 SSTable
        String filePath = tempDir.resolve("test-minmax.sst").toString();
        com.mini.paimon.manifest.DataFileMeta fileMeta = writer.flush(memTable, filePath, 1, 0);
        
        // 验证 minKey 和 maxKey
        // MemTable 是基于 TreeMap 的，会自动排序
        // minKey 应该是 1，maxKey 应该是 10
        
        RowKey expectedMinKey = RowKey.fromRow(createRow(1, "David", 40), schema);
        RowKey expectedMaxKey = RowKey.fromRow(createRow(10, "Eve", 45), schema);
        
        assertEquals(expectedMinKey, fileMeta.getMinKey(), "MinKey should be 1");
        assertEquals(expectedMaxKey, fileMeta.getMaxKey(), "MaxKey should be 10");
        
        // 验证从文件读取的数据也是有序的
        List<Row> rows = reader.scan(filePath);
        assertEquals(5, rows.size(), "Should have 5 rows");
        
        // 验证顺序：1, 2, 5, 8, 10
        assertEquals(1, rows.get(0).getValue(0), "First row should have id=1");
        assertEquals(2, rows.get(1).getValue(0), "Second row should have id=2");
        assertEquals(5, rows.get(2).getValue(0), "Third row should have id=5");
        assertEquals(8, rows.get(3).getValue(0), "Fourth row should have id=8");
        assertEquals(10, rows.get(4).getValue(0), "Fifth row should have id=10");
    }
    
    @Test
    void testCompactionWithMergeSort() throws IOException {
        // 创建多个 SSTable，模拟 Level 0 的文件
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(createSSTableWithKeys(Arrays.asList(1, 5, 9, 13, 17), "input1.sst"));
        inputFiles.add(createSSTableWithKeys(Arrays.asList(2, 6, 10, 14, 18), "input2.sst"));
        inputFiles.add(createSSTableWithKeys(Arrays.asList(3, 7, 11, 15, 19), "input3.sst"));
        inputFiles.add(createSSTableWithKeys(Arrays.asList(1, 2, 3, 20, 21), "input4.sst"));  // 重复 key
        
        // 创建 Compactor 并执行合并
        Compactor compactor = new Compactor(schema, pathFactory, "test_db", "test_table", new java.util.concurrent.atomic.AtomicLong(1000));
        
        List<Compactor.LeveledSSTable> leveledSSTables = new ArrayList<>();
        for (String file : inputFiles) {
            // 读取文件获取 minKey/maxKey
            List<Row> rows = reader.scan(file);
            RowKey minKey = RowKey.fromRow(rows.get(0), schema);
            RowKey maxKey = RowKey.fromRow(rows.get(rows.size() - 1), schema);
            
            leveledSSTables.add(new Compactor.LeveledSSTable(
                file, 0, minKey, maxKey, 
                java.nio.file.Files.size(java.nio.file.Paths.get(file)), 
                rows.size()
            ));
        }
        
        // 执行 Compaction
        Compactor.CompactionResult result = compactor.compact(leveledSSTables);
        
        // 验证结果
        assertFalse(result.isEmpty(), "Compaction should produce results");
        
        // 读取合并后的所有数据
        List<Row> allMergedRows = new ArrayList<>();
        for (Compactor.LeveledSSTable output : result.getOutputFiles()) {
            allMergedRows.addAll(reader.scan(output.getPath()));
        }
        
        // 验证：
        // 1. 数据有序
        // 2. 去重正确（key 1, 2, 3 只保留一次）
        // 3. 总数正确（17 条唯一记录）
        // 计算：
        // input1: [1, 5, 9, 13, 17]
        // input2: [2, 6, 10, 14, 18]
        // input3: [3, 7, 11, 15, 19]
        // input4: [1, 2, 3, 20, 21] (重复1,2,3)
        // 合并去重后: [1,2,3,5,6,7,9,10,11,13,14,15,17,18,19,20,21] = 17条
        
        assertEquals(17, allMergedRows.size(), "Should have 17 unique records after compaction");
        
        // 验证顺序（注意：缺少4,8,12,16）
        int[] expectedIds = {1,2,3,5,6,7,9,10,11,13,14,15,17,18,19,20,21};
        for (int i = 0; i < allMergedRows.size(); i++) {
            Row row = allMergedRows.get(i);
            int expectedId = expectedIds[i];
            assertEquals(expectedId, row.getValue(0), "Row " + i + " should have id=" + expectedId);
        }
    }
    
    // === 辅助方法 ===
    
    private String createSSTableWithKeys(List<Integer> keys, String fileName) throws IOException {
        MemTable memTable = new MemTable(schema, System.currentTimeMillis());
        
        // 提取文件名作为后缀（移除.sst后缀）
        String suffix = fileName.replace(".sst", "");
        
        for (int key : keys) {
            memTable.put(createRow(key, "name-" + key + "-" + suffix, 20 + key));
        }
        
        String filePath = tempDir.resolve(fileName).toString();
        writer.flush(memTable, filePath, 1, 0);
        
        return filePath;
    }
    
    private Row createRow(int id, String name, int age) {
        return new Row(new Object[]{id, name, age});
    }
}
