package com.mini.paimon.index;

import com.mini.paimon.schema.*;
import com.mini.paimon.storage.MemTable;
import com.mini.paimon.storage.SSTableReader;
import com.mini.paimon.storage.SSTableWriter;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.utils.PathFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bitmap索引集成测试
 * 测试完整的写入->索引构建->读取过滤流程
 */
public class BitmapIndexIntegrationTest {
    
    private Path testDir;
    private PathFactory pathFactory;
    private Schema schema;
    
    @BeforeEach
    public void setup() throws IOException {
        testDir = Files.createTempDirectory("bitmap-index-test-");
        pathFactory = new PathFactory(testDir.toString());
        
        schema = new Schema(
            0,
            Arrays.asList(
                new Field("id", DataType.INT(), false),
                new Field("name", DataType.STRING(), false),
                new Field("status", DataType.STRING(), false),
                new Field("category", DataType.STRING(), false)
            ),
            Arrays.asList("id")
        );
    }
    
    @AfterEach
    public void cleanup() throws IOException {
        if (testDir != null && Files.exists(testDir)) {
            Files.walk(testDir)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
        }
    }
    
    @Test
    public void testWriteAndReadWithBitmapIndex() throws IOException {
        // 1. 准备数据
        MemTable memTable = new MemTable(schema, 0);
        memTable.put(new Row(new Object[]{1, "Alice", "ACTIVE", "A"}));
        memTable.put(new Row(new Object[]{2, "Bob", "PENDING", "B"}));
        memTable.put(new Row(new Object[]{3, "Charlie", "ACTIVE", "A"}));
        memTable.put(new Row(new Object[]{4, "David", "DELETED", "C"}));
        memTable.put(new Row(new Object[]{5, "Eve", "ACTIVE", "A"}));
        
        // 2. 写入SSTable并构建索引
        String filePath = testDir.resolve("test-data.sst").toString();
        SSTableWriter writer = new SSTableWriter(pathFactory, true);
        writer.flush(memTable, filePath, 0, 0, schema, "test_db", "test_table");
        
        // 3. 验证索引文件已创建
        Path indexDir = testDir.resolve("test_db/test_table/index");
        assertTrue(Files.exists(indexDir), "Index directory should exist");
        
        // 4. 加载索引
        IndexFileManager indexManager = new IndexFileManager(pathFactory);
        
        // 检查status字段的Bitmap索引
        String statusBitmapIndexFile = "test-data_status.bmi";
        Path statusIndexPath = indexDir.resolve(statusBitmapIndexFile);
        assertTrue(Files.exists(statusIndexPath), "Status bitmap index should exist");
        
        IndexMeta statusIndexMeta = new IndexMeta(
            IndexType.BITMAP,
            "status",
            statusBitmapIndexFile,
            Files.size(statusIndexPath),
            System.currentTimeMillis()
        );
        
        BitmapIndex statusIndex = (BitmapIndex) indexManager.loadIndex("test_db", "test_table", statusIndexMeta);
        assertNotNull(statusIndex);
        
        // 5. 测试索引过滤
        Predicate predicate = Predicate.equal("status", "ACTIVE");
        SimpleBitmap bitmap = statusIndex.filter(predicate);
        
        assertNotNull(bitmap);
        assertEquals(3, bitmap.getCardinality(), "Should have 3 ACTIVE rows");
        
        // 6. 使用Bitmap过滤读取数据
        SSTableReader reader = new SSTableReader();
        List<Row> filteredRows = reader.scan(filePath, bitmap);
        
        assertEquals(3, filteredRows.size(), "Should read 3 filtered rows");
        
        // 验证读取的数据
        for (Row row : filteredRows) {
            assertEquals("ACTIVE", row.getValues()[2], "All rows should have status=ACTIVE");
        }
    }
    
    @Test
    public void testBitmapIndexWithInQuery() throws IOException {
        // 1. 准备数据
        MemTable memTable = new MemTable(schema, 0);
        memTable.put(new Row(new Object[]{1, "Alice", "ACTIVE", "A"}));
        memTable.put(new Row(new Object[]{2, "Bob", "PENDING", "B"}));
        memTable.put(new Row(new Object[]{3, "Charlie", "DELETED", "C"}));
        memTable.put(new Row(new Object[]{4, "David", "ACTIVE", "A"}));
        memTable.put(new Row(new Object[]{5, "Eve", "PENDING", "B"}));
        
        // 2. 写入SSTable
        String filePath = testDir.resolve("test-data.sst").toString();
        SSTableWriter writer = new SSTableWriter(pathFactory, true);
        writer.flush(memTable, filePath, 0, 0, schema, "test_db", "test_table");
        
        // 3. 加载索引
        IndexFileManager indexManager = new IndexFileManager(pathFactory);
        Path indexDir = testDir.resolve("test_db/test_table/index");
        String statusBitmapIndexFile = "test-data_status.bmi";
        
        IndexMeta statusIndexMeta = new IndexMeta(
            IndexType.BITMAP,
            "status",
            statusBitmapIndexFile,
            Files.size(indexDir.resolve(statusBitmapIndexFile)),
            System.currentTimeMillis()
        );
        
        BitmapIndex statusIndex = (BitmapIndex) indexManager.loadIndex("test_db", "test_table", statusIndexMeta);
        
        // 4. 测试IN查询
        List<Object> values = Arrays.asList("ACTIVE", "PENDING");
        BitmapIndex.InPredicate predicate = new BitmapIndex.InPredicate("status", values);
        SimpleBitmap bitmap = statusIndex.filter(predicate);
        
        assertEquals(4, bitmap.getCardinality(), "Should match 4 rows (ACTIVE or PENDING)");
        
        // 5. 使用Bitmap读取
        SSTableReader reader = new SSTableReader();
        List<Row> filteredRows = reader.scan(filePath, bitmap);
        
        assertEquals(4, filteredRows.size());
        
        // 验证所有行的status都是ACTIVE或PENDING
        for (Row row : filteredRows) {
            String status = (String) row.getValues()[2];
            assertTrue(status.equals("ACTIVE") || status.equals("PENDING"));
        }
    }
    
    @Test
    public void testPerformanceComparison() throws IOException {
        // 创建较大的数据集
        MemTable memTable = new MemTable(schema, 0);
        for (int i = 1; i <= 1000; i++) {
            String status = (i % 10 == 0) ? "ACTIVE" : "PENDING";
            String category = "CAT" + (i % 5);
            memTable.put(new Row(new Object[]{i, "User" + i, status, category}));
        }
        
        String filePath = testDir.resolve("large-data.sst").toString();
        SSTableWriter writer = new SSTableWriter(pathFactory, true);
        writer.flush(memTable, filePath, 0, 0, schema, "test_db", "test_table");
        
        // 加载索引
        IndexFileManager indexManager = new IndexFileManager(pathFactory);
        Path indexDir = testDir.resolve("test_db/test_table/index");
        String statusBitmapIndexFile = "large-data_status.bmi";
        
        IndexMeta statusIndexMeta = new IndexMeta(
            IndexType.BITMAP,
            "status",
            statusBitmapIndexFile,
            Files.size(indexDir.resolve(statusBitmapIndexFile)),
            System.currentTimeMillis()
        );
        
        BitmapIndex statusIndex = (BitmapIndex) indexManager.loadIndex("test_db", "test_table", statusIndexMeta);
        
        // 准备过滤条件
        Predicate predicate = Predicate.equal("status", "ACTIVE");
        SimpleBitmap bitmap = statusIndex.filter(predicate);
        
        SSTableReader reader = new SSTableReader();
        
        // 测试1: 不使用Bitmap过滤
        long start1 = System.nanoTime();
        List<Row> allRows = reader.scan(filePath);
        int manualFilterCount = 0;
        for (Row row : allRows) {
            if ("ACTIVE".equals(row.getValues()[2])) {
                manualFilterCount++;
            }
        }
        long time1 = System.nanoTime() - start1;
        
        // 测试2: 使用Bitmap过滤
        long start2 = System.nanoTime();
        List<Row> filteredRows = reader.scan(filePath, bitmap);
        long time2 = System.nanoTime() - start2;
        
        // 验证结果一致
        assertEquals(manualFilterCount, filteredRows.size());
        assertEquals(100, filteredRows.size(), "Should have 100 ACTIVE rows (10% of 1000)");
        
        System.out.println("Performance comparison:");
        System.out.println("  Without Bitmap: " + time1 / 1_000_000 + " ms (read " + allRows.size() + " rows)");
        System.out.println("  With Bitmap:    " + time2 / 1_000_000 + " ms (filtered to " + filteredRows.size() + " rows)");
        System.out.println("  Bitmap cardinality: " + bitmap.getCardinality());
    }
}

