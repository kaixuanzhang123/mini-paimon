package com.mini.paimon.storage;

import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.utils.PathFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LSMTree 集成测试类
 */
class LSMTreeTest {
    private Schema schema;
    private PathFactory pathFactory;
    private LSMTree lsmTree;
    private String testWarehousePath;

    @BeforeEach
    void setUp() throws IOException {
        // 创建测试用的 Schema
        Field idField = new Field("id", DataType.INT(), false);
        Field nameField = new Field("name", DataType.STRING(), true);
        schema = new Schema(0, Arrays.asList(idField, nameField), Collections.singletonList("id"));
        
        // 创建临时测试目录
        testWarehousePath = "./test-warehouse";
        pathFactory = new PathFactory(testWarehousePath);
        
        // 创建表目录
        pathFactory.createTableDirectories("test_db", "test_table");
        
        // 创建 LSMTree
        lsmTree = new LSMTree(schema, pathFactory, "test_db", "test_table");
    }

    @AfterEach
    void tearDown() throws IOException {
        // 关闭 LSMTree
        if (lsmTree != null) {
            lsmTree.close();
        }
        
        // 清理测试目录
        deleteDirectory(Paths.get(testWarehousePath));
    }

    @Test
    void testPutAndGet() throws IOException {
        // 创建测试数据
        Row row1 = new Row(new Object[]{1, "Alice"});
        Row row2 = new Row(new Object[]{2, "Bob"});
        
        // 插入数据
        lsmTree.put(row1);
        lsmTree.put(row2);
        
        // 获取数据
        RowKey key1 = RowKey.fromRow(row1, schema);
        RowKey key2 = RowKey.fromRow(row2, schema);
        
        Row retrievedRow1 = lsmTree.get(key1);
        Row retrievedRow2 = lsmTree.get(key2);
        
        assertNotNull(retrievedRow1);
        assertNotNull(retrievedRow2);
        assertEquals(row1, retrievedRow1);
        assertEquals(row2, retrievedRow2);
    }

    @Test
    void testGetNonExistentKey() throws IOException {
        // 创建测试数据
        Row row = new Row(new Object[]{1, "Alice"});
        lsmTree.put(row);
        
        // 查询不存在的键
        RowKey nonExistentKey = RowKey.fromRow(new Row(new Object[]{999, "NonExistent"}), schema);
        Row result = lsmTree.get(nonExistentKey);
        
        assertNull(result);
    }

    @Test
    void testStatus() {
        String status = lsmTree.getStatus();
        assertNotNull(status);
        assertFalse(status.isEmpty());
        assertTrue(status.contains("LSMTree"));
    }

    @Test
    void testSmallMemTableFlush() throws IOException {
        // 创建一个小容量的 LSMTree 来测试刷写
        LSMTree smallTree = new LSMTree(schema, pathFactory, "test_db", "small_table");
        
        // 插入少量数据
        Row row1 = new Row(new Object[]{1, "Alice"});
        Row row2 = new Row(new Object[]{2, "Bob"});
        
        smallTree.put(row1);
        smallTree.put(row2);
        
        // 验证数据可以获取
        RowKey key1 = RowKey.fromRow(row1, schema);
        Row retrievedRow1 = smallTree.get(key1);
        assertEquals(row1, retrievedRow1);
        
        smallTree.close();
    }

    /**
     * 递归删除目录
     */
    private void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted(java.util.Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }
}