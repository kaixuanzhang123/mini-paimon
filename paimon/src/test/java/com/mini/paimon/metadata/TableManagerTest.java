package com.mini.paimon.metadata;

import com.mini.paimon.utils.PathFactory;
import org.junit.jupiter.api.AfterEach;
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
 * TableManager 测试类
 */
class TableManagerTest {
    private TableManager tableManager;
    private PathFactory pathFactory;
    private String testWarehousePath;

    @BeforeEach
    void setUp() throws IOException {
        // 创建临时测试目录
        testWarehousePath = "./test-warehouse-table";
        pathFactory = new PathFactory(testWarehousePath);
        
        // 创建 TableManager
        tableManager = new TableManager(pathFactory);
    }

    @AfterEach
    void tearDown() throws IOException {
        // 清理测试目录
        deleteDirectory(Paths.get(testWarehousePath));
    }

    @Test
    void testCreateTableAlreadyExists() throws IOException {
        // 创建字段
        Field idField = new Field("id", DataType.INT, false);
        
        // 先创建表
        tableManager.createTable(
            "test_db",
            "test_table",
            Collections.singletonList(idField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        // 再次创建相同表应该抛出异常
        assertThrows(IOException.class, () -> {
            tableManager.createTable(
                "test_db",
                "test_table",
                Collections.singletonList(idField),
                Collections.singletonList("id"),
                Collections.emptyList()
            );
        });
    }

    @Test
    void testGetTableMetadata() throws IOException {
        // 创建字段
        Field idField = new Field("id", DataType.INT, false);
        
        // 先创建表
        TableMetadata originalMetadata = tableManager.createTable(
            "test_db",
            "test_table",
            Collections.singletonList(idField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        // 获取表元数据
        TableMetadata loadedMetadata = tableManager.getTableMetadata("test_db", "test_table");
        
        assertNotNull(loadedMetadata);
        assertEquals(originalMetadata.getTableName(), loadedMetadata.getTableName());
        assertEquals("test_table", loadedMetadata.getTableName());
        assertEquals("test_db", loadedMetadata.getDatabaseName());
    }

    @Test
    void testTableExists() throws IOException {
        assertFalse(tableManager.tableExists("test_db", "test_table"));
        
        // 创建字段
        Field idField = new Field("id", DataType.INT, false);
        
        // 创建表
        tableManager.createTable(
            "test_db",
            "test_table",
            Collections.singletonList(idField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        assertTrue(tableManager.tableExists("test_db", "test_table"));
    }

    @Test
    void testDropTable() throws IOException {
        // 创建字段
        Field idField = new Field("id", DataType.INT, false);
        
        // 创建表
        tableManager.createTable(
            "test_db",
            "test_table",
            Collections.singletonList(idField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        // 验证表存在
        assertTrue(tableManager.tableExists("test_db", "test_table"));
        
        // 删除表
        tableManager.dropTable("test_db", "test_table");
        
        // 验证表已删除
        assertFalse(tableManager.tableExists("test_db", "test_table"));
        
        // 验证表目录已删除
        Path tablePath = pathFactory.getTablePath("test_db", "test_table");
        assertFalse(Files.exists(tablePath));
    }

    @Test
    void testListTables() throws IOException {
        // 创建字段
        Field idField = new Field("id", DataType.INT, false);
        
        // 创建多个表
        tableManager.createTable(
            "test_db",
            "table1",
            Collections.singletonList(idField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        tableManager.createTable(
            "test_db",
            "table2",
            Collections.singletonList(idField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        // 列出表
        java.util.List<String> tables = tableManager.listTables("test_db");
        
        assertNotNull(tables);
        assertEquals(2, tables.size());
        assertTrue(tables.contains("table1"));
        assertTrue(tables.contains("table2"));
    }

    @Test
    void testGetSchemaManager() {
        SchemaManager schemaManager = tableManager.getSchemaManager("test_db", "test_table");
        assertNotNull(schemaManager);
        assertEquals("test_db", schemaManager.getDatabase());
        assertEquals("test_table", schemaManager.getTable());
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
