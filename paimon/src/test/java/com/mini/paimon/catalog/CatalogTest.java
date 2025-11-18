package com.mini.paimon.catalog;

import com.mini.paimon.exception.CatalogException;
import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.schema.TableMetadata;
import com.mini.paimon.snapshot.Snapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Catalog 测试类
 */
public class CatalogTest {
    private static final String TEST_WAREHOUSE = "./test-warehouse-catalog";
    private Catalog catalog;
    
    @BeforeEach
    public void setUp() throws Exception {
        // 清理测试环境
        cleanupWarehouse();
        
        // 创建 Catalog
        CatalogContext context = CatalogContext.builder()
            .warehouse(TEST_WAREHOUSE)
            .build();
        
        catalog = new FileSystemCatalog("test_catalog", "default", context);
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
        cleanupWarehouse();
    }
    
    @Test
    public void testCreateAndDropDatabase() throws Exception {
        // 创建数据库
        catalog.createDatabase("test_db", false);
        assertTrue(catalog.databaseExists("test_db"));
        
        // 重复创建（ignoreIfExists=true）
        catalog.createDatabase("test_db", true);
        
        // 重复创建（ignoreIfExists=false）应抛异常
        try {
            catalog.createDatabase("test_db", false);
            fail("Should throw DatabaseAlreadyExistException");
        } catch (CatalogException.DatabaseAlreadyExistException e) {
            // Expected
        }
        
        // 删除数据库
        catalog.dropDatabase("test_db", false, false);
        assertFalse(catalog.databaseExists("test_db"));
        
        // 删除不存在的数据库（ignoreIfNotExists=true）
        catalog.dropDatabase("test_db", true, false);
        
        // 删除不存在的数据库（ignoreIfNotExists=false）应抛异常
        try {
            catalog.dropDatabase("test_db", false, false);
            fail("Should throw DatabaseNotExistException");
        } catch (CatalogException.DatabaseNotExistException e) {
            // Expected
        }
    }
    
    @Test
    public void testListDatabases() throws Exception {
        catalog.createDatabase("db1", false);
        catalog.createDatabase("db2", false);
        catalog.createDatabase("db3", false);
        
        List<String> databases = catalog.listDatabases();
        assertEquals(3, databases.size());
        assertTrue(databases.contains("db1"));
        assertTrue(databases.contains("db2"));
        assertTrue(databases.contains("db3"));
    }
    
    @Test
    public void testCreateAndDropTable() throws Exception {
        catalog.createDatabase("test_db", false);
        
        Identifier tableId = new Identifier("test_db", "test_table");
        Schema schema = createTestSchema();
        
        // 创建表
        catalog.createTable(tableId, schema, false);
        assertTrue(catalog.tableExists(tableId));
        
        // 重复创建（ignoreIfExists=true）
        catalog.createTable(tableId, schema, true);
        
        // 重复创建（ignoreIfExists=false）应抛异常
        try {
            catalog.createTable(tableId, schema, false);
            fail("Should throw TableAlreadyExistException");
        } catch (CatalogException.TableAlreadyExistException e) {
            // Expected
        }
        
        // 删除表
        catalog.dropTable(tableId, false);
        assertFalse(catalog.tableExists(tableId));
        
        // 删除不存在的表（ignoreIfNotExists=true）
        catalog.dropTable(tableId, true);
        
        // 删除不存在的表（ignoreIfNotExists=false）应抛异常
        try {
            catalog.dropTable(tableId, false);
            fail("Should throw TableNotExistException");
        } catch (CatalogException.TableNotExistException e) {
            // Expected
        }
    }
    
    @Test
    public void testListTables() throws Exception {
        catalog.createDatabase("test_db", false);
        
        Schema schema = createTestSchema();
        catalog.createTable(new Identifier("test_db", "table1"), schema, false);
        catalog.createTable(new Identifier("test_db", "table2"), schema, false);
        catalog.createTable(new Identifier("test_db", "table3"), schema, false);
        
        List<String> tables = catalog.listTables("test_db");
        assertEquals(3, tables.size());
        assertTrue(tables.contains("table1"));
        assertTrue(tables.contains("table2"));
        assertTrue(tables.contains("table3"));
    }
    
    @Test
    public void testRenameTable() throws Exception {
        catalog.createDatabase("test_db", false);
        
        Identifier oldTable = new Identifier("test_db", "old_table");
        Identifier newTable = new Identifier("test_db", "new_table");
        
        Schema schema = createTestSchema();
        catalog.createTable(oldTable, schema, false);
        
        // 重命名表
        catalog.renameTable(oldTable, newTable);
        assertFalse(catalog.tableExists(oldTable));
        assertTrue(catalog.tableExists(newTable));
        
        // 重命名不存在的表应抛异常
        try {
            catalog.renameTable(oldTable, new Identifier("test_db", "another_table"));
            fail("Should throw TableNotExistException");
        } catch (CatalogException.TableNotExistException e) {
            // Expected
        }
    }
    
    @Test
    public void testAlterTable() throws Exception {
        catalog.createDatabase("test_db", false);
        
        Identifier tableId = new Identifier("test_db", "test_table");
        Schema schema = createTestSchema();
        catalog.createTable(tableId, schema, false);
        
        // 获取当前 Schema
        Schema currentSchema = catalog.getTableSchema(tableId);
        int oldSchemaId = currentSchema.getSchemaId();
        assertEquals(2, currentSchema.getFields().size());
        
        // 添加新字段
        List<Field> newFields = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("name", DataType.STRING(), false),
            new Field("age", DataType.INT(), true)  // 新增字段
        );
        
        Schema newSchema = catalog.alterTable(
            tableId, 
            newFields,
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        // 验证新 Schema
        assertTrue(newSchema.getSchemaId() > oldSchemaId);
        assertEquals(3, newSchema.getFields().size());
        
        // 验证可以获取旧版本 Schema
        Schema oldVersionSchema = catalog.getTableSchema(tableId, oldSchemaId);
        assertEquals(2, oldVersionSchema.getFields().size());
    }
    
    @Test
    public void testGetTableMetadata() throws Exception {
        catalog.createDatabase("test_db", false);
        
        Identifier tableId = new Identifier("test_db", "test_table");
        Schema schema = createTestSchema();
        catalog.createTable(tableId, schema, false);
        
        TableMetadata metadata = catalog.getTableMetadata(tableId);
        assertNotNull(metadata);
        assertEquals("test_table", metadata.getTableName());
        assertEquals("test_db", metadata.getDatabaseName());
    }
    
    @Test
    public void testSnapshotOperations() throws Exception {
        catalog.createDatabase("test_db", false);
        
        Identifier tableId = new Identifier("test_db", "test_table");
        Schema schema = createTestSchema();
        catalog.createTable(tableId, schema, false);
        
        // 新表没有快照
        Snapshot latestSnapshot = catalog.getLatestSnapshot(tableId);
        assertNull(latestSnapshot);
        
        List<Snapshot> snapshots = catalog.listSnapshots(tableId);
        assertEquals(0, snapshots.size());
    }
    
    @Test
    public void testDropDatabaseCascade() throws Exception {
        catalog.createDatabase("test_db", false);
        
        // 创建多个表
        Schema schema = createTestSchema();
        catalog.createTable(new Identifier("test_db", "table1"), schema, false);
        catalog.createTable(new Identifier("test_db", "table2"), schema, false);
        
        // 非级联删除应失败
        try {
            catalog.dropDatabase("test_db", false, false);
            fail("Should throw DatabaseNotEmptyException");
        } catch (CatalogException.DatabaseNotEmptyException e) {
            // Expected
        }
        
        // 级联删除应成功
        catalog.dropDatabase("test_db", false, true);
        assertFalse(catalog.databaseExists("test_db"));
    }
    
    @Test
    public void testIdentifier() {
        Identifier id1 = new Identifier("db", "table");
        assertEquals("db", id1.getDatabase());
        assertEquals("table", id1.getTable());
        assertEquals("db.table", id1.getFullName());
        
        Identifier id2 = Identifier.fromString("db.table");
        assertEquals(id1, id2);
        
        // 测试无效标识符
        try {
            Identifier.fromString("invalid");
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        
        try {
            new Identifier("db-test", "table");
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected: 包含非法字符
        }
    }
    
    @Test
    public void testCatalogClose() throws Exception {
        catalog.createDatabase("test_db", false);
        
        catalog.close();
        
        // 关闭后的操作应抛异常
        try {
            catalog.createDatabase("another_db", false);
            fail("Should throw CatalogException");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("closed"));
        }
    }
    
    // ==================== 辅助方法 ====================
    
    private Schema createTestSchema() {
        List<Field> fields = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("name", DataType.STRING(), false)
        );
        
        return new Schema(
            0,
            fields,
            Collections.singletonList("id"),
            Collections.emptyList()
        );
    }
    
    private void cleanupWarehouse() throws IOException {
        Path warehousePath = Paths.get(TEST_WAREHOUSE);
        if (Files.exists(warehousePath)) {
            Files.walk(warehousePath)
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
}
