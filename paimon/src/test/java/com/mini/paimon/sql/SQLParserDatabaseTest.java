package com.mini.paimon.sql;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.utils.PathFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 测试 SQLParser 对数据库名称的支持
 */
public class SQLParserDatabaseTest {
    private static final Logger logger = LoggerFactory.getLogger(SQLParserDatabaseTest.class);
    
    private static final String WAREHOUSE_PATH = "/tmp/mini-paimon-sql-db-test";
    private Catalog catalog;
    private PathFactory pathFactory;
    private SQLParser sqlParser;
    
    @BeforeEach
    public void setUp() throws IOException {
        // 清理旧的测试数据
        cleanupWarehouse();
        
        // 初始化 Catalog
        CatalogContext context = CatalogContext.builder()
            .warehouse(WAREHOUSE_PATH)
            .build();
        catalog = new FileSystemCatalog("test_catalog", context);
        pathFactory = new PathFactory(WAREHOUSE_PATH);
        sqlParser = new SQLParser(catalog, pathFactory);
        
        logger.info("测试环境初始化完成");
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
        // 清理测试数据
        cleanupWarehouse();
        logger.info("测试环境清理完成");
    }
    
    private void cleanupWarehouse() throws IOException {
        Path warehousePath = Paths.get(WAREHOUSE_PATH);
        if (Files.exists(warehousePath)) {
            Files.walk(warehousePath)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        logger.warn("Failed to delete: {}", path, e);
                    }
                });
        }
    }
    
    @Test
    public void testCreateTableWithDefaultDatabase() throws Exception {
        logger.info("=== 测试创建表（使用默认数据库）===");
        
        // 1. 创建默认数据库
        catalog.createDatabase("default", true);
        
        // 2. 创建表（不指定数据库名）
        String createSQL = "CREATE TABLE users (" +
                          "id INT NOT NULL, " +
                          "name VARCHAR(50), " +
                          "age INT)";
        
        sqlParser.executeSQL(createSQL);
        
        // 3. 验证表是否创建成功
        Identifier identifier = new Identifier("default", "users");
        assertTrue(catalog.tableExists(identifier), "表应该存在于default数据库中");
        
        // 4. 验证 Schema ID 是自动生成的（应该是0）
        Schema schema = catalog.getTableSchema(identifier);
        assertNotNull(schema, "Schema不应该为null");
        assertEquals(0, schema.getSchemaId(), "第一个Schema的ID应该是0");
        
        logger.info("Schema ID: {}", schema.getSchemaId());
        logger.info("测试通过：使用默认数据库创建表成功");
    }
    
    @Test
    public void testCreateTableWithSpecifiedDatabase() throws Exception {
        logger.info("=== 测试创建表（指定数据库名）===");
        
        // 1. 创建数据库
        catalog.createDatabase("test_db", false);
        
        // 2. 使用完整表名创建表（database.table格式）
        String createSQL = "CREATE TABLE test_db.users (" +
                          "id INT NOT NULL, " +
                          "name VARCHAR(50), " +
                          "age INT)";
        
        sqlParser.executeSQL(createSQL);
        
        // 3. 验证表是否创建在正确的数据库中
        Identifier identifier = new Identifier("test_db", "users");
        assertTrue(catalog.tableExists(identifier), "表应该存在于test_db数据库中");
        
        // 4. 验证表不存在于default数据库中
        Identifier defaultIdentifier = new Identifier("default", "users");
        assertFalse(catalog.tableExists(defaultIdentifier), "表不应该存在于default数据库中");
        
        logger.info("测试通过：指定数据库创建表成功");
    }
    
    @Test
    public void testAutoSchemaIdGeneration() throws Exception {
        logger.info("=== 测试 Schema ID 自动生成 ===");
        
        // 1. 创建数据库
        catalog.createDatabase("test_db", false);
        
        // 2. 创建第一个表
        String createSQL1 = "CREATE TABLE test_db.table1 (id INT NOT NULL)";
        sqlParser.executeSQL(createSQL1);
        
        Schema schema1 = catalog.getTableSchema(new Identifier("test_db", "table1"));
        assertEquals(0, schema1.getSchemaId(), "第一个Schema ID应该是0");
        
        // 3. 创建第二个表
        String createSQL2 = "CREATE TABLE test_db.table2 (id INT NOT NULL)";
        sqlParser.executeSQL(createSQL2);
        
        Schema schema2 = catalog.getTableSchema(new Identifier("test_db", "table2"));
        assertEquals(0, schema2.getSchemaId(), "第二个表的第一个Schema ID也应该是0（各表独立）");
        
        logger.info("Table1 Schema ID: {}", schema1.getSchemaId());
        logger.info("Table2 Schema ID: {}", schema2.getSchemaId());
        logger.info("测试通过：Schema ID自动生成正确");
    }
    
    @Test
    public void testInsertWithDatabaseName() throws Exception {
        logger.info("=== 测试 INSERT 语句（带数据库名）===");
        
        // 1. 创建数据库和表
        catalog.createDatabase("test_db", false);
        String createSQL = "CREATE TABLE test_db.users (" +
                          "id INT NOT NULL, " +
                          "name VARCHAR(50), " +
                          "age INT)";
        sqlParser.executeSQL(createSQL);
        
        // 2. 插入数据（使用完整表名）
        String insertSQL = "INSERT INTO test_db.users VALUES (1, 'Alice', 25)";
        sqlParser.executeSQL(insertSQL);
        
        // 3. 验证数据插入成功
        String selectSQL = "SELECT * FROM test_db.users";
        sqlParser.executeSQL(selectSQL);
        
        logger.info("测试通过：INSERT语句支持数据库名");
    }
    
    @Test
    public void testSelectWithDatabaseName() throws Exception {
        logger.info("=== 测试 SELECT 语句（带数据库名）===");
        
        // 1. 创建数据库和表
        catalog.createDatabase("test_db", false);
        String createSQL = "CREATE TABLE test_db.products (" +
                          "id INT NOT NULL, " +
                          "name VARCHAR(50), " +
                          "price INT)";
        sqlParser.executeSQL(createSQL);
        
        // 2. 插入测试数据
        String insertSQL = "INSERT INTO test_db.products VALUES (1, 'Laptop', 5000)";
        sqlParser.executeSQL(insertSQL);
        
        // 3. 查询数据（使用完整表名）
        String selectSQL = "SELECT * FROM test_db.products WHERE price > 1000";
        sqlParser.executeSQL(selectSQL);
        
        logger.info("测试通过：SELECT语句支持数据库名");
    }
    
    @Test
    public void testDeleteWithDatabaseName() throws Exception {
        logger.info("=== 测试 DELETE 语句（带数据库名）===");
        
        // 1. 创建数据库和表
        catalog.createDatabase("test_db", false);
        String createSQL = "CREATE TABLE test_db.orders (" +
                          "id INT NOT NULL, " +
                          "status VARCHAR(50))";
        sqlParser.executeSQL(createSQL);
        
        // 2. 插入测试数据
        String insertSQL = "INSERT INTO test_db.orders VALUES (1, 'pending')";
        sqlParser.executeSQL(insertSQL);
        
        // 3. 删除数据（使用完整表名）
        String deleteSQL = "DELETE FROM test_db.orders WHERE status = 'pending'";
        sqlParser.executeSQL(deleteSQL);
        
        logger.info("测试通过：DELETE语句支持数据库名");
    }
    
    @Test
    public void testDropTableWithDatabaseName() throws Exception {
        logger.info("=== 测试 DROP TABLE 语句（带数据库名）===");
        
        // 1. 创建数据库和表
        catalog.createDatabase("test_db", false);
        String createSQL = "CREATE TABLE test_db.temp_table (id INT NOT NULL)";
        sqlParser.executeSQL(createSQL);
        
        // 2. 验证表存在
        Identifier identifier = new Identifier("test_db", "temp_table");
        assertTrue(catalog.tableExists(identifier));
        
        // 3. 删除表（使用完整表名）
        String dropSQL = "DROP TABLE test_db.temp_table";
        sqlParser.executeSQL(dropSQL);
        
        // 4. 验证表已删除
        assertFalse(catalog.tableExists(identifier));
        
        logger.info("测试通过：DROP TABLE语句支持数据库名");
    }
}
