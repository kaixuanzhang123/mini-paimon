package com.mini.paimon.catalog;

import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CatalogFactory 和 SPI 机制测试
 */
public class CatalogFactoryTest {
    
    @TempDir
    Path tempDir;
    
    private Catalog catalog;
    
    @BeforeEach
    public void setUp() {
        // 使用 CatalogLoader 通过 SPI 机制加载 Catalog
        CatalogContext context = CatalogContext.builder()
            .warehouse(tempDir.toString())
            .option("catalog.name", "test_catalog")
            .option("catalog.default-database", "default")
            .build();
        
        catalog = CatalogLoader.load("filesystem", context);
        assertNotNull(catalog, "Catalog should be loaded via SPI");
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }
    
    @Test
    public void testCatalogFactoryIdentifier() {
        // 验证 FileSystemCatalogFactory 的标识符
        FileSystemCatalogFactory factory = new FileSystemCatalogFactory();
        assertEquals("filesystem", factory.identifier());
    }
    
    @Test
    public void testCatalogLoaderAvailableIdentifiers() {
        // 验证 CatalogLoader 能找到注册的 Factory
        assertTrue(CatalogLoader.isFactoryRegistered("filesystem"), 
                  "FileSystemCatalogFactory should be registered");
        assertTrue(CatalogLoader.getAvailableIdentifiers().contains("filesystem"),
                  "Available identifiers should contain 'filesystem'");
    }
    
    @Test
    public void testCreateCatalogViaSPI() throws Exception {
        // 验证通过 SPI 创建的 Catalog 能正常工作
        assertNotNull(catalog);
        assertEquals("test_catalog", catalog.name());
        assertEquals("default", catalog.getDefaultDatabase());
    }
    
    @Test
    public void testDatabaseOperations() throws Exception {
        // 测试数据库操作
        catalog.createDatabase("test_db", false);
        assertTrue(catalog.databaseExists("test_db"));
        
        catalog.dropDatabase("test_db", false, false);
        assertFalse(catalog.databaseExists("test_db"));
    }
    
    @Test
    public void testTableOperations() throws Exception {
        // 测试表操作
        catalog.createDatabase("test_db", false);
        
        Identifier identifier = new Identifier("test_db", "test_table");
        Schema schema = new Schema(
            0,
            Arrays.asList(
                new Field("id", DataType.LONG(), false),
                new Field("name", DataType.STRING(), true)
            ),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        catalog.createTable(identifier, schema, false);
        assertTrue(catalog.tableExists(identifier));
        
        Schema retrievedSchema = catalog.getTableSchema(identifier);
        assertNotNull(retrievedSchema);
        assertEquals(2, retrievedSchema.getFields().size());
        
        catalog.dropTable(identifier, false);
        assertFalse(catalog.tableExists(identifier));
    }
    
    @Test
    public void testCatalogContextOptions() {
        // 测试 CatalogContext 选项传递
        CatalogContext context = CatalogContext.builder()
            .warehouse(tempDir.toString())
            .option("catalog.name", "custom_catalog")
            .option("catalog.default-database", "custom_db")
            .option("custom.option", "custom_value")
            .build();
        
        assertEquals("custom_catalog", context.getOption("catalog.name"));
        assertEquals("custom_db", context.getOption("catalog.default-database"));
        assertEquals("custom_value", context.getOption("custom.option"));
        assertEquals("default_value", context.getOption("non.existent", "default_value"));
    }
    
    @Test
    public void testInvalidCatalogIdentifier() {
        // 测试无效的 Catalog 标识符
        CatalogContext context = CatalogContext.builder()
            .warehouse(tempDir.toString())
            .build();
        
        assertThrows(Exception.class, () -> {
            CatalogLoader.load("invalid_identifier", context);
        }, "Should throw exception for invalid catalog identifier");
    }
}

