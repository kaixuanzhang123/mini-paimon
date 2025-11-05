package com.minipaimon.metadata;

import com.minipaimon.utils.PathFactory;
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
 * SchemaManager 测试类
 */
class SchemaManagerTest {
    private SchemaManager schemaManager;
    private PathFactory pathFactory;
    private String testWarehousePath;

    @BeforeEach
    void setUp() throws IOException {
        // 创建临时测试目录
        testWarehousePath = "./test-warehouse-schema";
        pathFactory = new PathFactory(testWarehousePath);
        
        // 创建表目录
        pathFactory.createTableDirectories("test_db", "test_table");
        
        // 创建 SchemaManager
        schemaManager = new SchemaManager(pathFactory, "test_db", "test_table");
    }

    @AfterEach
    void tearDown() throws IOException {
        // 清理测试目录
        deleteDirectory(Paths.get(testWarehousePath));
    }

    @Test
    void testCreateNewSchemaVersion() throws IOException {
        // 创建字段
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, true);
        
        // 创建新的 Schema 版本
        Schema schema = schemaManager.createNewSchemaVersion(
            Arrays.asList(idField, nameField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        assertNotNull(schema);
        assertEquals(0, schema.getSchemaId());
        assertEquals(2, schema.getFields().size());
        assertEquals(1, schema.getPrimaryKeys().size());
        assertEquals("id", schema.getPrimaryKeys().get(0));
        
        // 验证文件已创建
        Path schemaPath = pathFactory.getSchemaPath("test_db", "test_table", 0);
        assertTrue(Files.exists(schemaPath));
    }

    @Test
    void testLoadSchema() throws IOException {
        // 先创建一个 Schema
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, true);
        
        Schema originalSchema = schemaManager.createNewSchemaVersion(
            Arrays.asList(idField, nameField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        // 重新加载 Schema
        Schema loadedSchema = schemaManager.loadSchema(0);
        
        assertNotNull(loadedSchema);
        assertEquals(originalSchema, loadedSchema);
        assertEquals(0, loadedSchema.getSchemaId());
        assertEquals(2, loadedSchema.getFields().size());
    }

    @Test
    void testGetCurrentSchema() throws IOException {
        // 先创建一个 Schema
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, true);
        
        Schema schema = schemaManager.createNewSchemaVersion(
            Arrays.asList(idField, nameField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        // 获取当前 Schema
        Schema currentSchema = schemaManager.getCurrentSchema();
        
        assertNotNull(currentSchema);
        assertEquals(schema, currentSchema);
    }

    @Test
    void testLoadLatestSchema() throws IOException {
        // 创建多个 Schema 版本
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, true);
        Field ageField = new Field("age", DataType.INT, true);
        
        Schema schema1 = schemaManager.createNewSchemaVersion(
            Arrays.asList(idField, nameField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        Schema schema2 = schemaManager.createNewSchemaVersion(
            Arrays.asList(idField, nameField, ageField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        // 加载最新的 Schema
        Schema latestSchema = schemaManager.loadLatestSchema();
        
        assertNotNull(latestSchema);
        assertEquals(1, latestSchema.getSchemaId());
        assertEquals(3, latestSchema.getFields().size());
        assertEquals(schema2, latestSchema);
    }

    @Test
    void testHasSchema() throws IOException {
        assertFalse(schemaManager.hasSchema());
        
        // 创建一个 Schema
        Field idField = new Field("id", DataType.INT, false);
        schemaManager.createNewSchemaVersion(
            Collections.singletonList(idField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        assertTrue(schemaManager.hasSchema());
    }

    @Test
    void testGetSchemaVersionCount() throws IOException {
        assertEquals(0, schemaManager.getSchemaVersionCount());
        
        // 创建多个 Schema 版本
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, true);
        
        schemaManager.createNewSchemaVersion(
            Collections.singletonList(idField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        assertEquals(1, schemaManager.getSchemaVersionCount());
        
        schemaManager.createNewSchemaVersion(
            Arrays.asList(idField, nameField),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        assertEquals(2, schemaManager.getSchemaVersionCount());
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
