package com.mini.paimon.metadata;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Schema 测试类
 */
class SchemaTest {

    @Test
    void testCreateSchema() {
        Field idField = new Field("id", DataType.INT(), false);
        Field nameField = new Field("name", DataType.STRING(), true);
        
        Schema schema = new Schema(0, Arrays.asList(idField, nameField), 
                Collections.singletonList("id"));
        
        assertEquals(0, schema.getSchemaId());
        assertEquals(2, schema.getFields().size());
        assertEquals(1, schema.getPrimaryKeys().size());
        assertEquals("id", schema.getPrimaryKeys().get(0));
    }

    @Test
    void testSchemaValidation() {
        Field idField = new Field("id", DataType.INT(), false);
        
        // 测试主键字段不存在
        assertThrows(IllegalArgumentException.class, () -> {
            new Schema(0, Collections.singletonList(idField), 
                    Collections.singletonList("non_existent"));
        });
        
        // 测试空字段列表
        assertThrows(IllegalArgumentException.class, () -> {
            new Schema(0, Collections.emptyList(), 
                    Collections.singletonList("id"));
        });
        
        // 测试空主键列表 - 这在当前实现中是允许的，所以不会抛出异常
        assertDoesNotThrow(() -> {
            new Schema(0, Collections.singletonList(idField), 
                    Collections.emptyList());
        });
    }

    @Test
    void testGetField() {
        Field idField = new Field("id", DataType.INT(), false);
        Field nameField = new Field("name", DataType.STRING(), true);
        
        Schema schema = new Schema(0, Arrays.asList(idField, nameField), 
                Collections.singletonList("id"));
        
        assertEquals(idField, schema.getField("id"));
        assertEquals(nameField, schema.getField("name"));
        assertNull(schema.getField("non_existent"));
    }

    @Test
    void testGetFieldIndex() {
        Field idField = new Field("id", DataType.INT(), false);
        Field nameField = new Field("name", DataType.STRING(), true);
        
        Schema schema = new Schema(0, Arrays.asList(idField, nameField), 
                Collections.singletonList("id"));
        
        assertEquals(0, schema.getFieldIndex("id"));
        assertEquals(1, schema.getFieldIndex("name"));
        assertEquals(-1, schema.getFieldIndex("non_existent"));
    }
}