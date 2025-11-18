package com.mini.paimon.schema;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RowKey 测试类
 */
class RowKeyTest {

    @Test
    void testFromRow() {
        Field idField = new Field("id", DataType.INT(), false);
        Field nameField = new Field("name", DataType.STRING(), true);
        Schema schema = new Schema(0, Arrays.asList(idField, nameField), 
                Collections.singletonList("id"));
        
        Row row = new Row(new Object[]{1, "Alice"});
        RowKey rowKey = RowKey.fromRow(row, schema);
        
        assertNotNull(rowKey);
        assertTrue(rowKey.size() > 0);
    }

    @Test
    void testCompareTo() {
        Field idField = new Field("id", DataType.INT(), false);
        Schema schema = new Schema(0, Collections.singletonList(idField), 
                Collections.singletonList("id"));
        
        Row row1 = new Row(new Object[]{1});
        Row row2 = new Row(new Object[]{2});
        Row row3 = new Row(new Object[]{1});
        
        RowKey key1 = RowKey.fromRow(row1, schema);
        RowKey key2 = RowKey.fromRow(row2, schema);
        RowKey key3 = RowKey.fromRow(row3, schema);
        
        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
        assertEquals(0, key1.compareTo(key3));
    }

    @Test
    void testEquals() {
        Field idField = new Field("id", DataType.INT(), false);
        Schema schema = new Schema(0, Collections.singletonList(idField), 
                Collections.singletonList("id"));
        
        Row row1 = new Row(new Object[]{1});
        Row row2 = new Row(new Object[]{1});
        
        RowKey key1 = RowKey.fromRow(row1, schema);
        RowKey key2 = RowKey.fromRow(row2, schema);
        
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void testNullPrimaryKey() {
        Field idField = new Field("id", DataType.INT(), false);
        Schema schema = new Schema(0, Collections.singletonList(idField), 
                Collections.singletonList("id"));
        
        Row row = new Row(new Object[]{null});
        
        assertThrows(IllegalArgumentException.class, () -> RowKey.fromRow(row, schema));
    }
}
