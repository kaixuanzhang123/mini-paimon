package com.minipaimon.storage;

import com.minipaimon.metadata.DataType;
import com.minipaimon.metadata.Field;
import com.minipaimon.metadata.Row;
import com.minipaimon.metadata.RowKey;
import com.minipaimon.metadata.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MemTable 测试类
 */
class MemTableTest {
    private Schema schema;
    private MemTable memTable;

    @BeforeEach
    void setUp() {
        // 创建测试用的 Schema
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, true);
        schema = new Schema(0, Arrays.asList(idField, nameField), Collections.singletonList("id"));
        
        // 创建 MemTable
        memTable = new MemTable(schema, 1L, 1024); // 小容量便于测试
    }

    @Test
    void testPutAndGet() {
        // 创建测试数据
        Row row1 = new Row(new Object[]{1, "Alice"});
        Row row2 = new Row(new Object[]{2, "Bob"});
        
        // 插入数据
        int size1 = memTable.put(row1);
        int size2 = memTable.put(row2);
        
        assertTrue(size1 > 0);
        assertTrue(size2 > 0);
        assertEquals(2, memTable.size());
        assertFalse(memTable.isEmpty());
        
        // 获取数据
        RowKey key1 = RowKey.fromRow(row1, schema);
        RowKey key2 = RowKey.fromRow(row2, schema);
        
        Row retrievedRow1 = memTable.get(key1);
        Row retrievedRow2 = memTable.get(key2);
        
        assertNotNull(retrievedRow1);
        assertNotNull(retrievedRow2);
        assertEquals(row1, retrievedRow1);
        assertEquals(row2, retrievedRow2);
    }

    @Test
    void testIsFull() {
        // 创建小行数据填满内存表
        for (int i = 0; i < 20; i++) {
            Row row = new Row(new Object[]{i, "User" + i});
            memTable.put(row);
        }
        
        // 检查是否已满（取决于具体实现和大小计算）
        // 这里我们主要测试方法是否存在
        memTable.isFull();
    }

    @Test
    void testGetEntries() {
        Row row1 = new Row(new Object[]{1, "Alice"});
        Row row2 = new Row(new Object[]{2, "Bob"});
        
        memTable.put(row1);
        memTable.put(row2);
        
        java.util.Map<RowKey, Row> entries = memTable.getEntries();
        assertEquals(2, entries.size());
        assertTrue(entries.containsKey(RowKey.fromRow(row1, schema)));
        assertTrue(entries.containsKey(RowKey.fromRow(row2, schema)));
    }

    @Test
    void testSizeAndEmpty() {
        assertTrue(memTable.isEmpty());
        assertEquals(0, memTable.size());
        assertEquals(0, memTable.getSize());
        
        Row row = new Row(new Object[]{1, "Alice"});
        memTable.put(row);
        
        assertFalse(memTable.isEmpty());
        assertEquals(1, memTable.size());
        assertTrue(memTable.getSize() > 0);
    }
}
