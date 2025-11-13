package com.mini.paimon.metadata;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Row 测试类
 */
class RowTest {

    @Test
    void testCreateRow() {
        Row row = new Row(new Object[]{1, "Alice"});
        
        assertEquals(2, row.getFieldCount());
        assertEquals(1, row.getValue(0));
        assertEquals("Alice", row.getValue(1));
    }

    @Test
    void testValidate() {
        Field idField = new Field("id", DataType.INT(), false);
        Field nameField = new Field("name", DataType.STRING(), true);
        Schema schema = new Schema(0, Arrays.asList(idField, nameField), 
                Collections.singletonList("id"));
        
        // 有效的行
        Row validRow = new Row(new Object[]{1, "Alice"});
        assertDoesNotThrow(() -> validRow.validate(schema));
        
        // 字段数量不匹配
        Row invalidRow1 = new Row(new Object[]{1});
        assertThrows(IllegalArgumentException.class, () -> invalidRow1.validate(schema));
        
        // 非空字段为null
        Row invalidRow2 = new Row(new Object[]{null, "Alice"});
        assertThrows(IllegalArgumentException.class, () -> invalidRow2.validate(schema));
        
        // 类型不匹配
        Row invalidRow3 = new Row(new Object[]{"not_an_int", "Alice"});
        assertThrows(IllegalArgumentException.class, () -> invalidRow3.validate(schema));
    }
}
