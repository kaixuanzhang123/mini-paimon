package com.mini.paimon.data;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * BinaryRow 单元测试
 */
public class BinaryRowTest {
    
    @Test
    public void testFromObjects() {
        Object[] values = new Object[]{1, "test", 3.14};
        BinaryRow row = BinaryRow.fromObjects(values);
        
        assertNotNull("Row should not be null", row);
        assertEquals("Field count should be 3", 3, row.getFieldCount());
        assertFalse("Row should not be empty", row.isEmpty());
    }
    
    @Test
    public void testFromString() {
        String value = "partition=2024-01-01";
        BinaryRow row = BinaryRow.fromString(value);
        
        assertNotNull("Row should not be null", row);
        assertEquals("Field count should be 1", 1, row.getFieldCount());
        assertEquals("Debug string should match", value, row.toDebugString());
    }
    
    @Test
    public void testEmpty() {
        BinaryRow row = BinaryRow.empty();
        
        assertNotNull("Row should not be null", row);
        assertTrue("Row should be empty", row.isEmpty());
        assertEquals("Field count should be 0", 0, row.getFieldCount());
        assertEquals("Size should be 0", 0, row.getSizeInBytes());
    }
    
    @Test
    public void testEquals() {
        BinaryRow row1 = BinaryRow.fromString("test");
        BinaryRow row2 = BinaryRow.fromString("test");
        BinaryRow row3 = BinaryRow.fromString("other");
        
        assertEquals("Same content should be equal", row1, row2);
        assertNotEquals("Different content should not be equal", row1, row3);
        assertEquals("Hash codes should match", row1.hashCode(), row2.hashCode());
    }
    
    @Test
    public void testCompareTo() {
        BinaryRow row1 = BinaryRow.fromString("a");
        BinaryRow row2 = BinaryRow.fromString("b");
        BinaryRow row3 = BinaryRow.fromString("a");
        
        assertTrue("a < b", row1.compareTo(row2) < 0);
        assertTrue("b > a", row2.compareTo(row1) > 0);
        assertEquals("a == a", 0, row1.compareTo(row3));
    }
    
    @Test
    public void testCopy() {
        BinaryRow original = BinaryRow.fromString("test");
        BinaryRow copy = original.copy();
        
        assertEquals("Copy should equal original", original, copy);
        assertNotSame("Copy should be different object", original, copy);
        assertNotSame("Data should be copied", original.getData(), copy.getData());
    }
    
    @Test
    public void testSerialization() {
        BinaryRow original = BinaryRow.fromObjects(new Object[]{1, "test", 3.14});
        
        // 序列化
        ByteBuffer buffer = ByteBuffer.allocate(original.getSerializedSize());
        original.serializeTo(buffer);
        
        // 反序列化
        buffer.flip();
        BinaryRow deserialized = BinaryRow.deserializeFrom(buffer);
        
        assertEquals("Deserialized should equal original", original, deserialized);
        assertEquals("Field count should match", original.getFieldCount(), deserialized.getFieldCount());
    }
    
    @Test
    public void testBuilder() {
        BinaryRow row = BinaryRow.builder(3)
            .setField(0, 1)
            .setField(1, "test")
            .setField(2, 3.14)
            .build();
        
        assertNotNull("Row should not be null", row);
        assertEquals("Field count should be 3", 3, row.getFieldCount());
    }
    
    @Test(expected = IndexOutOfBoundsException.class)
    public void testBuilderOutOfBounds() {
        BinaryRow.builder(2)
            .setField(0, 1)
            .setField(2, "invalid") // 应该抛出异常
            .build();
    }
    
    @Test
    public void testGetField() {
        BinaryRow row = BinaryRow.fromObjects(new Object[]{"field1", "field2", "field3"});
        
        assertEquals("First field should match", "field1", row.getField(0));
        assertEquals("Second field should match", "field2", row.getField(1));
        assertEquals("Third field should match", "field3", row.getField(2));
    }
    
    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetFieldOutOfBounds() {
        BinaryRow row = BinaryRow.fromObjects(new Object[]{"field1"});
        row.getField(1); // 应该抛出异常
    }
    
    @Test
    public void testCompareToWithNull() {
        BinaryRow row = BinaryRow.fromString("test");
        assertTrue("Non-null should be greater than null", row.compareTo(null) > 0);
    }
    
    @Test
    public void testCompareToWithDifferentLengths() {
        BinaryRow short1 = BinaryRow.fromString("a");
        BinaryRow long1 = BinaryRow.fromString("abc");
        
        assertTrue("Shorter should be less than longer with same prefix", 
                  short1.compareTo(long1) < 0);
        assertTrue("Longer should be greater than shorter with same prefix", 
                  long1.compareTo(short1) > 0);
    }
    
    @Test
    public void testToString() {
        BinaryRow row = BinaryRow.fromObjects(new Object[]{1, 2});
        String str = row.toString();
        
        assertNotNull("toString should not return null", str);
        assertTrue("toString should contain field count", str.contains("fieldCount=2"));
        assertTrue("toString should contain size", str.contains("sizeInBytes="));
    }
    
    @Test
    public void testEmptySerialization() {
        BinaryRow empty = BinaryRow.empty();
        
        ByteBuffer buffer = ByteBuffer.allocate(empty.getSerializedSize());
        empty.serializeTo(buffer);
        
        buffer.flip();
        BinaryRow deserialized = BinaryRow.deserializeFrom(buffer);
        
        assertEquals("Deserialized empty should equal original", empty, deserialized);
        assertTrue("Deserialized should be empty", deserialized.isEmpty());
    }
}

