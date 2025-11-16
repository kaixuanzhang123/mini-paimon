package com.mini.paimon.index;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SimpleBitmap测试
 */
public class SimpleBitmapTest {
    
    @Test
    public void testAddAndContains() {
        SimpleBitmap bitmap = new SimpleBitmap();
        
        // 添加行号
        bitmap.add(0);
        bitmap.add(5);
        bitmap.add(10);
        bitmap.add(100);
        
        // 检查包含
        assertTrue(bitmap.contains(0));
        assertTrue(bitmap.contains(5));
        assertTrue(bitmap.contains(10));
        assertTrue(bitmap.contains(100));
        
        // 检查不包含
        assertFalse(bitmap.contains(1));
        assertFalse(bitmap.contains(4));
        assertFalse(bitmap.contains(99));
        
        // 检查基数
        assertEquals(4, bitmap.getCardinality());
    }
    
    @Test
    public void testDuplicateAdd() {
        SimpleBitmap bitmap = new SimpleBitmap();
        
        bitmap.add(5);
        bitmap.add(5);
        bitmap.add(5);
        
        // 基数应该是1，不是3
        assertEquals(1, bitmap.getCardinality());
        assertTrue(bitmap.contains(5));
    }
    
    @Test
    public void testAnd() {
        SimpleBitmap bitmap1 = new SimpleBitmap();
        bitmap1.add(1);
        bitmap1.add(2);
        bitmap1.add(3);
        bitmap1.add(4);
        
        SimpleBitmap bitmap2 = new SimpleBitmap();
        bitmap2.add(2);
        bitmap2.add(3);
        bitmap2.add(5);
        bitmap2.add(6);
        
        SimpleBitmap result = SimpleBitmap.and(bitmap1, bitmap2);
        
        // 交集应该包含2和3
        assertEquals(2, result.getCardinality());
        assertTrue(result.contains(2));
        assertTrue(result.contains(3));
        assertFalse(result.contains(1));
        assertFalse(result.contains(5));
    }
    
    @Test
    public void testOr() {
        SimpleBitmap bitmap1 = new SimpleBitmap();
        bitmap1.add(1);
        bitmap1.add(2);
        
        SimpleBitmap bitmap2 = new SimpleBitmap();
        bitmap2.add(2);
        bitmap2.add(3);
        
        SimpleBitmap result = SimpleBitmap.or(bitmap1, bitmap2);
        
        // 并集应该包含1,2,3
        assertEquals(3, result.getCardinality());
        assertTrue(result.contains(1));
        assertTrue(result.contains(2));
        assertTrue(result.contains(3));
    }
    
    @Test
    public void testAndNot() {
        SimpleBitmap bitmap1 = new SimpleBitmap();
        bitmap1.add(1);
        bitmap1.add(2);
        bitmap1.add(3);
        
        SimpleBitmap bitmap2 = new SimpleBitmap();
        bitmap2.add(2);
        
        SimpleBitmap result = SimpleBitmap.andNot(bitmap1, bitmap2);
        
        // 差集应该包含1和3，不包含2
        assertEquals(2, result.getCardinality());
        assertTrue(result.contains(1));
        assertFalse(result.contains(2));
        assertTrue(result.contains(3));
    }
    
    @Test
    public void testIterator() {
        SimpleBitmap bitmap = new SimpleBitmap();
        bitmap.add(0);
        bitmap.add(5);
        bitmap.add(10);
        bitmap.add(15);
        
        List<Integer> values = new ArrayList<>();
        Iterator<Integer> iter = bitmap.iterator();
        while (iter.hasNext()) {
            values.add(iter.next());
        }
        
        assertEquals(4, values.size());
        assertEquals(0, values.get(0).intValue());
        assertEquals(5, values.get(1).intValue());
        assertEquals(10, values.get(2).intValue());
        assertEquals(15, values.get(3).intValue());
    }
    
    @Test
    public void testSerialization() throws IOException {
        SimpleBitmap bitmap = new SimpleBitmap();
        bitmap.add(0);
        bitmap.add(5);
        bitmap.add(10);
        bitmap.add(100);
        bitmap.add(1000);
        
        // 序列化
        byte[] data = bitmap.serialize();
        assertNotNull(data);
        assertTrue(data.length > 0);
        
        // 反序列化
        SimpleBitmap deserialized = SimpleBitmap.deserialize(data);
        
        // 验证内容相同
        assertEquals(bitmap.getCardinality(), deserialized.getCardinality());
        assertTrue(deserialized.contains(0));
        assertTrue(deserialized.contains(5));
        assertTrue(deserialized.contains(10));
        assertTrue(deserialized.contains(100));
        assertTrue(deserialized.contains(1000));
        assertFalse(deserialized.contains(1));
    }
    
    @Test
    public void testEmpty() {
        SimpleBitmap bitmap = new SimpleBitmap();
        
        assertTrue(bitmap.isEmpty());
        assertEquals(0, bitmap.getCardinality());
        
        bitmap.add(1);
        assertFalse(bitmap.isEmpty());
        assertEquals(1, bitmap.getCardinality());
    }
}

