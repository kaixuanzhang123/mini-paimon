package com.mini.paimon.index;

import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.DataType;
import com.mini.paimon.table.Predicate;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * BitmapIndex测试
 */
public class BitmapIndexTest {
    
    @Test
    public void testAddWithRowNumber() {
        BitmapIndex index = new BitmapIndex("status");
        
        // 添加数据
        index.addWithRowNumber("ACTIVE", 0);
        index.addWithRowNumber("PENDING", 1);
        index.addWithRowNumber("ACTIVE", 2);
        index.addWithRowNumber("DELETED", 3);
        index.addWithRowNumber("ACTIVE", 4);
        
        // 验证
        assertEquals(5, index.getRowCount());
        assertEquals(3, index.getDistinctValueCount()); // ACTIVE, PENDING, DELETED
        
        assertTrue(index.mightContain("ACTIVE"));
        assertTrue(index.mightContain("PENDING"));
        assertTrue(index.mightContain("DELETED"));
        assertFalse(index.mightContain("UNKNOWN"));
    }
    
    @Test
    public void testFilterEqual() {
        BitmapIndex index = new BitmapIndex("status");
        
        index.addWithRowNumber("ACTIVE", 0);
        index.addWithRowNumber("PENDING", 1);
        index.addWithRowNumber("ACTIVE", 2);
        index.addWithRowNumber("DELETED", 3);
        index.addWithRowNumber("ACTIVE", 4);
        
        // 等值查询
        Predicate predicate = Predicate.equal("status", "ACTIVE");
        SimpleBitmap result = index.filter(predicate);
        
        assertNotNull(result);
        assertEquals(3, result.getCardinality());
        assertTrue(result.contains(0));
        assertTrue(result.contains(2));
        assertTrue(result.contains(4));
        assertFalse(result.contains(1));
        assertFalse(result.contains(3));
    }
    
    @Test
    public void testFilterNotEqual() {
        BitmapIndex index = new BitmapIndex("status");
        
        index.addWithRowNumber("ACTIVE", 0);
        index.addWithRowNumber("PENDING", 1);
        index.addWithRowNumber("ACTIVE", 2);
        
        // 不等值查询
        Predicate predicate = Predicate.notEqual("status", "ACTIVE");
        SimpleBitmap result = index.filter(predicate);
        
        assertNotNull(result);
        assertEquals(1, result.getCardinality());
        assertFalse(result.contains(0));
        assertTrue(result.contains(1));
        assertFalse(result.contains(2));
    }
    
    @Test
    public void testFilterIn() {
        BitmapIndex index = new BitmapIndex("status");
        
        index.addWithRowNumber("ACTIVE", 0);
        index.addWithRowNumber("PENDING", 1);
        index.addWithRowNumber("DELETED", 2);
        index.addWithRowNumber("ACTIVE", 3);
        index.addWithRowNumber("PENDING", 4);
        
        // IN查询
        List<Object> values = Arrays.asList("ACTIVE", "PENDING");
        BitmapIndex.InPredicate predicate = new BitmapIndex.InPredicate("status", values);
        SimpleBitmap result = index.filter(predicate);
        
        assertNotNull(result);
        assertEquals(4, result.getCardinality());
        assertTrue(result.contains(0));
        assertTrue(result.contains(1));
        assertFalse(result.contains(2));
        assertTrue(result.contains(3));
        assertTrue(result.contains(4));
    }
    
    @Test
    public void testFilterNull() {
        BitmapIndex index = new BitmapIndex("optional_field");
        
        index.addWithRowNumber("value1", 0);
        index.addWithRowNumber(null, 1);
        index.addWithRowNumber("value2", 2);
        index.addWithRowNumber(null, 3);
        
        // 查询null值
        Predicate predicate = Predicate.equal("optional_field", null);
        SimpleBitmap result = index.filter(predicate);
        
        assertNotNull(result);
        assertEquals(2, result.getCardinality());
        assertFalse(result.contains(0));
        assertTrue(result.contains(1));
        assertFalse(result.contains(2));
        assertTrue(result.contains(3));
    }
    
    @Test
    public void testFilterAndCombination() {
        BitmapIndex index1 = new BitmapIndex("field1");
        index1.addWithRowNumber("A", 0);
        index1.addWithRowNumber("A", 1);
        index1.addWithRowNumber("B", 2);
        index1.addWithRowNumber("A", 3);
        
        BitmapIndex index2 = new BitmapIndex("field2");
        index2.addWithRowNumber("X", 0);
        index2.addWithRowNumber("Y", 1);
        index2.addWithRowNumber("X", 2);
        index2.addWithRowNumber("X", 3);
        
        // 单独查询
        Predicate pred1 = Predicate.equal("field1", "A");
        SimpleBitmap result1 = index1.filter(pred1);
        assertEquals(3, result1.getCardinality());
        
        Predicate pred2 = Predicate.equal("field2", "X");
        SimpleBitmap result2 = index2.filter(pred2);
        assertEquals(3, result2.getCardinality());
        
        // AND组合
        SimpleBitmap combined = SimpleBitmap.and(result1, result2);
        assertEquals(2, combined.getCardinality());
        assertTrue(combined.contains(0));
        assertFalse(combined.contains(1));
        assertFalse(combined.contains(2));
        assertTrue(combined.contains(3));
    }
    
    @Test
    public void testSerialization() throws IOException {
        BitmapIndex index = new BitmapIndex("status");
        
        index.addWithRowNumber("ACTIVE", 0);
        index.addWithRowNumber("PENDING", 1);
        index.addWithRowNumber("ACTIVE", 2);
        index.addWithRowNumber(null, 3);
        
        // 序列化
        byte[] data = index.serialize();
        assertNotNull(data);
        assertTrue(data.length > 0);
        
        // 反序列化
        BitmapIndex deserialized = new BitmapIndex("status");
        deserialized.deserialize(data);
        
        // 验证
        assertEquals(4, deserialized.getRowCount());
        assertEquals(3, deserialized.getDistinctValueCount());
        
        // 验证过滤功能
        Predicate predicate = Predicate.equal("status", "ACTIVE");
        SimpleBitmap result = deserialized.filter(predicate);
        assertEquals(2, result.getCardinality());
        assertTrue(result.contains(0));
        assertTrue(result.contains(2));
    }
    
    @Test
    public void testInPredicateTest() {
        // 创建Schema
        Schema schema = new Schema(
            0,
            Arrays.asList(
                new Field("id", DataType.INT(), false),
                new Field("status", DataType.STRING(), false)
            ),
            Arrays.asList("id")
        );
        
        // 创建Row
        Row row1 = new Row(new Object[]{1, "ACTIVE"});
        Row row2 = new Row(new Object[]{2, "PENDING"});
        Row row3 = new Row(new Object[]{3, "DELETED"});
        
        // 测试IN谓词
        List<Object> values = Arrays.asList("ACTIVE", "PENDING");
        BitmapIndex.InPredicate predicate = new BitmapIndex.InPredicate("status", values);
        
        assertTrue(predicate.test(row1, schema));
        assertTrue(predicate.test(row2, schema));
        assertFalse(predicate.test(row3, schema));
    }
}

