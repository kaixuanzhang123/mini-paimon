package com.mini.paimon.filter;

import com.mini.paimon.partition.PartitionSpec;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PartitionPredicate单元测试
 */
public class PartitionPredicateTest {
    
    @Test
    public void testEqualPredicate() {
        PartitionPredicate pred = PartitionPredicate.equal("dt", "2024-01-01");
        
        Map<String, String> values1 = new LinkedHashMap<>();
        values1.put("dt", "2024-01-01");
        assertTrue(pred.test(new PartitionSpec(values1)));
        
        Map<String, String> values2 = new LinkedHashMap<>();
        values2.put("dt", "2024-01-02");
        assertFalse(pred.test(new PartitionSpec(values2)));
    }
    
    @Test
    public void testInPredicate() {
        PartitionPredicate pred = PartitionPredicate.in("dt", "2024-01-01", "2024-01-02");
        
        Map<String, String> values1 = new LinkedHashMap<>();
        values1.put("dt", "2024-01-01");
        assertTrue(pred.test(new PartitionSpec(values1)));
        
        Map<String, String> values2 = new LinkedHashMap<>();
        values2.put("dt", "2024-01-03");
        assertFalse(pred.test(new PartitionSpec(values2)));
    }
    
    @Test
    public void testRangePredicate() {
        PartitionPredicate pred = PartitionPredicate.greaterThanOrEqual("dt", "2024-01-02");
        
        Map<String, String> values1 = new LinkedHashMap<>();
        values1.put("dt", "2024-01-01");
        assertFalse(pred.test(new PartitionSpec(values1)));
        
        Map<String, String> values2 = new LinkedHashMap<>();
        values2.put("dt", "2024-01-02");
        assertTrue(pred.test(new PartitionSpec(values2)));
        
        Map<String, String> values3 = new LinkedHashMap<>();
        values3.put("dt", "2024-01-03");
        assertTrue(pred.test(new PartitionSpec(values3)));
    }
    
    @Test
    public void testBetweenPredicate() {
        PartitionPredicate pred = PartitionPredicate.between("dt", "2024-01-02", "2024-01-05");
        
        Map<String, String> values1 = new LinkedHashMap<>();
        values1.put("dt", "2024-01-01");
        assertFalse(pred.test(new PartitionSpec(values1)));
        
        Map<String, String> values2 = new LinkedHashMap<>();
        values2.put("dt", "2024-01-03");
        assertTrue(pred.test(new PartitionSpec(values2)));
        
        Map<String, String> values3 = new LinkedHashMap<>();
        values3.put("dt", "2024-01-06");
        assertFalse(pred.test(new PartitionSpec(values3)));
    }
    
    @Test
    public void testPrefixPredicate() {
        PartitionPredicate pred = PartitionPredicate.prefix("dt", "2024-01");
        
        Map<String, String> values1 = new LinkedHashMap<>();
        values1.put("dt", "2024-01-01");
        assertTrue(pred.test(new PartitionSpec(values1)));
        
        Map<String, String> values2 = new LinkedHashMap<>();
        values2.put("dt", "2024-02-01");
        assertFalse(pred.test(new PartitionSpec(values2)));
    }
    
    @Test
    public void testAndPredicate() {
        PartitionPredicate pred = PartitionPredicate.equal("dt", "2024-01-01")
            .and(PartitionPredicate.equal("hour", "10"));
        
        Map<String, String> values1 = new LinkedHashMap<>();
        values1.put("dt", "2024-01-01");
        values1.put("hour", "10");
        assertTrue(pred.test(new PartitionSpec(values1)));
        
        Map<String, String> values2 = new LinkedHashMap<>();
        values2.put("dt", "2024-01-01");
        values2.put("hour", "11");
        assertFalse(pred.test(new PartitionSpec(values2)));
    }
    
    @Test
    public void testOrPredicate() {
        PartitionPredicate pred = PartitionPredicate.equal("dt", "2024-01-01")
            .or(PartitionPredicate.equal("dt", "2024-01-02"));
        
        Map<String, String> values1 = new LinkedHashMap<>();
        values1.put("dt", "2024-01-01");
        assertTrue(pred.test(new PartitionSpec(values1)));
        
        Map<String, String> values2 = new LinkedHashMap<>();
        values2.put("dt", "2024-01-02");
        assertTrue(pred.test(new PartitionSpec(values2)));
        
        Map<String, String> values3 = new LinkedHashMap<>();
        values3.put("dt", "2024-01-03");
        assertFalse(pred.test(new PartitionSpec(values3)));
    }
    
    @Test
    public void testNotPredicate() {
        PartitionPredicate pred = PartitionPredicate.equal("dt", "2024-01-01").not();
        
        Map<String, String> values1 = new LinkedHashMap<>();
        values1.put("dt", "2024-01-01");
        assertFalse(pred.test(new PartitionSpec(values1)));
        
        Map<String, String> values2 = new LinkedHashMap<>();
        values2.put("dt", "2024-01-02");
        assertTrue(pred.test(new PartitionSpec(values2)));
    }
    
    @Test
    public void testComplexPredicate() {
        // (dt='2024-01-01' AND hour >= '12') OR (dt='2024-01-02')
        PartitionPredicate pred = 
            PartitionPredicate.equal("dt", "2024-01-01")
                .and(PartitionPredicate.greaterThanOrEqual("hour", "12"))
            .or(PartitionPredicate.equal("dt", "2024-01-02"));
        
        Map<String, String> values1 = new LinkedHashMap<>();
        values1.put("dt", "2024-01-01");
        values1.put("hour", "10");
        assertFalse(pred.test(new PartitionSpec(values1)));
        
        Map<String, String> values2 = new LinkedHashMap<>();
        values2.put("dt", "2024-01-01");
        values2.put("hour", "13");
        assertTrue(pred.test(new PartitionSpec(values2)));
        
        Map<String, String> values3 = new LinkedHashMap<>();
        values3.put("dt", "2024-01-02");
        values3.put("hour", "10");
        assertTrue(pred.test(new PartitionSpec(values3)));
    }
    
    @Test
    public void testAlwaysTruePredicate() {
        PartitionPredicate pred = PartitionPredicate.alwaysTrue();
        
        Map<String, String> values = new LinkedHashMap<>();
        values.put("dt", "2024-01-01");
        assertTrue(pred.test(new PartitionSpec(values)));
    }
    
    @Test
    public void testAlwaysFalsePredicate() {
        PartitionPredicate pred = PartitionPredicate.alwaysFalse();
        
        Map<String, String> values = new LinkedHashMap<>();
        values.put("dt", "2024-01-01");
        assertFalse(pred.test(new PartitionSpec(values)));
    }
}
