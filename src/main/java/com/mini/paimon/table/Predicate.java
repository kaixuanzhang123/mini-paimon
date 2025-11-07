package com.mini.paimon.table;

import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Predicate
 * 谓词过滤条件，用于 WHERE 子句
 */
public abstract class Predicate {
    
    /**
     * 测试行是否满足条件
     */
    public abstract boolean test(Row row, Schema schema);
    
    /**
     * AND 组合
     */
    public Predicate and(Predicate other) {
        return new AndPredicate(this, other);
    }
    
    /**
     * OR 组合
     */
    public Predicate or(Predicate other) {
        return new OrPredicate(this, other);
    }
    
    /**
     * 比较操作符
     */
    public enum CompareOp {
        EQ,  // =
        NE,  // !=
        GT,  // >
        GE,  // >=
        LT,  // <
        LE   // <=
    }
    
    /**
     * 字段比较谓词
     */
    public static class FieldPredicate extends Predicate {
        private final String fieldName;
        private final CompareOp op;
        private final Object value;
        
        public FieldPredicate(String fieldName, CompareOp op, Object value) {
            this.fieldName = fieldName;
            this.op = op;
            this.value = value;
        }
        
        public String getFieldName() {
            return fieldName;
        }
        
        public CompareOp getOp() {
            return op;
        }
        
        public Object getValue() {
            return value;
        }
        
        @Override
        public boolean test(Row row, Schema schema) {
            // 查找字段索引
            int fieldIndex = findFieldIndex(schema, fieldName);
            if (fieldIndex == -1) {
                throw new IllegalArgumentException("Field not found: " + fieldName);
            }
            
            Object fieldValue = row.getValues()[fieldIndex];
            
            if (fieldValue == null || value == null) {
                return op == CompareOp.NE;
            }
            
            return compare(fieldValue, value, op);
        }
        
        private int findFieldIndex(Schema schema, String fieldName) {
            List<Field> fields = schema.getFields();
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).getName().equals(fieldName)) {
                    return i;
                }
            }
            return -1;
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        private boolean compare(Object left, Object right, CompareOp op) {
            if (!(left instanceof Comparable)) {
                return op == CompareOp.EQ ? left.equals(right) : !left.equals(right);
            }
            
            int cmp = ((Comparable) left).compareTo(right);
            
            switch (op) {
                case EQ: return cmp == 0;
                case NE: return cmp != 0;
                case GT: return cmp > 0;
                case GE: return cmp >= 0;
                case LT: return cmp < 0;
                case LE: return cmp <= 0;
                default: throw new IllegalArgumentException("Unsupported operator: " + op);
            }
        }
    }
    
    /**
     * AND 谓词
     */
    public static class AndPredicate extends Predicate {
        private final Predicate left;
        private final Predicate right;
        
        public AndPredicate(Predicate left, Predicate right) {
            this.left = left;
            this.right = right;
        }
        
        @Override
        public boolean test(Row row, Schema schema) {
            return left.test(row, schema) && right.test(row, schema);
        }
    }
    
    /**
     * OR 谓词
     */
    public static class OrPredicate extends Predicate {
        private final Predicate left;
        private final Predicate right;
        
        public OrPredicate(Predicate left, Predicate right) {
            this.left = left;
            this.right = right;
        }
        
        @Override
        public boolean test(Row row, Schema schema) {
            return left.test(row, schema) || right.test(row, schema);
        }
    }
    
    /**
     * 创建相等谓词
     */
    public static Predicate equal(String field, Object value) {
        return new FieldPredicate(field, CompareOp.EQ, value);
    }
    
    /**
     * 创建不等谓词
     */
    public static Predicate notEqual(String field, Object value) {
        return new FieldPredicate(field, CompareOp.NE, value);
    }
    
    /**
     * 创建大于谓词
     */
    public static Predicate greaterThan(String field, Object value) {
        return new FieldPredicate(field, CompareOp.GT, value);
    }
    
    /**
     * 创建大于等于谓词
     */
    public static Predicate greaterOrEqual(String field, Object value) {
        return new FieldPredicate(field, CompareOp.GE, value);
    }
    
    /**
     * 创建小于谓词
     */
    public static Predicate lessThan(String field, Object value) {
        return new FieldPredicate(field, CompareOp.LT, value);
    }
    
    /**
     * 创建小于等于谓词
     */
    public static Predicate lessOrEqual(String field, Object value) {
        return new FieldPredicate(field, CompareOp.LE, value);
    }
}
