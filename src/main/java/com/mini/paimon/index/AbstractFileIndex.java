package com.mini.paimon.index;

/**
 * 索引抽象基类
 * 提供索引的通用实现
 */
public abstract class AbstractFileIndex implements FileIndex {
    
    protected final IndexType indexType;
    protected final String fieldName;
    
    public AbstractFileIndex(IndexType indexType, String fieldName) {
        this.indexType = indexType;
        this.fieldName = fieldName;
    }
    
    @Override
    public IndexType getIndexType() {
        return indexType;
    }
    
    @Override
    public String getFieldName() {
        return fieldName;
    }
    
    /**
     * 默认实现：不支持范围查询
     * 子类可以覆盖此方法提供范围查询支持
     */
    @Override
    public boolean mightIntersect(Object min, Object max) {
        return true;
    }
    
    /**
     * 估算对象的内存大小
     */
    protected long estimateObjectSize(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof String) {
            return ((String) value).length() * 2 + 40;
        }
        if (value instanceof Integer) {
            return 4;
        }
        if (value instanceof Long) {
            return 8;
        }
        if (value instanceof Double) {
            return 8;
        }
        if (value instanceof Boolean) {
            return 1;
        }
        return 100;
    }
}
