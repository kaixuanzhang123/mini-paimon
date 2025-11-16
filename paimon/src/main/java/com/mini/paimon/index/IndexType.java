package com.mini.paimon.index;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 索引类型枚举
 * 定义系统支持的所有索引类型
 */
public enum IndexType {
    /** 布隆过滤器索引 - 用于快速判断值是否存在 */
    BLOOM_FILTER("bloom_filter", ".bfi"),
    
    /** 最小最大值索引 - 用于范围查询过滤 */
    MIN_MAX("min_max", ".mmi"),
    
    /** 位图索引 - 用于低基数列的行级精确过滤 */
    BITMAP("bitmap", ".bmi");
    
    private final String name;
    private final String fileSuffix;
    
    IndexType(String name, String fileSuffix) {
        this.name = name;
        this.fileSuffix = fileSuffix;
    }
    
    @JsonValue
    public String getName() {
        return name;
    }
    
    public String getFileSuffix() {
        return fileSuffix;
    }
    
    /**
     * 根据名称获取索引类型
     */
    public static IndexType fromName(String name) {
        for (IndexType type : values()) {
            if (type.name.equals(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown index type: " + name);
    }
}
