package com.mini.paimon.flink.procedure;

import com.mini.paimon.catalog.Catalog;

/**
 * Flink Procedure 基类
 * 
 * 提供通用的 Catalog 访问能力
 * 
 * 注意：此为简化实现，不依赖 Flink 的标准 Procedure 接口
 * 在实际使用中，可以通过 Flink SQL 的自定义函数或者扩展机制来调用
 */
public abstract class ProcedureBase {
    
    protected final Catalog catalog;
    
    public ProcedureBase(Catalog catalog) {
        this.catalog = catalog;
    }
    
    /**
     * 获取 Catalog 实例
     */
    protected Catalog catalog() {
        return catalog;
    }
    
    /**
     * 获取 Procedure 名称
     */
    public abstract String name();
    
    /**
     * 验证参数是否为空
     */
    protected void checkNotNull(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }
    
    /**
     * 验证字符串参数是否为空或空白
     */
    protected void checkNotBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " cannot be null or blank");
        }
    }
}

