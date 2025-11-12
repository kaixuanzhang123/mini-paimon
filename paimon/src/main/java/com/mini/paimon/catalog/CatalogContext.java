package com.mini.paimon.catalog;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Catalog 上下文配置
 * 参考 Apache Paimon 的 CatalogContext 设计
 * 
 * 用于配置 Catalog 的各种参数
 */
public class CatalogContext implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /** Warehouse 路径 */
    private final String warehouse;
    
    /** 其他配置选项 */
    private final Map<String, String> options;
    
    private CatalogContext(String warehouse, Map<String, String> options) {
        this.warehouse = Objects.requireNonNull(warehouse, "Warehouse path cannot be null");
        this.options = new HashMap<>(options);
    }
    
    public String getWarehouse() {
        return warehouse;
    }
    
    public Map<String, String> getOptions() {
        return new HashMap<>(options);
    }
    
    public String getOption(String key) {
        return options.get(key);
    }
    
    public String getOption(String key, String defaultValue) {
        return options.getOrDefault(key, defaultValue);
    }
    
    /**
     * 创建 CatalogContext Builder
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * CatalogContext Builder
     */
    public static class Builder {
        private String warehouse;
        private final Map<String, String> options = new HashMap<>();
        
        public Builder warehouse(String warehouse) {
            this.warehouse = warehouse;
            return this;
        }
        
        public Builder option(String key, String value) {
            this.options.put(key, value);
            return this;
        }
        
        public Builder options(Map<String, String> options) {
            this.options.putAll(options);
            return this;
        }
        
        public CatalogContext build() {
            if (warehouse == null || warehouse.isEmpty()) {
                throw new IllegalArgumentException("Warehouse path must be specified");
            }
            return new CatalogContext(warehouse, options);
        }
    }
    
    @Override
    public String toString() {
        return "CatalogContext{" +
                "warehouse='" + warehouse + '\'' +
                ", options=" + options +
                '}';
    }
}
