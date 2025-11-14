package com.mini.paimon.catalog;

/**
 * Catalog 工厂接口
 * 参考 Apache Paimon 的 CatalogFactory 设计
 * 
 * 通过 SPI (Service Provider Interface) 机制加载不同的 Catalog 实现
 * 每个 Catalog 实现需要提供一个对应的 Factory 实现
 */
public interface CatalogFactory {
    
    /**
     * 获取 Catalog 的唯一标识符
     * 
     * @return Catalog 类型标识符，例如 "filesystem", "hive" 等
     */
    String identifier();
    
    /**
     * 创建 Catalog 实例
     * 
     * @param context Catalog 上下文配置
     * @return Catalog 实例
     */
    Catalog create(CatalogContext context);
}

