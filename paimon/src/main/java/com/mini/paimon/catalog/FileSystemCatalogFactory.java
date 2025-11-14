package com.mini.paimon.catalog;

/**
 * FileSystemCatalog 工厂实现
 * 参考 Apache Paimon 的 HiveCatalogFactory 设计
 * 
 * 通过 SPI 机制注册，支持通过配置创建 FileSystemCatalog
 */
public class FileSystemCatalogFactory implements CatalogFactory {
    
    /** Catalog 类型标识符 */
    public static final String IDENTIFIER = "filesystem";
    
    /** Catalog 名称配置键 */
    public static final String CATALOG_NAME = "catalog.name";
    
    /** 默认数据库配置键 */
    public static final String DEFAULT_DATABASE = "catalog.default-database";
    
    /** 默认数据库名 */
    private static final String DEFAULT_DATABASE_NAME = "default";
    
    @Override
    public String identifier() {
        return IDENTIFIER;
    }
    
    @Override
    public Catalog create(CatalogContext context) {
        // 从 context 中获取配置
        String catalogName = context.getOption(CATALOG_NAME, "paimon");
        String defaultDatabase = context.getOption(DEFAULT_DATABASE, DEFAULT_DATABASE_NAME);
        
        // 创建 FileSystemCatalog
        return new FileSystemCatalog(catalogName, defaultDatabase, context);
    }
}

