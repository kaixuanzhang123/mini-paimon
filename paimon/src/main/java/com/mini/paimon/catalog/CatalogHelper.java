package com.mini.paimon.catalog;

import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.table.Table;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Catalog 辅助类
 * 提供便捷方法用于测试和示例代码
 */
public class CatalogHelper {
    
    private final Catalog catalog;
    private final String defaultDatabase;
    
    public CatalogHelper(PathFactory pathFactory, String warehousePath) {
        this(pathFactory, warehousePath, "default");
    }
    
    public CatalogHelper(PathFactory pathFactory, String warehousePath, String defaultDatabase) {
        // 使用 CatalogLoader 通过 SPI 机制加载 Catalog
        CatalogContext context = CatalogContext.builder()
            .warehouse(warehousePath)
            .option("catalog.name", "mini_paimon")
            .option("catalog.default-database", defaultDatabase)
            .build();
        this.catalog = CatalogLoader.load("filesystem", context);
        this.defaultDatabase = defaultDatabase;
    }
    
    /**
     * 创建表（如果不存在）
     */
    public void createTableIfNotExists(String tableName, List<Field> fields, List<String> primaryKeys) 
            throws IOException {
        try {
            catalog.createDatabase(defaultDatabase, true);
            Identifier identifier = new Identifier(defaultDatabase, tableName);
            Schema schema = new Schema(0, fields, primaryKeys, Collections.emptyList());
            catalog.createTable(identifier, schema, true);
        } catch (Exception e) {
            throw new IOException("Failed to create table: " + tableName, e);
        }
    }
    
    /**
     * 获取 Table 实例
     */
    public Table getTable(String tableName) throws IOException {
        try {
            Identifier identifier = new Identifier(defaultDatabase, tableName);
            return catalog.getTable(identifier);
        } catch (Exception e) {
            throw new IOException("Failed to get table: " + tableName, e);
        }
    }
    
    /**
     * 获取 Catalog
     */
    public Catalog getCatalog() {
        return catalog;
    }
    
    /**
     * 关闭 Catalog
     */
    public void close() throws IOException {
        try {
            catalog.close();
        } catch (Exception e) {
            throw new IOException("Failed to close catalog", e);
        }
    }
}
