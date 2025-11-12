package com.mini.paimon.index;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.sql.SQLParser;
import com.mini.paimon.utils.PathFactory;

import java.io.File;

/**
 * SQL 索引配置示例
 * 演示如何在 CREATE TABLE 时通过 TBLPROPERTIES 指定索引
 */
public class SQLIndexConfigExample {
    
    public static void main(String[] args) throws Exception {
        String warehouse = "warehouse_sql_index_demo";
        
        // 创建 Catalog
        CatalogContext context = CatalogContext.builder()
            .warehouse(warehouse)
            .build();
        Catalog catalog = new FileSystemCatalog("test_catalog", "default", context);
        PathFactory pathFactory = new PathFactory(warehouse);
        
        // 创建数据库
        catalog.createDatabase("test_db", true);
        
        // 创建 SQL 解析器
        SQLParser parser = new SQLParser(catalog, pathFactory);
        
        System.out.println("========== SQL 索引配置示例 ==========\n");
        
        // 示例1: 为所有字段创建索引
        System.out.println("=== 示例1: 为所有字段创建 BloomFilter 和 MinMax 索引 ===");
        String sql1 = "CREATE TABLE test_db.users (" +
                     "  id INT NOT NULL," +
                     "  name STRING," +
                     "  age INT," +
                     "  city STRING" +
                     ") TBLPROPERTIES (" +
                     "  'file.index.all' = 'bloom-filter,min-max'" +
                     ")";
        
        System.out.println("SQL: " + sql1);
        parser.executeSQL(sql1);
        System.out.println();
        
        // 示例2: 为指定字段创建索引
        System.out.println("=== 示例2: 为特定字段创建索引 ===");
        String sql2 = "CREATE TABLE test_db.products (" +
                     "  product_id INT NOT NULL," +
                     "  product_name STRING," +
                     "  price DOUBLE," +
                     "  category STRING" +
                     ") TBLPROPERTIES (" +
                     "  'file.index.product_name.bloom-filter' = 'true'," +
                     "  'file.index.price.min-max' = 'true'," +
                     "  'file.index.category.bloom-filter' = 'true'" +
                     ")";
        
        System.out.println("SQL: " + sql2);
        parser.executeSQL(sql2);
        System.out.println();
        
        // 示例3: 混合配置
        System.out.println("=== 示例3: 只为部分字段创建 MinMax 索引 ===");
        String sql3 = "CREATE TABLE test_db.orders (" +
                     "  order_id INT NOT NULL," +
                     "  customer_id INT," +
                     "  order_date STRING," +
                     "  amount DOUBLE" +
                     ") TBLPROPERTIES (" +
                     "  'file.index.amount.min-max' = 'true'," +
                     "  'file.index.customer_id.bloom-filter' = 'true'," +
                     "  'file.index.customer_id.min-max' = 'true'" +
                     ")";
        
        System.out.println("SQL: " + sql3);
        parser.executeSQL(sql3);
        System.out.println();
        
        System.out.println("=== 验证表创建 ===");
        System.out.println("Tables in test_db: " + catalog.listTables("test_db"));
        
        System.out.println("\n========== 示例完成 ==========");
        System.out.println("\n提示：索引配置已保存，在后续数据写入时会自动使用配置的索引类型创建索引文件。");
        
        // 关闭 catalog
        catalog.close();
        
        // 清理
        deleteDirectory(new File(warehouse));
    }
    
    private static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
}
