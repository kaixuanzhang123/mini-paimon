package com.mini.paimon.sql;

import com.mini.paimon.catalog.*;
import com.mini.paimon.schema.Row;
import com.mini.paimon.table.*;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;

/**
 * SQL 示例类
 * 演示如何使用 SQL 解析器执行 CREATE TABLE、INSERT 和 SELECT 语句
 */
public class SQLSelectTest {
    
    public static void main(String[] args) {
        try {
            // 创建路径工厂
            PathFactory pathFactory = new PathFactory("./warehouse");
            
            // 创建 Catalog
            CatalogContext context = CatalogContext.builder().warehouse("./warehouse").build();
            Catalog catalog = new FileSystemCatalog("default", context);

            catalog.createDatabase("default", true);
            // 创建 SQL 解析器
            SQLParser sqlParser = new SQLParser(catalog, pathFactory);
            
            // 先创建表
            System.out.println("1. 创建表:");
            String createTableSQL = "CREATE TABLE users (id INT NOT NULL, name VARCHAR(50), age INT)";
            sqlParser.executeSQL(createTableSQL);
            
            // 插入数据但不立即关闭LSMTree
            System.out.println("\n2. 插入数据:");
            insertDataWithoutClosing(pathFactory, catalog);
            
            // 查询表
            System.out.println("\n3. 查询表:");
            String selectTableSQL = "SELECT * FROM users";
            sqlParser.executeSQL(selectTableSQL);
            
        } catch (IOException e) {
            System.err.println("执行 SQL 时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 插入数据但不立即关闭LSMTree，以避免多次刷写
     */
    private static void insertDataWithoutClosing(PathFactory pathFactory, Catalog catalog) throws IOException {
        Table table = catalog.getTable(new Identifier("default", "users"));
        
        try (TableWrite writer = table.newWrite()) {
            writer.write(new Row(new Object[]{1, "Alice", 25}));
            System.out.println("插入数据: (1, 'Alice', 25)");
            
            writer.write(new Row(new Object[]{2, "Bob", 30}));
            System.out.println("插入数据: (2, 'Bob', 30)");
            
            writer.write(new Row(new Object[]{3, "Charlie", 35}));
            System.out.println("插入数据: (3, 'Charlie', 35)");
        }
    }
}