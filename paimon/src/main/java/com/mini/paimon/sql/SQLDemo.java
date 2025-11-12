package com.mini.paimon.sql;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.utils.PathFactory;

/**
 * SQL 演示类
 */
public class SQLDemo {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== Mini Paimon SQL 演示 ===\n");
            
            PathFactory pathFactory = new PathFactory("./warehouse");
            
            CatalogContext context = CatalogContext.builder()
                .warehouse("./warehouse")
                .build();
            Catalog catalog = new FileSystemCatalog("mini_paimon", "default", context);
            
            SQLParser sqlParser = new SQLParser(catalog, pathFactory);
            
            System.out.println("1. 创建用户表:");
            String createUsersTableSQL = "CREATE TABLE users (id INT NOT NULL, name STRING NOT NULL, age INT, email STRING)";
            sqlParser.executeSQL(createUsersTableSQL);
            
            System.out.println("\n2. 创建订单表:");
            String createOrdersTableSQL = "CREATE TABLE orders (order_id INT NOT NULL, user_id INT NOT NULL, product STRING, amount DOUBLE)";
            sqlParser.executeSQL(createOrdersTableSQL);
            
            System.out.println("\n3. 插入用户数据:");
            sqlParser.executeSQL("INSERT INTO users VALUES (1, 'Alice', 25, 'alice@example.com')");
            sqlParser.executeSQL("INSERT INTO users VALUES (2, 'Bob', 30, 'bob@example.com')");
            sqlParser.executeSQL("INSERT INTO users VALUES (3, 'Charlie', 35, 'charlie@example.com')");
            
            System.out.println("\n4. 插入订单数据:");
            sqlParser.executeSQL("INSERT INTO orders VALUES (101, 1, 'Laptop', 1200.0)");
            sqlParser.executeSQL("INSERT INTO orders VALUES (102, 2, 'Phone', 800.0)");
            sqlParser.executeSQL("INSERT INTO orders VALUES (103, 1, 'Mouse', 25.0)");
            
            System.out.println("\n5. 插入部分列数据:");
            sqlParser.executeSQL("INSERT INTO users (id, name) VALUES (4, 'David')");
            
            catalog.close();
            System.out.println("\n=== SQL 演示完成 ===");
            System.out.println("已成功创建表并插入数据，数据已持久化到文件系统中。");
            
        } catch (Exception e) {
            System.err.println("执行 SQL 时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}