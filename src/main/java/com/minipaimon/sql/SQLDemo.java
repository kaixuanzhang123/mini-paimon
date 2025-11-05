package com.minipaimon.sql;

import com.minipaimon.metadata.TableManager;
import com.minipaimon.utils.PathFactory;

import java.io.IOException;

/**
 * SQL 演示类
 * 展示完整的 SQL 读写数据流程
 */
public class SQLDemo {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== Mini Paimon SQL 演示 ===\n");
            
            // 创建路径工厂
            PathFactory pathFactory = new PathFactory("./warehouse");
            
            // 创建表管理器
            TableManager tableManager = new TableManager(pathFactory);
            
            // 创建 SQL 解析器
            SQLParser sqlParser = new SQLParser(tableManager, pathFactory);
            
            // 1. 创建用户表
            System.out.println("1. 创建用户表:");
            String createUsersTableSQL = "CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name STRING NOT NULL, age INT, email STRING)";
            sqlParser.executeSQL(createUsersTableSQL);
            
            // 2. 创建订单表
            System.out.println("\n2. 创建订单表:");
            String createOrdersTableSQL = "CREATE TABLE orders (order_id INT NOT NULL PRIMARY KEY, user_id INT NOT NULL, product STRING, amount DOUBLE)";
            sqlParser.executeSQL(createOrdersTableSQL);
            
            // 3. 插入用户数据
            System.out.println("\n3. 插入用户数据:");
            sqlParser.executeSQL("INSERT INTO users VALUES (1, 'Alice', 25, 'alice@example.com')");
            sqlParser.executeSQL("INSERT INTO users VALUES (2, 'Bob', 30, 'bob@example.com')");
            sqlParser.executeSQL("INSERT INTO users VALUES (3, 'Charlie', 35, 'charlie@example.com')");
            
            // 4. 插入订单数据
            System.out.println("\n4. 插入订单数据:");
            sqlParser.executeSQL("INSERT INTO orders VALUES (101, 1, 'Laptop', 1200.0)");
            sqlParser.executeSQL("INSERT INTO orders VALUES (102, 2, 'Phone', 800.0)");
            sqlParser.executeSQL("INSERT INTO orders VALUES (103, 1, 'Mouse', 25.0)");
            
            // 5. 插入部分列数据
            System.out.println("\n5. 插入部分列数据:");
            sqlParser.executeSQL("INSERT INTO users (id, name) VALUES (4, 'David')");
            
            System.out.println("\n=== SQL 演示完成 ===");
            System.out.println("已成功创建表并插入数据，数据已持久化到文件系统中。");
            
        } catch (IOException e) {
            System.err.println("执行 SQL 时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}