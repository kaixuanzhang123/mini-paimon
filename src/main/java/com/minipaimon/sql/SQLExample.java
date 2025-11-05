package com.minipaimon.sql;

import com.minipaimon.metadata.TableManager;
import com.minipaimon.utils.PathFactory;

import java.io.IOException;

/**
 * SQL 示例类
 * 演示如何使用 SQL 解析器执行 CREATE TABLE 和 INSERT 语句
 */
public class SQLExample {
    
    public static void main(String[] args) {
        try {
            // 创建路径工厂
            PathFactory pathFactory = new PathFactory("./warehouse");
            
            // 创建表管理器
            TableManager tableManager = new TableManager(pathFactory);
            
            // 创建 SQL 解析器
            SQLParser sqlParser = new SQLParser(tableManager, pathFactory);
            
            // 创建表（带主键）
            System.out.println("1. 创建表:");
            String createTableSQL = "CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name STRING, age INT)";
            sqlParser.executeSQL(createTableSQL);
            
            // 插入数据 - 方式1：指定所有列
            System.out.println("\n2. 插入数据 (方式1):");
            String insertSQL1 = "INSERT INTO users VALUES (1, 'Alice', 25)";
            sqlParser.executeSQL(insertSQL1);
            
            // 插入数据 - 方式2：指定列名
            System.out.println("\n3. 插入数据 (方式2):");
            String insertSQL2 = "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30)";
            sqlParser.executeSQL(insertSQL2);
            
            // 插入数据 - 方式3：部分列
            System.out.println("\n4. 插入数据 (方式3):");
            String insertSQL3 = "INSERT INTO users (id, name) VALUES (3, 'Charlie')";
            sqlParser.executeSQL(insertSQL3);
            
            System.out.println("\nSQL 示例执行完成!");
            
        } catch (IOException e) {
            System.err.println("执行 SQL 时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}