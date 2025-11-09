package com.mini.paimon.sql;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;

/**
 * SQL 示例类
 */
public class SQLExample {
    
    public static void main(String[] args) {
        try {
            PathFactory pathFactory = new PathFactory("./warehouse");
            
            CatalogContext context = CatalogContext.builder()
                .warehouse("./warehouse")
                .build();
            Catalog catalog = new FileSystemCatalog("mini_paimon", "default", context);
            
            SQLParser sqlParser = new SQLParser(catalog, pathFactory);
            
            System.out.println("1. 创建表:");
            String createTableSQL = "CREATE TABLE users (id INT NOT NULL, name STRING, age INT)";
            sqlParser.executeSQL(createTableSQL);
            
            System.out.println("\n2. 插入数据 (方式1):");
            String insertSQL1 = "INSERT INTO users VALUES (1, 'Alice', 25)";
            sqlParser.executeSQL(insertSQL1);
            
            System.out.println("\n3. 插入数据 (方式2):");
            String insertSQL2 = "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30)";
            sqlParser.executeSQL(insertSQL2);
            
            System.out.println("\n4. 插入数据 (方式3):");
            String insertSQL3 = "INSERT INTO users (id, name) VALUES (3, 'Charlie')";
            sqlParser.executeSQL(insertSQL3);
            
            catalog.close();
            System.out.println("\nSQL 示例执行完成!");
            
        } catch (Exception e) {
            System.err.println("执行 SQL 时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}