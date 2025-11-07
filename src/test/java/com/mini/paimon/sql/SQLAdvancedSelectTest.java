package com.mini.paimon.sql;

import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.metadata.TableManager;
import com.mini.paimon.storage.LSMTree;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;

/**
 * SQL 高级查询测试
 * 测试字段投影和 WHERE 条件过滤
 */
public class SQLAdvancedSelectTest {
    
    public static void main(String[] args) {
        try {
            // 创建路径工厂
            PathFactory pathFactory = new PathFactory("./warehouse");
            
            // 创建表管理器
            TableManager tableManager = new TableManager(pathFactory);
            
            // 创建 SQL 解析器
            SQLParser sqlParser = new SQLParser(tableManager, pathFactory);
            
            // 先创建表
            System.out.println("1. 创建表:");
            String createTableSQL = "CREATE TABLE users (id INT , name VARCHAR(50), age INT)";
            sqlParser.executeSQL(createTableSQL);
            
            // 插入数据
            System.out.println("\n2. 插入数据:");
            insertDataWithoutClosing(pathFactory, tableManager);
            
            // 测试 1: SELECT * 查询（全字段）
            System.out.println("\n3. SELECT * FROM users:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM users");
            
            // 测试 2: 字段投影查询
            System.out.println("\n4. SELECT name, age FROM users:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT name, age FROM users");
            
            // 测试 3: 单字段查询
            System.out.println("\n5. SELECT name FROM users:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT name FROM users");
            
            // 测试 4: WHERE 条件过滤（等于）
            System.out.println("\n6. SELECT * FROM users WHERE age = 30:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM users WHERE age = 30");
            
            // 测试 5: WHERE 条件过滤（大于）
            System.out.println("\n7. SELECT * FROM users WHERE age > 25:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM users WHERE age > 25");
            
            // 测试 6: WHERE 条件过滤（小于等于）
            System.out.println("\n8. SELECT * FROM users WHERE age <= 30:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM users WHERE age <= 30");
            
            // 测试 7: 字段投影 + WHERE 过滤
            System.out.println("\n9. SELECT name, age FROM users WHERE age > 25:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT name, age FROM users WHERE age > 25");
            
            // 测试 8: 字符串条件过滤
            System.out.println("\n10. SELECT * FROM users WHERE name = 'Bob':");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM users WHERE name = 'Bob'");
            
        } catch (IOException e) {
            System.err.println("执行 SQL 时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 插入数据但不立即关闭LSMTree，以避免多次刷写
     */
    private static void insertDataWithoutClosing(PathFactory pathFactory, TableManager tableManager) throws IOException {
        // 获取表的Schema
        Schema schema = tableManager.getSchemaManager("default", "users").getCurrentSchema();
        
        // 创建LSMTree实例
        LSMTree lsmTree = new LSMTree(schema, pathFactory, "default", "users");
        
        try {
            // 插入数据
            lsmTree.put(new Row(new Object[]{1, "Alice", 25}));
            System.out.println("插入数据: (1, 'Alice', 25)");
            
            lsmTree.put(new Row(new Object[]{2, "Bob", 30}));
            System.out.println("插入数据: (2, 'Bob', 30)");
            
            lsmTree.put(new Row(new Object[]{3, "Charlie", 35}));
            System.out.println("插入数据: (3, 'Charlie', 35)");
            
            lsmTree.put(new Row(new Object[]{4, "David", 28}));
            System.out.println("插入数据: (4, 'David', 28)");
            
            lsmTree.put(new Row(new Object[]{5, "Eve", 22}));
            System.out.println("插入数据: (5, 'Eve', 22)");
        } finally {
            // 最后关闭LSMTree，触发一次刷写
            lsmTree.close();
        }
    }
}
