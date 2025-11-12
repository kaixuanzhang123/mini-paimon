package com.mini.paimon.sql;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

/**
 * SQL 数据库支持示例
 * 演示如何使用 database.table 格式创建和操作表
 */
public class SQLDatabaseExample {
    private static final Logger logger = LoggerFactory.getLogger(SQLDatabaseExample.class);
    private static final String WAREHOUSE_PATH = "/tmp/mini-paimon-db-example";
    
    public static void main(String[] args) throws Exception {
        // 清理旧数据
        cleanupWarehouse();
        
        // 初始化 Catalog
        CatalogContext context = CatalogContext.builder()
            .warehouse(WAREHOUSE_PATH)
            .build();
        
        try (Catalog catalog = new FileSystemCatalog("example_catalog", context)) {
            PathFactory pathFactory = new PathFactory(WAREHOUSE_PATH);
            SQLParser sqlParser = new SQLParser(catalog, pathFactory);
            
            System.out.println("=== MiniPaimon SQL 数据库支持示例 ===\n");
            
            // 示例 1: 创建数据库
            System.out.println("1. 创建数据库");
            catalog.createDatabase("test", false);
            catalog.createDatabase("production", false);
            System.out.println("   ✓ 创建了数据库: test, production\n");
            
            // 示例 2: 使用完整表名创建表（database.table格式）
            System.out.println("2. 使用完整表名创建表");
            String createSQL1 = "CREATE TABLE test.users (" +
                              "id INT NOT NULL, " +
                              "name VARCHAR(50), " +
                              "age INT)";
            sqlParser.executeSQL(createSQL1);
            
            // 验证 Schema ID 自动生成
            Schema schema = catalog.getTableSchema(new Identifier("test", "users"));
            System.out.println("   ✓ 表 test.users 创建成功");
            System.out.println("   ✓ Schema ID 自动生成: " + schema.getSchemaId() + "\n");
            
            // 示例 3: 创建另一个数据库的表
            String createSQL2 = "CREATE TABLE production.orders (" +
                              "order_id INT NOT NULL, " +
                              "product VARCHAR(100), " +
                              "quantity INT)";
            sqlParser.executeSQL(createSQL2);
            System.out.println("   ✓ 表 production.orders 创建成功\n");
            
            // 示例 4: 插入数据（使用完整表名）
            System.out.println("3. 插入数据");
            String insertSQL1 = "INSERT INTO test.users VALUES (1, 'Alice', 25)";
            String insertSQL2 = "INSERT INTO test.users VALUES (2, 'Bob', 30)";
            String insertSQL3 = "INSERT INTO production.orders VALUES (100, 'Laptop', 5)";
            
            sqlParser.executeSQL(insertSQL1);
            sqlParser.executeSQL(insertSQL2);
            sqlParser.executeSQL(insertSQL3);
            System.out.println();
            
            // 示例 5: 查询数据
            System.out.println("4. 查询数据");
            System.out.println("   查询 test.users:");
            String selectSQL1 = "SELECT * FROM test.users";
            sqlParser.executeSQL(selectSQL1);
            System.out.println();
            
            System.out.println("   查询 production.orders:");
            String selectSQL2 = "SELECT * FROM production.orders";
            sqlParser.executeSQL(selectSQL2);
            System.out.println();
            
            // 示例 6: 列出所有数据库和表
            System.out.println("5. 列出所有数据库和表");
            for (String database : catalog.listDatabases()) {
                System.out.println("   数据库: " + database);
                for (String table : catalog.listTables(database)) {
                    Schema tableSchema = catalog.getTableSchema(new Identifier(database, table));
                    System.out.println("      └─ " + table + " (Schema ID: " + tableSchema.getSchemaId() + ")");
                }
            }
            System.out.println();
            
            // 示例 7: 使用默认数据库创建表
            System.out.println("6. 使用默认数据库创建表");
            catalog.createDatabase("default", true);
            String createSQL3 = "CREATE TABLE products (id INT NOT NULL, name VARCHAR(50))";
            sqlParser.executeSQL(createSQL3);
            
            Schema defaultSchema = catalog.getTableSchema(new Identifier("default", "products"));
            System.out.println("   ✓ 表 default.products 创建成功");
            System.out.println("   ✓ Schema ID: " + defaultSchema.getSchemaId() + "\n");
            
            // 示例 8: Schema 版本演化
            System.out.println("7. Schema 版本演化");
            Identifier identifier = new Identifier("test", "users");
            Schema currentSchema = catalog.getTableSchema(identifier);
            System.out.println("   当前 Schema 版本: " + currentSchema.getSchemaId());
            
            // 添加新字段（演示 Schema 演化）
            java.util.List<com.mini.paimon.metadata.Field> newFields = new java.util.ArrayList<>(currentSchema.getFields());
            newFields.add(new com.mini.paimon.metadata.Field("email", com.mini.paimon.metadata.DataType.STRING, true));
            
            Schema newSchema = catalog.alterTable(identifier, newFields, 
                currentSchema.getPrimaryKeys(), currentSchema.getPartitionKeys());
            
            System.out.println("   新 Schema 版本: " + newSchema.getSchemaId());
            System.out.println("   ✓ 自动生成新的 Schema ID\n");
            
            // 示例 9: 删除表
            System.out.println("8. 删除表");
            String dropSQL = "DROP TABLE production.orders";
            sqlParser.executeSQL(dropSQL);
            System.out.println();
            
            System.out.println("=== 示例执行完成 ===");
            
        } finally {
            // 清理测试数据
            cleanupWarehouse();
        }
    }
    
    private static void cleanupWarehouse() throws IOException {
        Path warehousePath = Paths.get(WAREHOUSE_PATH);
        if (Files.exists(warehousePath)) {
            Files.walk(warehousePath)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        logger.warn("Failed to delete: {}", path, e);
                    }
                });
        }
    }
}
