package com.mini.paimon.catalog;

import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.metadata.TableMetadata;
import com.mini.paimon.snapshot.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Catalog 使用示例
 * 演示如何使用 Catalog 接口管理数据库、表和快照
 */
public class CatalogExample {
    private static final Logger logger = LoggerFactory.getLogger(CatalogExample.class);
    
    public static void main(String[] args) {
        try {
            // 1. 创建 Catalog
            logger.info("=== Step 1: Create Catalog ===");
            CatalogContext context = CatalogContext.builder()
                .warehouse("./warehouse")
                .option("catalog.type", "filesystem")
                .build();
            
            Catalog catalog = new FileSystemCatalog("mini_paimon_catalog", "default", context);
            logger.info("Created catalog: {}", catalog.name());
            
            // 2. 创建数据库
            logger.info("\n=== Step 2: Create Database ===");
            catalog.createDatabase("test_db", false);
            logger.info("Created database: test_db");
            
            // 列出所有数据库
            List<String> databases = catalog.listDatabases();
            logger.info("All databases: {}", databases);
            
            // 3. 创建表
            logger.info("\n=== Step 3: Create Table ===");
            Identifier userTable = new Identifier("test_db", "users");
            
            List<Field> fields = Arrays.asList(
                new Field("user_id", DataType.INT(), false),
                new Field("username", DataType.STRING(), false),
                new Field("email", DataType.STRING(), true),
                new Field("age", DataType.INT(), true)
            );
            
            Schema schema = new Schema(
                0,  // 初始版本由 SchemaManager 自动分配
                fields,
                Collections.singletonList("user_id"),  // 主键
                Collections.emptyList()  // 分区键
            );
            
            catalog.createTable(userTable, schema, false);
            logger.info("Created table: {}", userTable);
            
            // 4. 列出表
            logger.info("\n=== Step 4: List Tables ===");
            List<String> tables = catalog.listTables("test_db");
            logger.info("Tables in test_db: {}", tables);
            
            // 5. 获取表元数据
            logger.info("\n=== Step 5: Get Table Metadata ===");
            TableMetadata metadata = catalog.getTableMetadata(userTable);
            logger.info("Table metadata: {}", metadata);
            
            // 6. 获取表 Schema
            logger.info("\n=== Step 6: Get Table Schema ===");
            Schema currentSchema = catalog.getTableSchema(userTable);
            logger.info("Current schema: {}", currentSchema);
            logger.info("Schema ID: {}", currentSchema.getSchemaId());
            logger.info("Fields: {}", currentSchema.getFields());
            logger.info("Primary keys: {}", currentSchema.getPrimaryKeys());
            
            // 7. Schema 演化 - 添加新字段
            logger.info("\n=== Step 7: Alter Table (Schema Evolution) ===");
            List<Field> newFields = Arrays.asList(
                new Field("user_id", DataType.INT(), false),
                new Field("username", DataType.STRING(), false),
                new Field("email", DataType.STRING(), true),
                new Field("age", DataType.INT(), true),
                new Field("create_time", DataType.LONG(), true)  // 新增字段
            );
            
            Schema newSchema = catalog.alterTable(
                userTable, 
                newFields,
                Collections.singletonList("user_id"),
                Collections.emptyList()
            );
            logger.info("New schema version: {}", newSchema.getSchemaId());
            logger.info("New fields: {}", newSchema.getFields());
            
            // 8. 查询快照
            logger.info("\n=== Step 8: Query Snapshots ===");
            Snapshot latestSnapshot = catalog.getLatestSnapshot(userTable);
            if (latestSnapshot != null) {
                logger.info("Latest snapshot: {}", latestSnapshot);
            } else {
                logger.info("No snapshots yet (expected for new table)");
            }
            
            List<Snapshot> snapshots = catalog.listSnapshots(userTable);
            logger.info("Total snapshots: {}", snapshots.size());
            
            // 9. 创建另一个表
            logger.info("\n=== Step 9: Create Another Table ===");
            Identifier orderTable = new Identifier("test_db", "orders");
            
            List<Field> orderFields = Arrays.asList(
                new Field("order_id", DataType.STRING(), false),
                new Field("user_id", DataType.INT(), false),
                new Field("amount", DataType.DOUBLE(), false),
                new Field("status", DataType.STRING(), false)
            );
            
            Schema orderSchema = new Schema(
                0,
                orderFields,
                Collections.singletonList("order_id"),
                Collections.emptyList()
            );
            
            catalog.createTable(orderTable, orderSchema, false);
            logger.info("Created table: {}", orderTable);
            
            // 10. 重命名表
            logger.info("\n=== Step 10: Rename Table ===");
            Identifier newOrderTable = new Identifier("test_db", "order_info");
            catalog.renameTable(orderTable, newOrderTable);
            logger.info("Renamed table from {} to {}", orderTable, newOrderTable);
            
            // 验证重命名
            boolean oldExists = catalog.tableExists(orderTable);
            boolean newExists = catalog.tableExists(newOrderTable);
            logger.info("Old table exists: {}, New table exists: {}", oldExists, newExists);
            
            // 11. 删除表
            logger.info("\n=== Step 11: Drop Table ===");
            catalog.dropTable(newOrderTable, false);
            logger.info("Dropped table: {}", newOrderTable);
            
            // 12. 列出剩余的表
            logger.info("\n=== Step 12: List Remaining Tables ===");
            tables = catalog.listTables("test_db");
            logger.info("Remaining tables in test_db: {}", tables);
            
            // 13. 删除数据库（级联删除）
            logger.info("\n=== Step 13: Drop Database (Cascade) ===");
            catalog.dropDatabase("test_db", false, true);
            logger.info("Dropped database: test_db (cascade)");
            
            // 14. 验证删除
            logger.info("\n=== Step 14: Verify Deletion ===");
            boolean dbExists = catalog.databaseExists("test_db");
            logger.info("Database test_db exists: {}", dbExists);
            
            // 15. 关闭 Catalog
            logger.info("\n=== Step 15: Close Catalog ===");
            catalog.close();
            logger.info("Catalog closed");
            
            logger.info("\n=== Catalog Example Completed Successfully ===");
            
        } catch (Exception e) {
            logger.error("Error in catalog example", e);
        }
    }
}
