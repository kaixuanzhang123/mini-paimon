package com.mini.paimon.metadata;

import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

/**
 * Schema 管理示例程序
 * 演示如何使用 Schema 管理器管理表结构
 */
public class SchemaManagementExample {
    private static final Logger logger = LoggerFactory.getLogger(SchemaManagementExample.class);

    public static void main(String[] args) {
        logger.info("=== Schema Management Example ===");
        
        try {
            // 1. 创建路径工厂
            logger.info("\n1. Creating PathFactory...");
            PathFactory pathFactory = new PathFactory("./warehouse");
            
            // 2. 创建表管理器
            logger.info("\n2. Creating TableManager...");
            TableManager tableManager = new TableManager(pathFactory);
            
            // 3. 定义字段
            logger.info("\n3. Defining fields...");
            Field idField = new Field("id", DataType.INT, false);
            Field nameField = new Field("name", DataType.STRING, true);
            Field ageField = new Field("age", DataType.INT, true);
            Field emailField = new Field("email", DataType.STRING, true);
            
            // 4. 创建表
            logger.info("\n4. Creating table...");
            TableMetadata tableMetadata = tableManager.createTable(
                "example_db",
                "user_table",
                Arrays.asList(idField, nameField, ageField),
                Collections.singletonList("id"),
                Collections.emptyList()
            );
            
            logger.info("Created table: {}", tableMetadata);
            
            // 5. 获取 Schema 管理器
            logger.info("\n5. Getting SchemaManager...");
            SchemaManager schemaManager = tableManager.getSchemaManager("example_db", "user_table");
            
            // 6. 获取当前 Schema
            logger.info("\n6. Getting current schema...");
            Schema currentSchema = schemaManager.getCurrentSchema();
            logger.info("Current schema: {}", currentSchema);
            
            // 7. 演示 Schema 演化 - 添加新字段
            logger.info("\n7. Evolving schema - adding email field...");
            Schema newSchema = schemaManager.createNewSchemaVersion(
                Arrays.asList(idField, nameField, ageField, emailField),
                Collections.singletonList("id"),
                Collections.emptyList()
            );
            
            logger.info("New schema version created: {}", newSchema);
            
            // 8. 验证 Schema 版本数量
            logger.info("\n8. Checking schema version count...");
            int versionCount = schemaManager.getSchemaVersionCount();
            logger.info("Schema version count: {}", versionCount);
            
            // 9. 加载特定版本的 Schema
            logger.info("\n9. Loading specific schema version...");
            Schema loadedSchemaV0 = schemaManager.loadSchema(0);
            Schema loadedSchemaV1 = schemaManager.loadSchema(1);
            
            logger.info("Schema V0: {}", loadedSchemaV0);
            logger.info("Schema V1: {}", loadedSchemaV1);
            
            // 10. 加载最新 Schema
            logger.info("\n10. Loading latest schema...");
            Schema latestSchema = schemaManager.loadLatestSchema();
            logger.info("Latest schema: {}", latestSchema);
            
            // 11. 验证表元数据
            logger.info("\n11. Verifying table metadata...");
            TableMetadata loadedMetadata = tableManager.getTableMetadata("example_db", "user_table");
            logger.info("Loaded table metadata: {}", loadedMetadata);
            
            // 12. 列出数据库中的表
            logger.info("\n12. Listing tables...");
            java.util.List<String> tables = tableManager.listTables("example_db");
            logger.info("Tables in example_db: {}", tables);
            
            // 13. 查看持久化的文件
            logger.info("\n13. Checking persisted files...");
            logger.info("Schema directory: {}", pathFactory.getSchemaDir("example_db", "user_table"));
            logger.info("Schema V0 file: {}", pathFactory.getSchemaPath("example_db", "user_table", 0));
            logger.info("Schema V1 file: {}", pathFactory.getSchemaPath("example_db", "user_table", 1));
            logger.info("Table metadata file: {}", pathFactory.getTablePath("example_db", "user_table").resolve("metadata"));
            
            logger.info("\n=== Schema Management Example Completed Successfully ===");
            
        } catch (Exception e) {
            logger.error("Error in Schema Management example", e);
        }
    }
}
