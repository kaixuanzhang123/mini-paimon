package com.minipaimon;

import com.minipaimon.metadata.*;
import com.minipaimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

/**
 * Mini Paimon 示例程序
 * 演示基础数据结构的使用
 */
public class MiniPaimonExample {
    private static final Logger logger = LoggerFactory.getLogger(MiniPaimonExample.class);

    public static void main(String[] args) {
        logger.info("=== Mini Paimon Example ===");

        // 1. 创建 Schema
        logger.info("\n1. Creating table schema...");
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, true);
        Field ageField = new Field("age", DataType.INT, true);
        Field isActiveField = new Field("is_active", DataType.BOOLEAN, false);

        Schema schema = new Schema(
                0,
                Arrays.asList(idField, nameField, ageField, isActiveField),
                Collections.singletonList("id")
        );

        logger.info("Schema created: {}", schema);
        logger.info("Primary keys: {}", schema.getPrimaryKeys());
        logger.info("Field count: {}", schema.getFields().size());

        // 2. 创建数据行
        logger.info("\n2. Creating rows...");
        Row row1 = new Row(new Object[]{1, "Alice", 25, true});
        Row row2 = new Row(new Object[]{2, "Bob", 30, true});
        Row row3 = new Row(new Object[]{3, "Charlie", null, false});

        logger.info("Row 1: {}", row1);
        logger.info("Row 2: {}", row2);
        logger.info("Row 3: {}", row3);

        // 3. 验证数据行
        logger.info("\n3. Validating rows...");
        try {
            row1.validate(schema);
            logger.info("Row 1 validation: PASSED");
            
            row2.validate(schema);
            logger.info("Row 2 validation: PASSED");
            
            row3.validate(schema);
            logger.info("Row 3 validation: PASSED");
        } catch (IllegalArgumentException e) {
            logger.error("Row validation failed: {}", e.getMessage());
        }

        // 4. 生成主键
        logger.info("\n4. Generating row keys...");
        RowKey key1 = RowKey.fromRow(row1, schema);
        RowKey key2 = RowKey.fromRow(row2, schema);
        RowKey key3 = RowKey.fromRow(row3, schema);

        logger.info("Row 1 key size: {} bytes", key1.size());
        logger.info("Row 2 key size: {} bytes", key2.size());
        logger.info("Row 3 key size: {} bytes", key3.size());

        // 5. 比较主键
        logger.info("\n5. Comparing row keys...");
        logger.info("key1 < key2: {}", key1.compareTo(key2) < 0);
        logger.info("key2 < key3: {}", key2.compareTo(key3) < 0);
        logger.info("key1 == key1: {}", key1.compareTo(key1) == 0);

        // 6. 演示路径工厂
        logger.info("\n6. Demonstrating path factory...");
        PathFactory pathFactory = new PathFactory("./warehouse");
        
        String database = "test_db";
        String table = "user_table";
        
        logger.info("Database path: {}", pathFactory.getDatabasePath(database));
        logger.info("Table path: {}", pathFactory.getTablePath(database, table));
        logger.info("Schema dir: {}", pathFactory.getSchemaDir(database, table));
        logger.info("Snapshot dir: {}", pathFactory.getSnapshotDir(database, table));
        logger.info("Manifest dir: {}", pathFactory.getManifestDir(database, table));
        logger.info("Data dir: {}", pathFactory.getDataDir(database, table));
        logger.info("Schema file (v0): {}", pathFactory.getSchemaPath(database, table, 0));
        logger.info("SSTable file (L0-001): {}", pathFactory.getSSTPath(database, table, 0, 1));

        // 7. 测试无效数据
        logger.info("\n7. Testing invalid data...");
        Row invalidRow = new Row(new Object[]{null, "Invalid", 20, true});
        try {
            invalidRow.validate(schema);
            logger.warn("Invalid row should not pass validation!");
        } catch (IllegalArgumentException e) {
            logger.info("Invalid row caught as expected: {}", e.getMessage());
        }

        logger.info("\n=== Example completed successfully ===");
    }
}
