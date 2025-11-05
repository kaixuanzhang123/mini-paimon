package com.minipaimon.storage;

import com.minipaimon.metadata.DataType;
import com.minipaimon.metadata.Field;
import com.minipaimon.metadata.Row;
import com.minipaimon.metadata.RowKey;
import com.minipaimon.metadata.Schema;
import com.minipaimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

/**
 * LSM Tree 示例程序
 * 演示如何使用 LSM Tree 存储和检索数据
 */
public class LSMTreeExample {
    private static final Logger logger = LoggerFactory.getLogger(LSMTreeExample.class);

    public static void main(String[] args) {
        logger.info("=== LSM Tree Example ===");
        
        try {
            // 1. 创建表结构
            logger.info("\n1. Creating table schema...");
            Field idField = new Field("id", DataType.INT, false);
            Field nameField = new Field("name", DataType.STRING, true);
            Field ageField = new Field("age", DataType.INT, true);
            
            Schema schema = new Schema(
                0,
                Arrays.asList(idField, nameField, ageField),
                Collections.singletonList("id")
            );
            
            logger.info("Schema created: {} fields", schema.getFields().size());
            
            // 2. 创建路径工厂和 LSM Tree
            logger.info("\n2. Creating LSM Tree...");
            PathFactory pathFactory = new PathFactory("./warehouse");
            pathFactory.createTableDirectories("example_db", "user_table");
            
            LSMTree lsmTree = new LSMTree(schema, pathFactory, "example_db", "user_table");
            logger.info("LSM Tree created: {}", lsmTree.getStatus());
            
            // 3. 插入数据
            logger.info("\n3. Inserting data...");
            Row[] rows = {
                new Row(new Object[]{1, "Alice", 25}),
                new Row(new Object[]{2, "Bob", 30}),
                new Row(new Object[]{3, "Charlie", 35}),
                new Row(new Object[]{4, "David", 28}),
                new Row(new Object[]{5, "Eve", 32})
            };
            
            for (Row row : rows) {
                lsmTree.put(row);
                logger.info("Inserted: {}", row);
            }
            
            logger.info("Data insertion completed. Current status: {}", lsmTree.getStatus());
            
            // 4. 查询数据
            logger.info("\n4. Querying data...");
            for (Row row : rows) {
                RowKey key = RowKey.fromRow(row, schema);
                Row result = lsmTree.get(key);
                
                if (result != null) {
                    logger.info("Found: {} -> {}", key, result);
                } else {
                    logger.warn("Not found: {}", key);
                }
            }
            
            // 5. 查询不存在的数据
            logger.info("\n5. Querying non-existent data...");
            Row nonExistentRow = new Row(new Object[]{999, "NonExistent", 0});
            RowKey nonExistentKey = RowKey.fromRow(nonExistentRow, schema);
            Row result = lsmTree.get(nonExistentKey);
            
            if (result == null) {
                logger.info("Correctly returned null for non-existent key: {}", nonExistentKey);
            } else {
                logger.warn("Unexpected result for non-existent key: {}", result);
            }
            
            // 6. 关闭 LSM Tree
            logger.info("\n6. Closing LSM Tree...");
            lsmTree.close();
            logger.info("LSM Tree closed successfully");
            
            logger.info("\n=== LSM Tree Example Completed Successfully ===");
            
        } catch (Exception e) {
            logger.error("Error in LSM Tree example", e);
        }
    }
}
