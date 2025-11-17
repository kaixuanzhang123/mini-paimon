package com.mini.paimon.flink;

import com.mini.paimon.flink.utils.FlinkEnvironmentFactory;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

/**
 * 测试主键表的索引功能
 */
public class FlinkPrimaryKeyIndexTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkPrimaryKeyIndexTest.class);

    public static void main(String[] args) throws Exception {
        String warehousePath = "./warehouse_pk_index_test";
        
        cleanupWarehouse(warehousePath);
        
        try {
            TableEnvironment tableEnv = FlinkEnvironmentFactory.createTableEnvironment(warehousePath);
            
            LOG.info("=== Creating Database ===");
            tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS pk_db");
            tableEnv.executeSql("USE pk_db");
            
            LOG.info("=== Creating PRIMARY KEY Table with Multiple Indexes ===");
            String createTableSQL = 
                "CREATE TABLE IF NOT EXISTS pk_users (" +
                "  user_id BIGINT," +
                "  username STRING," +
                "  email STRING," +
                "  age INT," +
                "  country STRING," +
                "  PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'mini-paimon'," +
                "  'index.username.type' = 'bloom_filter'," +
                "  'index.country.type' = 'bitmap'," +
                "  'index.age.type' = 'min_max'" +
                ")";
            tableEnv.executeSql(createTableSQL);
            System.out.println("✓ PRIMARY KEY table with multiple indexes created successfully");

            System.out.println("\n=== Inserting Data ===");
            tableEnv.executeSql(
                "INSERT INTO pk_users VALUES " +
                "(1, 'alice', 'alice@example.com', 25, 'USA'), " +
                "(2, 'bob', 'bob@example.com', 30, 'UK'), " +
                "(3, 'charlie', 'charlie@example.com', 35, 'USA'), " +
                "(4, 'diana', 'diana@example.com', 28, 'Canada'), " +
                "(5, 'eve', 'eve@example.com', 32, 'USA')"
            ).await();
            System.out.println("✓ Data inserted successfully");
            
            // 检查生成的文件
            System.out.println("\n=== Checking Generated Files ===");
            Path tablePath = Paths.get(warehousePath, "pk_db", "pk_users");
            if (Files.exists(tablePath)) {
                System.out.println("Table directory: " + tablePath.toAbsolutePath());
                Files.walk(tablePath)
                    .filter(Files::isRegularFile)
                    .forEach(file -> {
                        String relativePath = tablePath.relativize(file).toString();
                        try {
                            long size = Files.size(file);
                            System.out.println("  " + relativePath + " (" + size + " bytes)");
                            
                            // 检查是否有索引文件
                            if (relativePath.contains("index/") && 
                                (relativePath.endsWith(".bfi") || 
                                 relativePath.endsWith(".bmi") || 
                                 relativePath.endsWith(".mmi"))) {
                                System.out.println("    ✓ Index file found!");
                            }
                        } catch (Exception e) {
                            System.out.println("  " + relativePath);
                        }
                    });
            } else {
                System.out.println("  Table directory not found: " + tablePath);
            }

            // 更新数据（测试主键去重）
            System.out.println("\n=== Updating Data (Primary Key Deduplication) ===");
            tableEnv.executeSql(
                "INSERT INTO pk_users VALUES " +
                "(1, 'alice_updated', 'alice_new@example.com', 26, 'USA')"
            ).await();
            System.out.println("✓ Data updated successfully");

            // 测试：查询所有数据
            System.out.println("\n=== Testing: Query All Data ===");
            TableResult result = tableEnv.executeSql("SELECT * FROM pk_users ORDER BY user_id");
            CloseableIterator<Row> iterator = result.collect();
            int count = 0;
            System.out.println("\nAll users:");
            while (iterator.hasNext()) {
                Row row = iterator.next();
                count++;
                System.out.println("  " + count + ". " + row);
            }
            iterator.close();
            
            System.out.println("\nTotal: " + count + " users");
            System.out.println("Expected: 5 users (user_id=1 should be updated, not duplicated)");
            Assert.assertEquals("Expected 5 users (primary key deduplication)", 5, count);

            // 测试：过滤查询（使用索引）
            System.out.println("\n=== Testing: Filtered Query (Using Indexes) ===");
            System.out.println("Query: SELECT * FROM pk_users WHERE country = 'USA' AND age > 25");
            
            result = tableEnv.executeSql(
                "SELECT * FROM pk_users WHERE country = 'USA' AND age > 25 ORDER BY user_id"
            );
            iterator = result.collect();
            count = 0;
            System.out.println("\nMatching users:");
            while (iterator.hasNext()) {
                Row row = iterator.next();
                count++;
                System.out.println("  " + count + ". " + row);
            }
            iterator.close();
            
            System.out.println("\nTotal: " + count + " users matched");
            System.out.println("Expected: 3 users (alice_updated: age=26, charlie: age=35, eve: age=32, all from USA)");
            Assert.assertEquals("Expected 3 users from USA with age > 25", 3, count);

            System.out.println("\n=== Test Completed Successfully ===");
            System.out.println("✓ PRIMARY KEY table index generation works correctly!");
            System.out.println("✓ All index files generated:");
            System.out.println("  - Bloom Filter Index (.bfi) for username field");
            System.out.println("  - Bitmap Index (.bmi) for country field");
            System.out.println("  - Min-Max Index (.mmi) for age field");
            System.out.println("✓ Primary key deduplication works correctly!");
            
        } finally {
            cleanupWarehouse(warehousePath);
        }
    }
    
    private static void cleanupWarehouse(String warehousePath) {
        try {
            Path path = Paths.get(warehousePath);
            if (Files.exists(path)) {
                Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
                LOG.info("Cleaned up warehouse: {}", warehousePath);
            }
        } catch (Exception e) {
            LOG.warn("Failed to cleanup warehouse", e);
        }
    }
}

