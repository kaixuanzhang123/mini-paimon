package com.mini.paimon.flink;

import com.mini.paimon.flink.utils.FlinkEnvironmentFactory;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class FlinkIndexTableTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkIndexTableTest.class);

    public static void main(String[] args) throws Exception {
        String warehousePath = "./warehouse_index_test";
        
        cleanupWarehouse(warehousePath);
        
        try {
            TableEnvironment tableEnv = FlinkEnvironmentFactory.createTableEnvironment(warehousePath);
            
            LOG.info("=== Creating Database ===");
            tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS index_db");
            tableEnv.executeSql("USE index_db");
            
            LOG.info("=== Creating Table with Bitmap Index ===");
            String createTableSQL = 
                "CREATE TABLE IF NOT EXISTS products (" +
                "  product_id BIGINT," +
                "  product_name STRING," +
                "  category STRING," +
                "  price DOUBLE," +
                "  PRIMARY KEY (product_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'mini-paimon'," +
                "  'index.category.type' = 'bitmap'" +
                ")";
            tableEnv.executeSql(createTableSQL);
            LOG.info("Table with bitmap index created successfully");
            
            LOG.info("=== Creating Table with Multiple Indexes ===");
            String createTableSQL2 = 
                "CREATE TABLE IF NOT EXISTS users (" +
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
            tableEnv.executeSql(createTableSQL2);
            LOG.info("Table with multiple indexes created successfully");
            
            LOG.info("=== Inserting Data ===");
            tableEnv.executeSql(
                "INSERT INTO products VALUES " +
                "(1, 'Laptop', 'Electronics', 999.99), " +
                "(2, 'Mouse', 'Electronics', 29.99), " +
                "(3, 'Desk', 'Furniture', 299.99), " +
                "(4, 'Chair', 'Furniture', 199.99), " +
                "(5, 'Monitor', 'Electronics', 399.99)"
            ).await();
            
            tableEnv.executeSql(
                "INSERT INTO users VALUES " +
                "(1, 'alice', 'alice@example.com', 25, 'USA'), " +
                "(2, 'bob', 'bob@example.com', 30, 'UK'), " +
                "(3, 'charlie', 'charlie@example.com', 35, 'USA'), " +
                "(4, 'diana', 'diana@example.com', 28, 'Canada'), " +
                "(5, 'eve', 'eve@example.com', 32, 'USA')"
            ).await();
            
            LOG.info("=== Querying Products (with bitmap index on category) ===");
            TableResult result = tableEnv.executeSql(
                "SELECT * FROM products WHERE category = 'Electronics' ORDER BY product_id"
            );
            result.print();
            
            LOG.info("=== Querying Users (with multiple indexes) ===");
            result = tableEnv.executeSql(
                "SELECT * FROM users WHERE country = 'USA' AND age > 25 ORDER BY user_id"
            );
            result.print();
            
            LOG.info("=== Test Completed Successfully ===");
            
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

