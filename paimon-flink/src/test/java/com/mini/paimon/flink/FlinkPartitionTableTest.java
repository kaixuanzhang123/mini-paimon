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

public class FlinkPartitionTableTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkPartitionTableTest.class);

    public static void main(String[] args) throws Exception {
        String warehousePath = "./warehouse_partition_test";
        
        cleanupWarehouse(warehousePath);
        
        try {
            TableEnvironment tableEnv = FlinkEnvironmentFactory.createTableEnvironment(warehousePath);
            
            LOG.info("=== Creating Database ===");
            tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS partition_db");
            tableEnv.executeSql("USE partition_db");
            
            LOG.info("=== Creating Partitioned Table: orders ===");
            String createOrdersSQL = 
                "CREATE TABLE IF NOT EXISTS orders (" +
                "  order_id BIGINT," +
                "  user_id BIGINT," +
                "  product STRING," +
                "  amount DOUBLE," +
                "  order_date STRING," +
                "  region STRING," +
                "  PRIMARY KEY (order_id) NOT ENFORCED" +
                ") PARTITIONED BY (order_date, region) " +
                "WITH (" +
                "  'connector' = 'mini-paimon'" +
                ")";
            tableEnv.executeSql(createOrdersSQL);
            LOG.info("Partitioned table created successfully");
            
            LOG.info("=== Inserting Partitioned Data ===");
            tableEnv.executeSql(
                "INSERT INTO orders VALUES " +
                "(1001, 1, 'Laptop', 1299.99, '2024-01-01', 'North'), " +
                "(1002, 2, 'Mouse', 29.99, '2024-01-01', 'North'), " +
                "(1003, 3, 'Keyboard', 89.99, '2024-01-01', 'South'), " +
                "(1004, 4, 'Monitor', 399.99, '2024-01-02', 'North'), " +
                "(1005, 5, 'Laptop', 1599.99, '2024-01-02', 'South'), " +
                "(1006, 6, 'Headphone', 199.99, '2024-01-02', 'East')"
            ).await();
            LOG.info("Data inserted successfully");
            
            LOG.info("=== Query 1: All orders ===");
            TableResult result = tableEnv.executeSql("SELECT * FROM orders ORDER BY order_id");
            result.print();
            
            LOG.info("=== Query 2: Filter by single partition (order_date='2024-01-01') ===");
            result = tableEnv.executeSql(
                "SELECT * FROM orders WHERE order_date = '2024-01-01' ORDER BY order_id"
            );
            result.print();
            
            LOG.info("=== Query 3: Filter by multiple partition columns ===");
            result = tableEnv.executeSql(
                "SELECT * FROM orders WHERE order_date = '2024-01-02' AND region = 'North'"
            );
            result.print();
            
            LOG.info("=== Query 4: Filter by partition and non-partition columns ===");
            result = tableEnv.executeSql(
                "SELECT * FROM orders WHERE order_date = '2024-01-01' AND amount > 50"
            );
            result.print();
            
            LOG.info("=== Query 5: Aggregation by partition ===");
            result = tableEnv.executeSql(
                "SELECT order_date, region, COUNT(*) as order_count, SUM(amount) as total_amount " +
                "FROM orders " +
                "GROUP BY order_date, region " +
                "ORDER BY order_date, region"
            );
            result.print();
            
            LOG.info("=== All partition table tests completed successfully! ===");
            
        } finally {
            cleanupWarehouse(warehousePath);
            LOG.info("Warehouse cleaned up");
        }
    }
    
    private static void cleanupWarehouse(String warehousePath) {
        try {
            Path path = Paths.get(warehousePath);
            if (Files.exists(path)) {
                Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (Exception e) {
                        }
                    });
            }
        } catch (Exception e) {
            LOG.warn("Failed to cleanup warehouse: {}", e.getMessage());
        }
    }
}
