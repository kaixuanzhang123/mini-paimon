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

public class FlinkPartitionCRUDTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkPartitionCRUDTest.class);

    public static void main(String[] args) throws Exception {
        String warehousePath = "./warehouse_partition_crud_test";
        
        cleanupWarehouse(warehousePath);
        
        try {
            TableEnvironment tableEnv = FlinkEnvironmentFactory.createTableEnvironment(warehousePath);
            
            LOG.info("=== Creating Database ===");
            tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS test_db");
            tableEnv.executeSql("USE test_db");
            
            LOG.info("=== Creating Partitioned Table ===");
            String createTableSQL = 
                "CREATE TABLE IF NOT EXISTS sales (" +
                "  sale_id BIGINT," +
                "  product_name STRING," +
                "  amount DOUBLE," +
                "  sale_date STRING," +
                "  region STRING," +
                "  PRIMARY KEY (sale_id) NOT ENFORCED" +
                ") PARTITIONED BY (sale_date, region) " +
                "WITH (" +
                "  'connector' = 'mini-paimon'" +
                ")";
            tableEnv.executeSql(createTableSQL);
            LOG.info("Partitioned table created successfully");
            
            LOG.info("=== INSERT: Adding data to multiple partitions ===");
            tableEnv.executeSql(
                "INSERT INTO sales VALUES " +
                "(1, 'Laptop', 999.99, '2024-01-15', 'US'), " +
                "(2, 'Mouse', 29.99, '2024-01-15', 'US'), " +
                "(3, 'Keyboard', 79.99, '2024-01-15', 'EU'), " +
                "(4, 'Monitor', 399.99, '2024-01-16', 'US'), " +
                "(5, 'Headset', 149.99, '2024-01-16', 'EU')"
            ).await();
            LOG.info("Data inserted into partitions");
            
            LOG.info("=== SELECT: Querying all data ===");
            TableResult result = tableEnv.executeSql("SELECT * FROM sales ORDER BY sale_id");
            result.print();
            
            LOG.info("=== SELECT: Querying specific partition (sale_date='2024-01-15', region='US') ===");
            result = tableEnv.executeSql(
                "SELECT * FROM sales WHERE sale_date = '2024-01-15' AND region = 'US' ORDER BY sale_id"
            );
            result.print();
            
            LOG.info("=== SELECT: Querying by date partition ===");
            result = tableEnv.executeSql(
                "SELECT * FROM sales WHERE sale_date = '2024-01-16' ORDER BY sale_id"
            );
            result.print();
            
            LOG.info("=== UPDATE: Updating data in partition (using INSERT with same key) ===");
            tableEnv.executeSql(
                "INSERT INTO sales VALUES " +
                "(2, 'Gaming Mouse', 49.99, '2024-01-15', 'US')"
            ).await();
            LOG.info("Data updated");
            
            LOG.info("=== SELECT: Verifying update ===");
            result = tableEnv.executeSql(
                "SELECT * FROM sales WHERE sale_id = 2"
            );
            result.print();
            
            LOG.info("=== INSERT: Adding more data to new partition ===");
            tableEnv.executeSql(
                "INSERT INTO sales VALUES " +
                "(6, 'Webcam', 89.99, '2024-01-17', 'ASIA'), " +
                "(7, 'Speaker', 129.99, '2024-01-17', 'ASIA')"
            ).await();
            LOG.info("New partition data inserted");
            
            LOG.info("=== SELECT: Querying all partitions ===");
            result = tableEnv.executeSql(
                "SELECT sale_date, region, COUNT(*) as count, SUM(amount) as total " +
                "FROM sales " +
                "GROUP BY sale_date, region " +
                "ORDER BY sale_date, region"
            );
            result.print();
            
            LOG.info("=== Partition Statistics ===");
            result = tableEnv.executeSql(
                "SELECT sale_date, COUNT(DISTINCT region) as regions, " +
                "COUNT(*) as sales, SUM(amount) as revenue " +
                "FROM sales " +
                "GROUP BY sale_date " +
                "ORDER BY sale_date"
            );
            result.print();
            
            LOG.info("=== Test Completed Successfully ===");
            LOG.info("=== Demonstrated: CREATE, INSERT, SELECT (with partition filtering), UPDATE ===");
            LOG.info("=== Note: DELETE operations require additional implementation ===");
            
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

