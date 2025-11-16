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

public class FlinkUpdateDeleteTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkUpdateDeleteTest.class);

    public static void main(String[] args) throws Exception {
        String warehousePath = "./warehouse_update_delete_test";
        
        cleanupWarehouse(warehousePath);
        
        try {
            TableEnvironment tableEnv = FlinkEnvironmentFactory.createTableEnvironment(warehousePath);
            
            LOG.info("=== Creating Database ===");
            tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS test_db");
            tableEnv.executeSql("USE test_db");
            
            LOG.info("=== Creating Table with Primary Key ===");
            String createTableSQL = 
                "CREATE TABLE IF NOT EXISTS employees (" +
                "  emp_id BIGINT," +
                "  name STRING," +
                "  salary DOUBLE," +
                "  department STRING," +
                "  PRIMARY KEY (emp_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'mini-paimon'" +
                ")";
            tableEnv.executeSql(createTableSQL);
            LOG.info("Table created successfully");
            
            LOG.info("=== Inserting Initial Data ===");
            tableEnv.executeSql(
                "INSERT INTO employees VALUES " +
                "(1, 'Alice', 75000.0, 'Engineering'), " +
                "(2, 'Bob', 65000.0, 'Sales'), " +
                "(3, 'Charlie', 80000.0, 'Engineering'), " +
                "(4, 'Diana', 70000.0, 'HR')"
            ).await();
            
            LOG.info("=== Querying Initial Data ===");
            TableResult result = tableEnv.executeSql("SELECT * FROM employees ORDER BY emp_id");
            result.print();
            
            LOG.info("=== Updating Data (using INSERT with same primary key) ===");
            tableEnv.executeSql(
                "INSERT INTO employees VALUES " +
                "(2, 'Bob Smith', 70000.0, 'Sales'), " +
                "(3, 'Charlie Brown', 85000.0, 'Engineering')"
            ).await();
            
            LOG.info("=== Querying After Update ===");
            result = tableEnv.executeSql("SELECT * FROM employees ORDER BY emp_id");
            result.print();
            
            LOG.info("=== Note: DELETE operations are not yet fully supported ===");
            LOG.info("=== For primary key tables, updates are handled by inserting with same key ===");
            
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

