package com.mini.paimon.flink;

import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.flink.catalog.FlinkCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class FlinkPaimonIntegrationTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkPaimonIntegrationTest.class);

    public static void main(String[] args) throws Exception {
        String warehousePath = "warehouse_flink_test";
        
        cleanupWarehouse(warehousePath);
        
        LOG.info("=== Initializing Flink TableEnvironment ===");
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inBatchMode()
            .build();
        
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        
        CatalogContext catalogContext = CatalogContext.builder()
            .warehouse(warehousePath)
            .build();
        
        FileSystemCatalog paimonCatalog = new FileSystemCatalog("paimon", warehousePath, catalogContext);
        
        FlinkCatalog flinkCatalog = new FlinkCatalog("paimon", paimonCatalog, "default", warehousePath);
        
        tableEnv.registerCatalog("paimon", flinkCatalog);
        tableEnv.useCatalog("paimon");
        
        LOG.info("=== Test 1: Database Operations ===");
        testDatabaseOperations(tableEnv);
        
        LOG.info("\n=== Test 2: Simple Table Operations ===");
        testSimpleTableOperations(tableEnv);
        
        LOG.info("\n=== Test 3: Partitioned Table Operations ===");
        testPartitionedTableOperations(tableEnv);
        
        LOG.info("\n=== Test 4: Complex Queries ===");
        testComplexQueries(tableEnv);
        
        LOG.info("\n=== All Tests Completed Successfully! ===");
        
        cleanupWarehouse(warehousePath);
    }
    
    private static void testDatabaseOperations(TableEnvironment tableEnv) throws Exception {
        LOG.info("Creating databases...");
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS db1");
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS db2");
        
        LOG.info("Listing databases...");
        TableResult result = tableEnv.executeSql("SHOW DATABASES");
        result.print();
        
        tableEnv.executeSql("USE db1");
        LOG.info("Current database: db1");
    }
    
    private static void testSimpleTableOperations(TableEnvironment tableEnv) throws Exception {
        LOG.info("Creating table: employees");
        String createTableSQL = 
            "CREATE TABLE employees (" +
            "  emp_id BIGINT," +
            "  name STRING," +
            "  department STRING," +
            "  salary DOUBLE," +
            "  PRIMARY KEY (emp_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'mini-paimon'" +
            ")";
        tableEnv.executeSql(createTableSQL);
        
        LOG.info("Inserting data...");
        tableEnv.executeSql(
            "INSERT INTO employees VALUES " +
            "(1, 'Alice', 'Engineering', 95000.0), " +
            "(2, 'Bob', 'Sales', 75000.0), " +
            "(3, 'Charlie', 'Engineering', 88000.0), " +
            "(4, 'Diana', 'HR', 72000.0), " +
            "(5, 'Eve', 'Engineering', 102000.0)"
        ).await();
        
        LOG.info("Querying all employees:");
        TableResult result = tableEnv.executeSql("SELECT * FROM employees ORDER BY emp_id");
        result.print();
        
        LOG.info("Querying Engineering department:");
        result = tableEnv.executeSql(
            "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC"
        );
        result.print();
        
        LOG.info("Calculating average salary:");
        result = tableEnv.executeSql(
            "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department"
        );
        result.print();
    }
    
    private static void testPartitionedTableOperations(TableEnvironment tableEnv) throws Exception {
        LOG.info("Creating partitioned table: sales");
        String createTableSQL = 
            "CREATE TABLE sales (" +
            "  sale_id BIGINT," +
            "  product STRING," +
            "  amount DOUBLE," +
            "  sale_date STRING," +
            "  region STRING," +
            "  PRIMARY KEY (sale_id) NOT ENFORCED" +
            ") PARTITIONED BY (region, sale_date) " +
            "WITH (" +
            "  'connector' = 'mini-paimon'" +
            ")";
        tableEnv.executeSql(createTableSQL);
        
        LOG.info("Inserting partitioned data...");
        tableEnv.executeSql(
            "INSERT INTO sales VALUES " +
            "(1, 'Laptop', 1500.0, '2024-01-01', 'North'), " +
            "(2, 'Mouse', 25.0, '2024-01-01', 'North'), " +
            "(3, 'Keyboard', 75.0, '2024-01-01', 'South'), " +
            "(4, 'Monitor', 300.0, '2024-01-02', 'North'), " +
            "(5, 'Laptop', 1600.0, '2024-01-02', 'South')"
        ).await();
        
        LOG.info("Querying partition: region=North");
        TableResult result = tableEnv.executeSql(
            "SELECT * FROM sales WHERE region = 'North' ORDER BY sale_id"
        );
        result.print();
        
        LOG.info("Querying partition: sale_date=2024-01-01");
        result = tableEnv.executeSql(
            "SELECT * FROM sales WHERE sale_date = '2024-01-01' ORDER BY sale_id"
        );
        result.print();
        
        LOG.info("Aggregating by partition:");
        result = tableEnv.executeSql(
            "SELECT region, sale_date, SUM(amount) as total_sales " +
            "FROM sales GROUP BY region, sale_date"
        );
        result.print();
    }
    
    private static void testComplexQueries(TableEnvironment tableEnv) throws Exception {
        LOG.info("Testing JOIN operations");
        
        LOG.info("Creating customer table:");
        tableEnv.executeSql(
            "CREATE TABLE customers (" +
            "  customer_id BIGINT," +
            "  customer_name STRING," +
            "  PRIMARY KEY (customer_id) NOT ENFORCED" +
            ") WITH ('connector' = 'mini-paimon')"
        );
        
        tableEnv.executeSql(
            "INSERT INTO customers VALUES " +
            "(1, 'ACME Corp'), " +
            "(2, 'TechStart Inc'), " +
            "(3, 'Global Systems')"
        ).await();
        
        LOG.info("Creating orders table:");
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
            "  order_id BIGINT," +
            "  customer_id BIGINT," +
            "  total DOUBLE," +
            "  PRIMARY KEY (order_id) NOT ENFORCED" +
            ") WITH ('connector' = 'mini-paimon')"
        );
        
        tableEnv.executeSql(
            "INSERT INTO orders VALUES " +
            "(101, 1, 5000.0), " +
            "(102, 2, 3500.0), " +
            "(103, 1, 7200.0), " +
            "(104, 3, 4100.0)"
        ).await();
        
        LOG.info("Executing JOIN query:");
        TableResult result = tableEnv.executeSql(
            "SELECT c.customer_name, SUM(o.total) as total_orders " +
            "FROM customers c " +
            "JOIN orders o ON c.customer_id = o.customer_id " +
            "GROUP BY c.customer_name"
        );
        result.print();
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
                            // Ignore
                        }
                    });
            }
        } catch (Exception e) {
            LOG.warn("Failed to cleanup warehouse: {}", e.getMessage());
        }
    }
}
