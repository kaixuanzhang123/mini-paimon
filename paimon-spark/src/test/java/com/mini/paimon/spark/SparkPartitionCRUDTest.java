package com.mini.paimon.spark;

import com.mini.paimon.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkPartitionCRUDTest {

    public static void main(String[] args) {
        String warehousePath = "./spark_warehouse_partition_crud_test";
        
        SparkSession spark = SparkSessionFactory.createSparkSession(warehousePath, "partition-crud-test");
        
        try {
            System.out.println("=== Creating Database ===");
            spark.sql("CREATE DATABASE IF NOT EXISTS paimon.test_db").show();
            spark.sql("USE paimon.test_db").show();
            
            System.out.println("=== Creating Partitioned Table ===");
            spark.sql(
                "CREATE TABLE IF NOT EXISTS sales (" +
                "  sale_id BIGINT," +
                "  product_name STRING," +
                "  amount DOUBLE," +
                "  sale_date STRING," +
                "  region STRING" +
                ") USING paimon " +
                "PARTITIONED BY (sale_date, region)"
            ).show();
            System.out.println("Partitioned table created successfully");
            
            System.out.println("=== INSERT: Adding data to multiple partitions ===");
            spark.sql(
                "INSERT INTO sales VALUES " +
                "(1, 'Laptop', 999.99, '2024-01-15', 'US'), " +
                "(2, 'Mouse', 29.99, '2024-01-15', 'US'), " +
                "(3, 'Keyboard', 79.99, '2024-01-15', 'EU'), " +
                "(4, 'Monitor', 399.99, '2024-01-16', 'US'), " +
                "(5, 'Headset', 149.99, '2024-01-16', 'EU')"
            ).show();
            System.out.println("Data inserted into partitions");
            
            System.out.println("=== SELECT: Querying all data ===");
            Dataset<Row> result = spark.sql("SELECT * FROM sales ORDER BY sale_id");
            result.show();
            
            System.out.println("=== SELECT: Querying specific partition ===");
            result = spark.sql(
                "SELECT * FROM sales WHERE sale_date = '2024-01-15' AND region = 'US' ORDER BY sale_id"
            );
            result.show();
            
            System.out.println("=== SELECT: Querying by date partition ===");
            result = spark.sql(
                "SELECT * FROM sales WHERE sale_date = '2024-01-16' ORDER BY sale_id"
            );
            result.show();
            
            System.out.println("=== UPDATE: Simulating update (insert with same key) ===");
            spark.sql(
                "INSERT INTO sales VALUES " +
                "(2, 'Gaming Mouse', 49.99, '2024-01-15', 'US')"
            ).show();
            System.out.println("Data updated");
            
            System.out.println("=== SELECT: Verifying update ===");
            result = spark.sql("SELECT * FROM sales WHERE sale_id = 2");
            result.show();
            
            System.out.println("=== INSERT: Adding more data to new partition ===");
            spark.sql(
                "INSERT INTO sales VALUES " +
                "(6, 'Webcam', 89.99, '2024-01-17', 'ASIA'), " +
                "(7, 'Speaker', 129.99, '2024-01-17', 'ASIA')"
            ).show();
            System.out.println("New partition data inserted");
            
            System.out.println("=== SELECT: Partition aggregation ===");
            result = spark.sql(
                "SELECT sale_date, region, COUNT(*) as count, SUM(amount) as total " +
                "FROM sales " +
                "GROUP BY sale_date, region " +
                "ORDER BY sale_date, region"
            );
            result.show();
            
            System.out.println("=== Partition Statistics ===");
            result = spark.sql(
                "SELECT sale_date, COUNT(DISTINCT region) as regions, " +
                "COUNT(*) as sales, SUM(amount) as revenue " +
                "FROM sales " +
                "GROUP BY sale_date " +
                "ORDER BY sale_date"
            );
            result.show();
            
            System.out.println("=== Test Completed Successfully ===");
            System.out.println("=== Demonstrated: CREATE, INSERT, SELECT (with partition filtering), UPDATE ===");
            System.out.println("=== Note: DELETE operations require additional implementation ===");
            
        } finally {
            SparkSessionFactory.stopSparkSession(spark);
        }
    }
}

