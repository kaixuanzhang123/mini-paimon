package com.mini.paimon.spark;

import com.mini.paimon.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkAlterTableTest {

    public static void main(String[] args) {
        String warehousePath = "./spark_warehouse_alter_table_test";
        
        SparkSession spark = SparkSessionFactory.createSparkSession(warehousePath, "alter-table-test");
        
        try {
            System.out.println("=== Creating Database ===");
            spark.sql("CREATE DATABASE IF NOT EXISTS paimon.test_db").show();
            spark.sql("USE paimon.test_db").show();
            
            System.out.println("=== Creating Initial Table ===");
            spark.sql(
                "CREATE TABLE IF NOT EXISTS products (" +
                "  product_id BIGINT," +
                "  product_name STRING," +
                "  price DOUBLE" +
                ") USING paimon"
            ).show();
            System.out.println("Initial table created");
            
            System.out.println("=== Inserting Data ===");
            spark.sql(
                "INSERT INTO products VALUES " +
                "(1, 'Laptop', 999.99), " +
                "(2, 'Mouse', 29.99), " +
                "(3, 'Keyboard', 79.99)"
            ).show();
            
            System.out.println("=== Querying Initial Data ===");
            Dataset<Row> result = spark.sql("SELECT * FROM products ORDER BY product_id");
            result.show();
            
            System.out.println("=== Adding New Column ===");
            spark.sql("ALTER TABLE products ADD COLUMN category STRING").show();
            System.out.println("Column 'category' added successfully");
            
            System.out.println("=== Querying After ALTER TABLE ===");
            result = spark.sql("SELECT * FROM products ORDER BY product_id");
            result.show();
            
            System.out.println("=== Note: Schema evolution is supported ===");
            System.out.println("=== New schema versions are automatically managed ===");
            
            System.out.println("=== Test Completed Successfully ===");
            
        } catch (Exception e) {
            System.err.println("Error during ALTER TABLE test: " + e.getMessage());
            e.printStackTrace();
        } finally {
            SparkSessionFactory.stopSparkSession(spark);
        }
    }
}

