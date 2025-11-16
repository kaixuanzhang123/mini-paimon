package com.mini.paimon.spark;

import com.mini.paimon.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkIndexTableTest {

    public static void main(String[] args) {
        String warehousePath = "./spark_warehouse_index_test";
        
        SparkSession spark = SparkSessionFactory.createSparkSession(warehousePath, "index-test");
        
        try {
            System.out.println("=== Creating Database ===");
            spark.sql("CREATE DATABASE IF NOT EXISTS paimon.index_db").show();
            spark.sql("USE paimon.index_db").show();
            
            System.out.println("=== Creating Table with Bitmap Index ===");
            spark.sql(
                "CREATE TABLE IF NOT EXISTS products (" +
                "  product_id BIGINT," +
                "  product_name STRING," +
                "  category STRING," +
                "  price DOUBLE" +
                ") USING paimon " +
                "TBLPROPERTIES (" +
                "  'index.category.type' = 'bitmap'" +
                ")"
            ).show();
            System.out.println("Table with bitmap index created successfully");
            
            System.out.println("=== Creating Table with Multiple Indexes ===");
            spark.sql(
                "CREATE TABLE IF NOT EXISTS users (" +
                "  user_id BIGINT," +
                "  username STRING," +
                "  email STRING," +
                "  age INT," +
                "  country STRING" +
                ") USING paimon " +
                "TBLPROPERTIES (" +
                "  'index.username.type' = 'bloom_filter'," +
                "  'index.country.type' = 'bitmap'," +
                "  'index.age.type' = 'min_max'" +
                ")"
            ).show();
            System.out.println("Table with multiple indexes created successfully");
            
            System.out.println("=== Inserting Data into Products ===");
            spark.sql(
                "INSERT INTO products VALUES " +
                "(1, 'Laptop', 'Electronics', 999.99), " +
                "(2, 'Mouse', 'Electronics', 29.99), " +
                "(3, 'Desk', 'Furniture', 299.99), " +
                "(4, 'Chair', 'Furniture', 199.99), " +
                "(5, 'Monitor', 'Electronics', 399.99)"
            ).show();
            
            System.out.println("=== Inserting Data into Users ===");
            spark.sql(
                "INSERT INTO users VALUES " +
                "(1, 'alice', 'alice@example.com', 25, 'USA'), " +
                "(2, 'bob', 'bob@example.com', 30, 'UK'), " +
                "(3, 'charlie', 'charlie@example.com', 35, 'USA'), " +
                "(4, 'diana', 'diana@example.com', 28, 'Canada'), " +
                "(5, 'eve', 'eve@example.com', 32, 'USA')"
            ).show();
            
            System.out.println("=== Querying Products (with bitmap index on category) ===");
            Dataset<Row> result = spark.sql(
                "SELECT * FROM products WHERE category = 'Electronics' ORDER BY product_id"
            );
            result.show();
            
            System.out.println("=== Querying Users (with multiple indexes) ===");
            result = spark.sql(
                "SELECT * FROM users WHERE country = 'USA' AND age > 25 ORDER BY user_id"
            );
            result.show();
            
            System.out.println("=== Test Completed Successfully ===");
            
        } finally {
            SparkSessionFactory.stopSparkSession(spark);
        }
    }
}

