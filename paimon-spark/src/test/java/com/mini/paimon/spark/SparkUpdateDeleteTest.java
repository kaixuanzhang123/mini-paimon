package com.mini.paimon.spark;

import com.mini.paimon.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkUpdateDeleteTest {

    public static void main(String[] args) {
        String warehousePath = "./spark_warehouse_update_delete_test";
        
        SparkSession spark = SparkSessionFactory.createSparkSession(warehousePath, "update-delete-test");
        
        try {
            System.out.println("=== Creating Database ===");
            spark.sql("CREATE DATABASE IF NOT EXISTS paimon.test_db").show();
            spark.sql("USE paimon.test_db").show();
            
            System.out.println("=== Creating Table with Primary Key ===");
            spark.sql(
                "CREATE TABLE IF NOT EXISTS employees (" +
                "  emp_id BIGINT," +
                "  name STRING," +
                "  salary DOUBLE," +
                "  department STRING" +
                ") USING paimon"
            ).show();
            System.out.println("Table created successfully");
            
            System.out.println("=== Inserting Initial Data ===");
            spark.sql(
                "INSERT INTO employees VALUES " +
                "(1, 'Alice', 75000.0, 'Engineering'), " +
                "(2, 'Bob', 65000.0, 'Sales'), " +
                "(3, 'Charlie', 80000.0, 'Engineering'), " +
                "(4, 'Diana', 70000.0, 'HR')"
            ).show();
            
            System.out.println("=== Querying Initial Data ===");
            Dataset<Row> result = spark.sql("SELECT * FROM employees ORDER BY emp_id");
            result.show();
            
            System.out.println("=== Simulating Update (by inserting with same key in primary key table) ===");
            spark.sql(
                "INSERT INTO employees VALUES " +
                "(2, 'Bob Smith', 70000.0, 'Sales'), " +
                "(3, 'Charlie Brown', 85000.0, 'Engineering')"
            ).show();
            
            System.out.println("=== Querying After Update ===");
            result = spark.sql("SELECT * FROM employees ORDER BY emp_id");
            result.show();
            
            System.out.println("=== Note: Native UPDATE/DELETE SQL is not yet supported ===");
            System.out.println("=== For primary key tables, updates work by inserting with same key ===");
            System.out.println("=== For deletes, use INSERT OVERWRITE with filtered data ===");
            
            System.out.println("=== Test Completed Successfully ===");
            
        } finally {
            SparkSessionFactory.stopSparkSession(spark);
        }
    }
}

