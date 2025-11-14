package com.mini.paimon.spark;

import com.mini.paimon.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLExample {

    public static void main(String[] args) {
        String warehousePath = "./spark_warehouse";
        
        SparkSession spark = SparkSessionFactory.createSparkSession(warehousePath);
        
        try {
            spark.sql("CREATE DATABASE IF NOT EXISTS paimon.test_db").show();
            
            spark.sql("USE paimon.test_db").show();
            
            spark.sql(
                "CREATE TABLE IF NOT EXISTS users (" +
                "  id BIGINT," +
                "  name STRING," +
                "  age INT" +
                ") USING paimon"
            ).show();
            
            spark.sql(
                "INSERT INTO users VALUES " +
                "(1, 'Alice', 25), " +
                "(2, 'Bob', 30), " +
                "(3, 'Charlie', 35)"
            ).show();
            
            Dataset<Row> result = spark.sql("SELECT * FROM users");
            result.show();
            
            Dataset<Row> filtered = spark.sql("SELECT * FROM users WHERE age > 25");
            System.out.println("过滤结果:");
            filtered.show();
            
        } finally {
            SparkSessionFactory.stopSparkSession(spark);
        }
    }
}

