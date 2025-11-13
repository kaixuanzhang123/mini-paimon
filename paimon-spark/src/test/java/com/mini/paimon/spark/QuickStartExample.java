package com.mini.paimon.spark;

import com.mini.paimon.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

public class QuickStartExample {

    public static void main(String[] args) {
        String warehousePath = "quickstart_warehouse";
        
        SparkSession spark = SparkSessionFactory.createSparkSession(warehousePath, "quickstart");
        
        try {
            System.out.println("创建数据库...");
            spark.sql("CREATE DATABASE IF NOT EXISTS paimon.my_db").show();
            
            System.out.println("使用数据库...");
            spark.sql("USE paimon.my_db").show();
            
            System.out.println("创建表...");
            spark.sql(
                "CREATE TABLE IF NOT EXISTS orders (" +
                "  order_id BIGINT," +
                "  customer_name STRING," +
                "  amount DOUBLE" +
                ") USING paimon"
            ).show();
            
            System.out.println("插入数据...");
            spark.sql(
                "INSERT INTO orders VALUES " +
                "(1001, 'Alice', 150.0), " +
                "(1002, 'Bob', 200.5)"
            ).show();
            
            System.out.println("查询数据:");
            spark.sql("SELECT * FROM orders").show();
            
            System.out.println("快速开始示例完成!");
            
        } finally {
            SparkSessionFactory.stopSparkSession(spark);
        }
    }
}

