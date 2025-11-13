package com.mini.paimon.spark;

import com.mini.paimon.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkPaimonIntegrationTest {

    public static void main(String[] args) {
        String warehousePath = "./test_spark_warehouse";
        
        SparkSession spark = SparkSessionFactory.createSparkSession(warehousePath, "integration-test");
        
        try {
            testBasicOperations(spark);
            testQueryOperations(spark);
        } finally {
            SparkSessionFactory.stopSparkSession(spark);
        }
    }

    private static void testBasicOperations(SparkSession spark) {
        System.out.println("=== 测试基础操作 ===");
        
        spark.sql("CREATE DATABASE IF NOT EXISTS paimon.integration_db").show();
        spark.sql("USE paimon.integration_db").show();
        
        spark.sql(
            "CREATE TABLE IF NOT EXISTS products (" +
            "  product_id BIGINT," +
            "  product_name STRING," +
            "  price DOUBLE," +
            "  in_stock BOOLEAN" +
            ") USING paimon"
        ).show();
        
        spark.sql(
            "INSERT INTO products VALUES " +
            "(1, 'Laptop', 999.99, true), " +
            "(2, 'Mouse', 29.99, true), " +
            "(3, 'Keyboard', 79.99, false)"
        ).show();
        
        System.out.println("所有产品:");
        spark.sql("SELECT * FROM products").show();
    }

    private static void testQueryOperations(SparkSession spark) {
        System.out.println("\n=== 测试查询操作 ===");
        
        System.out.println("过滤查询 - 价格大于50:");
        spark.sql("SELECT * FROM products WHERE price > 50.0").show();
        
        System.out.println("投影查询 - 仅显示产品名称和价格:");
        spark.sql("SELECT product_name, price FROM products").show();
        
        System.out.println("排序查询 - 按价格降序:");
        spark.sql("SELECT * FROM products ORDER BY price DESC").show();
        
        System.out.println("聚合查询 - 计算平均价格:");
        spark.sql("SELECT AVG(price) as avg_price FROM products").show();
        
        System.out.println("计数查询 - 统计库存产品:");
        spark.sql("SELECT COUNT(*) as total FROM products WHERE in_stock = true").show();
    }
}

