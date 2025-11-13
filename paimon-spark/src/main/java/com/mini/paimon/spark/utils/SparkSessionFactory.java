package com.mini.paimon.spark.utils;

import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    public static SparkSession createSparkSession(String warehousePath) {
        return createSparkSession(warehousePath, "mini-paimon-spark");
    }

    public static SparkSession createSparkSession(String warehousePath, String appName) {
        return SparkSession.builder()
            .appName(appName)
            .master("local[*]")
            .config("spark.sql.catalog.paimon", "com.mini.paimon.spark.catalog.SparkCatalog")
            .config("spark.sql.catalog.paimon.warehouse", warehousePath)
            .config("spark.sql.extensions", "org.apache.spark.sql.catalyst.extensions.CatalystExtensions")
            .getOrCreate();
    }

    public static void stopSparkSession(SparkSession spark) {
        if (spark != null) {
            spark.stop();
        }
    }
}

