package com.mini.paimon.sql;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * SQL 分区和删除功能示例
 * 演示 CREATE TABLE PARTITIONED BY 和 DELETE 语句
 */
public class SQLPartitionDeleteExample {

    public static void main(String[] args) throws IOException {
        // 创建临时目录
        Path basePath = Paths.get("./warehouse");
        cleanDirectory(basePath);
        Files.createDirectories(basePath);

        // 初始化 Catalog 和 SQLParser
        PathFactory pathFactory = new PathFactory("./warehouse");
        CatalogContext context = CatalogContext.builder()
                .warehouse(basePath.toString())
                .build();
        Catalog catalog = new FileSystemCatalog("test-catalog", context);
        SQLParser sqlParser = new SQLParser(catalog, pathFactory);

        System.out.println("=== Mini-Paimon 分区和删除功能示例 ===\n");

        try {
            // 0. 创建默认数据库
            catalog.createDatabase("default", true);

            // 1. 创建分区表（在 Schema 中直接指定分区键）
            System.out.println("1. 创建分区表（按 dt 分区）:");

            // 先手动创建带分区的表
            com.mini.paimon.catalog.Identifier identifier1 = new com.mini.paimon.catalog.Identifier("default", "user_events");
            List<com.mini.paimon.schema.Field> fields1 = new ArrayList<>();
            fields1.add(new com.mini.paimon.schema.Field("user_id", com.mini.paimon.schema.DataType.INT(), false));
            fields1.add(new com.mini.paimon.schema.Field("event_type", com.mini.paimon.schema.DataType.STRING(), true));
            fields1.add(new com.mini.paimon.schema.Field("event_time", com.mini.paimon.schema.DataType.INT(), true));
            fields1.add(new com.mini.paimon.schema.Field("dt", com.mini.paimon.schema.DataType.STRING(), false));

            List<String> primaryKeys1 = Arrays.asList("user_id");
            List<String> partitionKeys1 = Arrays.asList("dt");

            com.mini.paimon.schema.Schema schema1 = new com.mini.paimon.schema.Schema(0, fields1, primaryKeys1, partitionKeys1);
            catalog.createTable(identifier1, schema1, true);

            System.out.println("Table 'user_events' created successfully.");
            System.out.println("Partition keys: " + partitionKeys1);
            System.out.println();

            // 2. 插入数据到不同分区
            System.out.println("2. 插入数据到不同分区:");

            String insert1 = "INSERT INTO user_events VALUES (1, 'login', 1234567890, '2024-01-01')";
            sqlParser.executeSQL(insert1);
            System.out.println("插入分区 dt=2024-01-01 的数据 (user_id=1)");

            String insert2 = "INSERT INTO user_events VALUES (2, 'logout', 1234567891, '2024-01-01')";
            sqlParser.executeSQL(insert2);
            System.out.println("插入分区 dt=2024-01-01 的数据 (user_id=2)");

            String insert3 = "INSERT INTO user_events VALUES (3, 'click', 1234567892, '2024-01-02')";
            sqlParser.executeSQL(insert3);
            System.out.println("插入分区 dt=2024-01-02 的数据 (user_id=3)");
            System.out.println();

            // 3. 查询所有数据
            System.out.println("3. 查询所有数据:");
            String selectAllSQL = "SELECT * FROM user_events";
            sqlParser.executeSQL(selectAllSQL);
            System.out.println();

            // 4. 按主键删除数据
            System.out.println("4. 按主键删除数据 (user_id=1):");
            String deleteSQL = "DELETE FROM user_events WHERE user_id = 1";
            sqlParser.executeSQL(deleteSQL);
            System.out.println();

            // 5. 查询删除后的数据
            System.out.println("5. 查询删除后的数据:");
            sqlParser.executeSQL(selectAllSQL);
            System.out.println();

            // 6. 删除整个分区
            System.out.println("6. 删除分区 dt='2024-01-02':");
            String deletePartitionSQL = "DELETE FROM user_events WHERE dt = '2024-01-02'";
            sqlParser.executeSQL(deletePartitionSQL);
            System.out.println();

            // 7. 查询分区删除后的数据
            System.out.println("7. 查询分区删除后的数据:");
            sqlParser.executeSQL(selectAllSQL);
            System.out.println();

            // 8. 创建多级分区表
            System.out.println("8. 创建多级分区表（按 dt 和 hour 分区）:");

            com.mini.paimon.catalog.Identifier identifier2 = new com.mini.paimon.catalog.Identifier("default", "user_behavior");
            List<com.mini.paimon.schema.Field> fields2 = new ArrayList<>();
            fields2.add(new com.mini.paimon.schema.Field("user_id", com.mini.paimon.schema.DataType.INT(), false));
            fields2.add(new com.mini.paimon.schema.Field("action", com.mini.paimon.schema.DataType.STRING(), true));
            fields2.add(new com.mini.paimon.schema.Field("dt", com.mini.paimon.schema.DataType.STRING(), false));
            fields2.add(new com.mini.paimon.schema.Field("hour", com.mini.paimon.schema.DataType.STRING(), false));

            List<String> primaryKeys2 = Arrays.asList("user_id");
            List<String> partitionKeys2 = Arrays.asList("dt", "hour");

            com.mini.paimon.schema.Schema schema2 = new com.mini.paimon.schema.Schema(0, fields2, primaryKeys2, partitionKeys2);
            catalog.createTable(identifier2, schema2, true);

            System.out.println("Table 'user_behavior' created successfully.");
            System.out.println("Partition keys: " + partitionKeys2);
            System.out.println();

            // 9. 插入多级分区数据
            System.out.println("9. 插入多级分区数据:");
            String insert4 = "INSERT INTO user_behavior VALUES (1, 'view', '2024-01-01', '10')";
            sqlParser.executeSQL(insert4);
            System.out.println("插入分区 dt=2024-01-01/hour=10 的数据");

            String insert5 = "INSERT INTO user_behavior VALUES (2, 'click', '2024-01-01', '11')";
            sqlParser.executeSQL(insert5);
            System.out.println("插入分区 dt=2024-01-01/hour=11 的数据");
            System.out.println();

            // 10. 查询多级分区表
            System.out.println("10. 查询多级分区表:");
            String selectAllSQL2 = "SELECT * FROM user_behavior";
            sqlParser.executeSQL(selectAllSQL2);

        } catch (Exception e) {
            System.err.println("执行错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 清理测试数据
            //cleanDirectory(basePath);
        }

        System.out.println("\n=== 示例完成 ===");
    }

    /**
     * 清理目录
     */
    private static void cleanDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            Files.walk(directory)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }
}
