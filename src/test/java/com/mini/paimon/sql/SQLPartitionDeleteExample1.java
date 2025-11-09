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
public class SQLPartitionDeleteExample1 {
    
    public static void main(String[] args) throws IOException {
        // 创建临时目录

        
        // 初始化 Catalog 和 SQLParser
        PathFactory pathFactory = new PathFactory("./warehouse");
        CatalogContext context = CatalogContext.builder()
            .warehouse("./warehouse")
            .build();
        Catalog catalog = new FileSystemCatalog("test-catalog", context);
        SQLParser sqlParser = new SQLParser(catalog, pathFactory);
        
        System.out.println("=== Mini-Paimon 分区和删除功能示例 ===\n");
        
        try {


            // 4. 删除整个表（测试 DROP TABLE）
            System.out.println("4. 删除表 user_behavior:");
            String dropSQL = "DROP TABLE user_behavior";
            sqlParser.executeSQL(dropSQL);
            System.out.println();
            
            // 5. 验证表目录已被删除
            System.out.println("5. 验证表目录是否存在:");
            java.nio.file.Path tableDir = Paths.get("./warehouse/default/user_behavior");
            if (Files.exists(tableDir)) {
                System.out.println("错误：表目录仍然存在！");
            } else {
                System.out.println("成功：表目录已被完全删除！");
            }
            System.out.println();

            
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
