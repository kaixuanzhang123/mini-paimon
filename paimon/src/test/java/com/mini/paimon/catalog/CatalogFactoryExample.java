package com.mini.paimon.catalog;

import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.table.Table;
import com.mini.paimon.table.TableCommit;
import com.mini.paimon.table.TableRead;
import com.mini.paimon.table.TableWrite;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * CatalogFactory 使用示例
 * 演示如何通过 SPI 机制创建和使用 Catalog
 */
public class CatalogFactoryExample {
    
    public static void main(String[] args) throws Exception {
        String warehousePath = "warehouse_catalog_factory_example";
        
        // 清理旧数据
        cleanupWarehouse(warehousePath);
        
        System.out.println("=== CatalogFactory 使用示例 ===\n");
        
        // 1. 通过 CatalogLoader 创建 Catalog（使用 SPI 机制）
        System.out.println("1. 创建 Catalog（通过 SPI）");
        CatalogContext context = CatalogContext.builder()
            .warehouse(warehousePath)
            .option("catalog.name", "example_catalog")
            .option("catalog.default-database", "default")
            .build();
        
        Catalog catalog = CatalogLoader.load("filesystem", context);
        System.out.println("   ✓ Catalog 名称: " + catalog.name());
        System.out.println("   ✓ 默认数据库: " + catalog.getDefaultDatabase());
        System.out.println("   ✓ 可用的 Catalog 类型: " + CatalogLoader.getAvailableIdentifiers());
        System.out.println();
        
        // 2. 创建数据库
        System.out.println("2. 创建数据库");
        catalog.createDatabase("test_db", false);
        System.out.println("   ✓ 数据库 'test_db' 创建成功");
        System.out.println();
        
        // 3. 创建表
        System.out.println("3. 创建表");
        Identifier identifier = new Identifier("test_db", "users");
        Schema schema = new Schema(
            0,
            Arrays.asList(
                new Field("id", DataType.LONG(), false),
                new Field("name", DataType.STRING(), true),
                new Field("age", DataType.INT(), true)
            ),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        catalog.createTable(identifier, schema, false);
        System.out.println("   ✓ 表 'users' 创建成功");
        System.out.println("   ✓ Schema: " + schema.getFields());
        System.out.println();
        
        // 4. 写入数据
        System.out.println("4. 写入数据");
        Table table = catalog.getTable(identifier);
        TableWrite tableWrite = table.newWrite();
        
        tableWrite.write(new Row(Arrays.asList(1L, "Alice", 25)));
        tableWrite.write(new Row(Arrays.asList(2L, "Bob", 30)));
        tableWrite.write(new Row(Arrays.asList(3L, "Charlie", 35)));
        
        TableCommit tableCommit = table.newCommit();
        tableCommit.commit(tableWrite.prepareCommit());
        tableWrite.close();
        
        System.out.println("   ✓ 写入 3 条记录");
        System.out.println();
        
        // 5. 读取数据
        System.out.println("5. 读取数据");
        TableRead tableRead = table.newRead();
        com.mini.paimon.table.TableScan.Plan plan = table.newScan().plan();
        List<Row> rows = tableRead.read(plan);
        
        System.out.println("   ✓ 读取到 " + rows.size() + " 条记录:");
        for (Row row : rows) {
            System.out.println("     - " + row);
        }
        System.out.println();
        
        // 6. 查询 Catalog 信息
        System.out.println("6. 查询 Catalog 信息");
        List<String> databases = catalog.listDatabases();
        System.out.println("   ✓ 数据库列表: " + databases);
        
        List<String> tables = catalog.listTables("test_db");
        System.out.println("   ✓ 表列表: " + tables);
        System.out.println();
        
        // 7. 清理资源
        System.out.println("7. 清理资源");
        catalog.close();
        System.out.println("   ✓ Catalog 已关闭");
        
        System.out.println("\n=== 示例完成 ===");
    }
    
    private static void cleanupWarehouse(String path) throws Exception {
        File warehouseDir = new File(path);
        if (warehouseDir.exists()) {
            Files.walk(warehouseDir.toPath())
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }
}

