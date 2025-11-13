package com.mini.paimon.partition;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.table.*;
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
 * 分区表写入和查询测试示例
 * 演示如何往分区表中写入数据并按分区查询
 */
public class PartitionWriteReadExample {
    
    public static void main(String[] args) throws IOException {
        // 创建临时目录
        Path basePath = Paths.get("/tmp/mini-paimon-partition-write-test");
        cleanDirectory(basePath);
        Files.createDirectories(basePath);
        
        // 初始化 Catalog
        PathFactory pathFactory = new PathFactory(basePath.toString());
        CatalogContext context = CatalogContext.builder()
            .warehouse(basePath.toString())
            .build();
        Catalog catalog = new FileSystemCatalog("test-catalog", context);
        
        System.out.println("=== Mini-Paimon 分区表写入和查询示例 ===\n");
        
        try {
            catalog.createDatabase("default", true);
            
            // ========== 示例 1: 单级分区表 ==========
            System.out.println("【示例 1】单级分区表（按日期分区）");
            singlePartitionExample(catalog, pathFactory);
            
            System.out.println("\n" + "============================================================" + "\n");
            
            // ========== 示例 2: 多级分区表 ==========
            System.out.println("【示例 2】多级分区表（按日期和小时分区）");
            multiLevelPartitionExample(catalog, pathFactory);
            
            System.out.println("\n" + "============================================================" + "\n");
            
            // ========== 示例 3: 分区过滤查询 ==========
            System.out.println("【示例 3】分区过滤查询");
            partitionFilterExample(catalog);
            
        } catch (Exception e) {
            System.err.println("执行错误: " + e.getMessage());
            e.printStackTrace();
        }
        // 不删除目录，保留结果查看
        // cleanDirectory(basePath);
        
        System.out.println("\n=== 示例完成 ===");
    }
    
    /**
     * 单级分区表示例
     */
    private static void singlePartitionExample(Catalog catalog, PathFactory pathFactory) 
            throws IOException {
        
        // 1. 创建分区表
        System.out.println("\n1. 创建分区表 user_events (partitioned by dt)");
        
        Identifier tableId = new Identifier("default", "user_events");
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("user_id", DataType.INT(), false));
        fields.add(new Field("event_type", DataType.STRING(), true));
        fields.add(new Field("event_time", DataType.LONG(), true));
        fields.add(new Field("dt", DataType.STRING(), false));
        
        List<String> primaryKeys = Arrays.asList("user_id");
        List<String> partitionKeys = Arrays.asList("dt");
        
        Schema schema = new Schema(0, fields, primaryKeys, partitionKeys);
        catalog.createTable(tableId, schema, true);
        
        System.out.println("   ✓ 表创建成功，分区键: " + partitionKeys);
        
        // 2. 写入数据到不同分区
        System.out.println("\n2. 写入数据到不同分区");
        
        com.mini.paimon.table.Table table = catalog.getTable(tableId);
        
        try (TableWrite writer = table.newWrite()) {
            // 写入 2024-01-01 分区的数据
            writer.write(new Row(new Object[]{1, "login", 1704067200000L, "2024-01-01"}));
            writer.write(new Row(new Object[]{2, "click", 1704067201000L, "2024-01-01"}));
            writer.write(new Row(new Object[]{3, "logout", 1704067202000L, "2024-01-01"}));
            
            // 写入 2024-01-02 分区的数据
            writer.write(new Row(new Object[]{4, "login", 1704153600000L, "2024-01-02"}));
            writer.write(new Row(new Object[]{5, "view", 1704153601000L, "2024-01-02"}));
            
            // 写入 2024-01-03 分区的数据
            writer.write(new Row(new Object[]{6, "purchase", 1704240000000L, "2024-01-03"}));
            
            TableWrite.TableCommitMessage commitMsg = writer.prepareCommit();
            
            // 提交快照
            TableCommit commit = table.newCommit();
            commit.commit(commitMsg, Snapshot.CommitKind.APPEND);
        }
        
        System.out.println("   ✓ 写入 6 条数据到 3 个分区:");
        System.out.println("     - dt=2024-01-01: 3 条");
        System.out.println("     - dt=2024-01-02: 2 条");
        System.out.println("     - dt=2024-01-03: 1 条");
        
        // 3. 检查分区目录
        System.out.println("\n3. 检查分区目录结构");
        List<PartitionSpec> partitions = table.partitionManager().listPartitions();
        System.out.println("   ✓ 共创建 " + partitions.size() + " 个分区:");
        for (PartitionSpec spec : partitions) {
            System.out.println("     - " + spec.toPath());
        }
        
        // 4. 查询所有数据
        System.out.println("\n4. 查询所有数据");
        TableScan scan = table.newScan().withLatestSnapshot();
        TableScan.Plan plan = scan.plan();
        TableRead reader = table.newRead();
        List<Row> allRows = reader.read(plan);
        
        System.out.println("   ✓ 查询到 " + allRows.size() + " 条数据:");
        printRows(allRows, schema);
    }
    
    /**
     * 多级分区表示例
     */
    private static void multiLevelPartitionExample(Catalog catalog, PathFactory pathFactory) 
            throws IOException {
        
        // 1. 创建多级分区表
        System.out.println("\n1. 创建多级分区表 user_behavior (partitioned by dt, hour)");
        
        Identifier tableId = new Identifier("default", "user_behavior");
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("user_id", DataType.INT(), false));
        fields.add(new Field("action", DataType.STRING(), true));
        fields.add(new Field("dt", DataType.STRING(), false));
        fields.add(new Field("hour", DataType.STRING(), false));
        
        List<String> primaryKeys = Arrays.asList("user_id");
        List<String> partitionKeys = Arrays.asList("dt", "hour");
        
        Schema schema = new Schema(0, fields, primaryKeys, partitionKeys);
        catalog.createTable(tableId, schema, true);
        
        System.out.println("   ✓ 表创建成功，分区键: " + partitionKeys);
        
        // 2. 写入数据到不同分区
        System.out.println("\n2. 写入数据到多级分区");
        
        com.mini.paimon.table.Table table = catalog.getTable(tableId);
        
        try (TableWrite writer = table.newWrite()) {
            // dt=2024-01-01, hour=10
            writer.write(new Row(new Object[]{1, "view", "2024-01-01", "10"}));
            writer.write(new Row(new Object[]{2, "click", "2024-01-01", "10"}));
            
            // dt=2024-01-01, hour=11
            writer.write(new Row(new Object[]{3, "purchase", "2024-01-01", "11"}));
            
            // dt=2024-01-02, hour=10
            writer.write(new Row(new Object[]{4, "view", "2024-01-02", "10"}));
            
            TableWrite.TableCommitMessage commitMsg = writer.prepareCommit();
            
            TableCommit commit = table.newCommit();
            commit.commit(commitMsg, Snapshot.CommitKind.APPEND);
        }
        
        System.out.println("   ✓ 写入 4 条数据到多级分区:");
        System.out.println("     - dt=2024-01-01/hour=10: 2 条");
        System.out.println("     - dt=2024-01-01/hour=11: 1 条");
        System.out.println("     - dt=2024-01-02/hour=10: 1 条");
        
        // 3. 查询数据
        System.out.println("\n3. 查询所有数据");
        TableScan scan = table.newScan().withLatestSnapshot();
        TableScan.Plan plan = scan.plan();
        TableRead reader = table.newRead();
        List<Row> allRows = reader.read(plan);
        
        System.out.println("   ✓ 查询到 " + allRows.size() + " 条数据:");
        printRows(allRows, schema);
    }
    
    /**
     * 分区过滤查询示例
     */
    private static void partitionFilterExample(Catalog catalog) throws IOException {
        System.out.println("\n1. 使用分区过滤查询 user_events 表");
        
        Identifier tableId = new Identifier("default", "user_events");
        com.mini.paimon.table.Table table = catalog.getTable(tableId);
        
        // 创建分区过滤条件：只查询 dt=2024-01-01 的数据
        PartitionSpec partitionFilter = PartitionSpec.of("dt", "2024-01-01");
        
        System.out.println("   分区过滤: " + partitionFilter.toPath());
        
        TableScan scan = table.newScan()
            .withLatestSnapshot()
            .withPartitionFilter(partitionFilter);
        
        TableScan.Plan plan = scan.plan();
        TableRead reader = table.newRead();
        List<Row> rows = reader.read(plan);
        
        System.out.println("   ✓ 查询到 " + rows.size() + " 条数据:");
        printRows(rows, table.schema());
    }
    
    /**
     * 打印行数据
     */
    private static void printRows(List<Row> rows, Schema schema) {
        if (rows.isEmpty()) {
            System.out.println("     (无数据)");
            return;
        }
        
        // 打印表头
        System.out.print("     ");
        for (Field field : schema.getFields()) {
            System.out.printf("%-15s", field.getName());
        }
        System.out.println();
        
        // 打印分隔线
        System.out.print("     ");
        for (int i = 0; i < schema.getFields().size(); i++) {
            System.out.print("---------------");
        }
        System.out.println();
        
        // 打印数据
        for (Row row : rows) {
            System.out.print("     ");
            for (Object value : row.getValues()) {
                System.out.printf("%-15s", value != null ? value.toString() : "NULL");
            }
            System.out.println();
        }
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
