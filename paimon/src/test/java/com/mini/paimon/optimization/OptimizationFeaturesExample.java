package com.mini.paimon.optimization;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.filter.PartitionPredicate;
import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.reader.ParallelDataReader;
import com.mini.paimon.table.*;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * 性能优化功能演示
 * 展示分区过滤、Manifest缓存、并行读取三大优化特性
 */
public class OptimizationFeaturesExample {
    private static final Logger logger = LoggerFactory.getLogger(OptimizationFeaturesExample.class);
    
    public static void main(String[] args) {
        Path basePath = Paths.get("./warehouse");
        PathFactory pathFactory = new PathFactory(basePath.toString());
        
        try {
            // 清理旧数据
            if (Files.exists(basePath)) {
                deleteDirectory(basePath);
            }
            
            CatalogContext context = CatalogContext.builder()
                .warehouse(basePath.toString())
                .build();
            Catalog catalog = new FileSystemCatalog("test-catalog", context);
            
            System.out.println("========================================");
            System.out.println("性能优化功能演示");
            System.out.println("========================================\n");
            
            // 1. 创建分区表并写入大量数据
            createTableAndInsertData(catalog, pathFactory);
            
            // 2. 演示分区谓词下推
            demonstratePartitionPredicatePushdown(catalog, pathFactory);
            
            // 3. 演示Manifest增量读取
            demonstrateIncrementalManifestRead(catalog, pathFactory);
            
            // 4. 演示并行读取优化
            demonstrateParallelRead(catalog, pathFactory);
            
            // 5. 性能对比测试
            performanceComparison(catalog, pathFactory);
            
            System.out.println("\n========================================");
            System.out.println("演示完成");
            System.out.println("========================================");
            
        } catch (Exception e) {
            logger.error("执行失败", e);
            e.printStackTrace();
        }
    }
    
    /**
     * 创建表并插入测试数据
     */
    private static void createTableAndInsertData(Catalog catalog, PathFactory pathFactory) 
            throws IOException {
        System.out.println("【1】创建分区表并插入测试数据\n");

        catalog.createDatabase("default", true);
        
        Identifier tableId = new Identifier("default", "user_behavior");
        
        // 创建Schema
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("user_id", DataType.INT(), false));
        fields.add(new Field("action", DataType.STRING(), true));
        fields.add(new Field("timestamp", DataType.LONG(), true));
        fields.add(new Field("dt", DataType.STRING(), false));
        fields.add(new Field("hour", DataType.STRING(), false));
        
        List<String> primaryKeys = Arrays.asList("user_id", "dt", "hour");
        List<String> partitionKeys = Arrays.asList("dt", "hour");
        
        Schema schema = new Schema(0, fields, primaryKeys, partitionKeys);
        catalog.createTable(tableId, schema, true);
        
        System.out.println("✓ 创建表: user_behavior");
        System.out.println("  分区键: dt, hour");
        
        // 写入多个分区的数据
        com.mini.paimon.table.Table table = catalog.getTable(tableId);
        TableWrite writer = table.newWrite();
        
        int totalRows = 0;
        String[] dates = {"2024-01-01", "2024-01-02", "2024-01-03"};
        String[] hours = {"00", "06", "12", "18"};
        String[] actions = {"click", "view", "purchase", "logout"};
        
        for (String date : dates) {
            for (String hour : hours) {
                // 每个分区写入100条数据
                for (int i = 0; i < 100; i++) {
                    int userId = 1000 + (totalRows % 1000);
                    String action = actions[i % actions.length];
                    long timestamp = System.currentTimeMillis() + totalRows;
                    
                    Row row = new Row(new Object[]{userId, action, timestamp, date, hour});
                    writer.write(row);
                    totalRows++;
                }
            }
        }
        
        TableWrite.TableCommitMessage commitMsg = writer.prepareCommit();
        TableCommit commit = table.newCommit();
        commit.commit(commitMsg, com.mini.paimon.snapshot.Snapshot.CommitKind.APPEND);
        writer.close();
        
        System.out.println("✓ 写入数据: " + totalRows + " 行");
        System.out.println("  分区数量: " + (dates.length * hours.length) + "\n");
    }
    
    /**
     * 演示分区谓词下推
     */
    private static void demonstratePartitionPredicatePushdown(Catalog catalog, PathFactory pathFactory) 
            throws IOException {
        System.out.println("【2】分区谓词下推演示\n");
        
        Identifier tableId = new Identifier("default", "user_behavior");
        com.mini.paimon.table.Table table = catalog.getTable(tableId);
        
        // 测试1: 简单等值过滤
        System.out.println("测试1: 等值过滤 (dt='2024-01-01')");
        PartitionPredicate equalPred = PartitionPredicate.equal("dt", "2024-01-01");
        testPartitionFilter(table, equalPred);
        
        // 测试2: IN 过滤
        System.out.println("\n测试2: IN 过滤 (dt IN ('2024-01-01', '2024-01-02'))");
        PartitionPredicate inPred = PartitionPredicate.in("dt", "2024-01-01", "2024-01-02");
        testPartitionFilter(table, inPred);
        
        // 测试3: 范围过滤
        System.out.println("\n测试3: 范围过滤 (dt >= '2024-01-02')");
        PartitionPredicate rangePred = PartitionPredicate.greaterThanOrEqual("dt", "2024-01-02");
        testPartitionFilter(table, rangePred);
        
        // 测试4: 复合条件
        System.out.println("\n测试4: 复合条件 (dt='2024-01-01' AND hour >= '12')");
        PartitionPredicate complexPred = PartitionPredicate.equal("dt", "2024-01-01")
            .and(PartitionPredicate.greaterThanOrEqual("hour", "12"));
        testPartitionFilter(table, complexPred);
        
        System.out.println();
    }
    
    private static void testPartitionFilter(com.mini.paimon.table.Table table, 
                                           PartitionPredicate predicate) throws IOException {
        long startTime = System.currentTimeMillis();
        
        FileStoreTableScan scan = (FileStoreTableScan) table.newScan();
        scan.withLatestSnapshot();
        scan.withPartitionPredicate(predicate);
        
        TableScan.Plan plan = scan.plan();
        long scanTime = System.currentTimeMillis() - startTime;
        
        TableRead reader = table.newRead();
        List<Row> rows = reader.read(plan);
        long totalTime = System.currentTimeMillis() - startTime;
        
        System.out.println("  ✓ 谓词: " + predicate);
        System.out.println("  ✓ 扫描文件数: " + plan.files().size());
        System.out.println("  ✓ 读取行数: " + rows.size());
        System.out.println("  ✓ 扫描时间: " + scanTime + " ms");
        System.out.println("  ✓ 总时间: " + totalTime + " ms");
    }
    
    /**
     * 演示Manifest增量读取
     */
    private static void demonstrateIncrementalManifestRead(Catalog catalog, PathFactory pathFactory) 
            throws IOException {
        System.out.println("【3】Manifest增量读取演示\n");
        
        Identifier tableId = new Identifier("default", "user_behavior");
        com.mini.paimon.table.Table table = catalog.getTable(tableId);
        
        // 获取当前快照ID
        long currentSnapshotId = table.newScan().withLatestSnapshot().plan().snapshot().getId();
        System.out.println("当前快照ID: " + currentSnapshotId);
        
        // 写入新数据
        System.out.println("\n追加新数据...");
        TableWrite writer = table.newWrite();
        for (int i = 0; i < 50; i++) {
            Row row = new Row(new Object[]{2000 + i, "new_action", System.currentTimeMillis(), 
                           "2024-01-04", "00"});
            writer.write(row);
        }
        TableWrite.TableCommitMessage commitMsg = writer.prepareCommit();
        TableCommit commit = table.newCommit();
        commit.commit(commitMsg, com.mini.paimon.snapshot.Snapshot.CommitKind.APPEND);
        writer.close();
        
        long newSnapshotId = table.newScan().withLatestSnapshot().plan().snapshot().getId();
        System.out.println("新快照ID: " + newSnapshotId);
        
        // 测试增量读取
        System.out.println("\n增量读取测试:");
        long startTime = System.currentTimeMillis();
        
        FileStoreTableScan incrementalScan = (FileStoreTableScan) table.newScan();
        incrementalScan.withLatestSnapshot();
        incrementalScan.withIncrementalRead(currentSnapshotId);
        
        TableScan.Plan plan = incrementalScan.plan();
        long scanTime = System.currentTimeMillis() - startTime;
        
        System.out.println("  ✓ 增量文件数: " + plan.files().size());
        System.out.println("  ✓ 扫描时间: " + scanTime + " ms");
        
        // 打印缓存统计
        System.out.println("\nManifest缓存统计:");
        System.out.println("  " + FileStoreTableScan.getCacheStats());
        
        System.out.println();
    }
    
    /**
     * 演示并行读取优化
     */
    private static void demonstrateParallelRead(Catalog catalog, PathFactory pathFactory) 
            throws IOException {
        System.out.println("【4】并行读取优化演示\n");
        
        Identifier tableId = new Identifier("default", "user_behavior");
        com.mini.paimon.table.Table table = catalog.getTable(tableId);
        
        // 准备扫描计划
        TableScan scan = table.newScan().withLatestSnapshot();
        TableScan.Plan plan = scan.plan();
        
        System.out.println("总文件数: " + plan.files().size());
        
        // 测试不同并行度
        int[] parallelismLevels = {1, 2, 4};
        
        for (int parallelism : parallelismLevels) {
            long startTime = System.currentTimeMillis();
            
            try (ParallelDataReader reader = new ParallelDataReader(
                    table.schema(), pathFactory, "default", "user_behavior", parallelism)) {
                
                DataTableScan.Plan dataPlan = new DataTableScan.Plan(
                    plan.snapshot(), plan.files());
                List<Row> rows = reader.read(dataPlan);
                
                long duration = System.currentTimeMillis() - startTime;
                System.out.println("\n并行度 " + parallelism + ":");
                System.out.println("  ✓ 读取行数: " + rows.size());
                System.out.println("  ✓ 耗时: " + duration + " ms");
                System.out.println("  ✓ 吞吐量: " + (rows.size() * 1000 / Math.max(1, duration)) + " 行/秒");
            }
        }
        
        System.out.println();
    }
    
    /**
     * 性能对比测试
     */
    private static void performanceComparison(Catalog catalog, PathFactory pathFactory) 
            throws IOException {
        System.out.println("【5】性能对比测试\n");
        
        Identifier tableId = new Identifier("default", "user_behavior");
        com.mini.paimon.table.Table table = catalog.getTable(tableId);
        
        // 测试场景1: 全表扫描
        System.out.println("场景1: 全表扫描");
        compareReadPerformance(table, null, "全表扫描");
        
        // 测试场景2: 单分区查询
        System.out.println("\n场景2: 单分区查询 (dt='2024-01-01')");
        PartitionPredicate singlePartition = PartitionPredicate.equal("dt", "2024-01-01");
        compareReadPerformance(table, singlePartition, "单分区");
        
        // 测试场景3: 多分区范围查询
        System.out.println("\n场景3: 多分区范围查询 (dt >= '2024-01-02')");
        PartitionPredicate rangePartition = PartitionPredicate.greaterThanOrEqual("dt", "2024-01-02");
        compareReadPerformance(table, rangePartition, "范围查询");
        
        System.out.println();
    }
    
    private static void compareReadPerformance(com.mini.paimon.table.Table table,
                                              PartitionPredicate predicate,
                                              String scenario) throws IOException {
        // 串行读取
        long serialStart = System.currentTimeMillis();
        FileStoreTableScan serialScan = (FileStoreTableScan) table.newScan();
        serialScan.withLatestSnapshot();
        if (predicate != null) {
            serialScan.withPartitionPredicate(predicate);
        }
        TableScan.Plan serialPlan = serialScan.plan();
        
        FileStoreTableRead serialRead = (FileStoreTableRead) table.newRead();
        List<Row> serialRows = serialRead.read(serialPlan);
        long serialTime = System.currentTimeMillis() - serialStart;
        
        // 并行读取
        long parallelStart = System.currentTimeMillis();
        FileStoreTableScan parallelScan = (FileStoreTableScan) table.newScan();
        parallelScan.withLatestSnapshot();
        if (predicate != null) {
            parallelScan.withPartitionPredicate(predicate);
        }
        TableScan.Plan parallelPlan = parallelScan.plan();
        
        FileStoreTableRead parallelRead = (FileStoreTableRead) table.newRead();
        parallelRead.withParallelRead(true).withParallelism(4);
        List<Row> parallelRows = parallelRead.read(parallelPlan);
        long parallelTime = System.currentTimeMillis() - parallelStart;
        
        // 计算加速比
        double speedup = (double) serialTime / Math.max(1, parallelTime);
        
        System.out.println("  串行读取:");
        System.out.println("    - 文件数: " + serialPlan.files().size());
        System.out.println("    - 行数: " + serialRows.size());
        System.out.println("    - 耗时: " + serialTime + " ms");
        
        System.out.println("  并行读取:");
        System.out.println("    - 文件数: " + parallelPlan.files().size());
        System.out.println("    - 行数: " + parallelRows.size());
        System.out.println("    - 耗时: " + parallelTime + " ms");
        System.out.println("    - 加速比: " + String.format("%.2f", speedup) + "x");
    }
    
    /**
     * 删除目录
     */
    private static void deleteDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            Files.walk(directory)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // 忽略
                    }
                });
        }
    }
}
