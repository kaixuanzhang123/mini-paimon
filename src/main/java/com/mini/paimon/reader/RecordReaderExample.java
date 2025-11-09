package com.mini.paimon.reader;

import com.mini.paimon.metadata.*;
import com.mini.paimon.storage.LSMTree;
import com.mini.paimon.table.DataTableRead;
import com.mini.paimon.table.DataTableScan;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Record Reader Example
 * 演示新的 RecordReader 架构和优化功能
 * 
 * 优化亮点：
 * 1. Block 级别的懒加载 - 只读取需要的 Block
 * 2. 多层过滤优化 - 文件级、Block 级、行级
 * 3. 谓词和投影下推 - 减少数据传输和处理
 * 4. 流式处理 - 支持大数据集的内存友好处理
 */
public class RecordReaderExample {
    private static final Logger logger = LoggerFactory.getLogger(RecordReaderExample.class);
    
    public static void main(String[] args) throws IOException {
        logger.info("===== RecordReader 优化示例 =====");
        
        // 1. 创建测试 Schema
        Schema schema = createTestSchema();
        
        // 2. 写入测试数据
        String tablePath = "/tmp/mini-paimon-reader-test";
        writeTestData(schema, tablePath);
        
        // 3. 演示优化读取
        demonstrateOptimizedRead(schema, tablePath);
        
        logger.info("===== 示例完成 =====");
    }
    
    /**
     * 创建测试 Schema
     */
    private static Schema createTestSchema() {
        Field idField = new Field("id", DataType.INT, false);
        Field nameField = new Field("name", DataType.STRING, false);
        Field ageField = new Field("age", DataType.INT, true);
        Field scoreField = new Field("score", DataType.DOUBLE, true);
        
        List<Field> fields = Arrays.asList(idField, nameField, ageField, scoreField);
        List<String> primaryKeys = Arrays.asList("id");
        
        return new Schema(1, fields, primaryKeys);
    }
    
    /**
     * 写入测试数据
     */
    private static void writeTestData(Schema schema, String tablePath) throws IOException {
        logger.info("写入测试数据到: {}", tablePath);
        
        PathFactory pathFactory = new PathFactory(tablePath);
        LSMTree lsmTree = new LSMTree(schema, pathFactory, "default", "test");
        
        // 写入 1000 条数据
        for (int i = 1; i <= 1000; i++) {
            Object[] values = new Object[]{
                    i,
                    "User" + i,
                    20 + (i % 50),
                    60.0 + (i % 40)
            };
            Row row = new Row(values);
            lsmTree.put(row);
        }
        
        lsmTree.close();
        
        logger.info("写入完成: 1000 条数据");
    }
    
    /**
     * 演示优化读取
     */
    private static void demonstrateOptimizedRead(Schema schema, String tablePath) throws IOException {
        PathFactory pathFactory = new PathFactory(tablePath);
        
        logger.info("\n===== 场景 1: 全表扫描（无过滤） =====");
        fullTableScan(schema, pathFactory);
        
        logger.info("\n===== 场景 2: 主键范围查询（文件级过滤） =====");
        primaryKeyRangeQuery(schema, pathFactory);
        
        logger.info("\n===== 场景 3: 条件过滤 + 投影（多层优化） =====");
        filterWithProjection(schema, pathFactory);
        
        logger.info("\n===== 场景 4: 流式处理大数据集 =====");
        streamProcessing(schema, pathFactory);
    }
    
    /**
     * 场景 1: 全表扫描
     */
    private static void fullTableScan(Schema schema, PathFactory pathFactory) throws IOException {
        DataTableScan scan = new DataTableScan(pathFactory, "default", "test", schema);
        DataTableScan.Plan plan = scan.plan();
        
        DataTableRead read = new DataTableRead(schema);
        List<Row> rows = read.read(plan);
        
        logger.info("全表扫描结果: {} 条记录", rows.size());
        if (!rows.isEmpty()) {
            logger.info("第一条: {}", Arrays.toString(rows.get(0).getValues()));
            logger.info("最后一条: {}", Arrays.toString(rows.get(rows.size() - 1).getValues()));
        }
    }
    
    /**
     * 场景 2: 主键范围查询
     * 优化点：文件级过滤，跳过不相关的文件
     */
    private static void primaryKeyRangeQuery(Schema schema, PathFactory pathFactory) throws IOException {
        DataTableScan scan = new DataTableScan(pathFactory, "default", "test", schema);
        DataTableScan.Plan plan = scan.plan();
        
        // 查询 id >= 200 AND id <= 300
        RowKey startKey = new RowKey(java.nio.ByteBuffer.allocate(4).putInt(200).array());
        RowKey endKey = new RowKey(java.nio.ByteBuffer.allocate(4).putInt(300).array());
        
        DataTableRead read = new DataTableRead(schema)
                .withKeyRange(startKey, endKey);
        
        List<Row> rows = read.read(plan);
        
        logger.info("范围查询结果: {} 条记录", rows.size());
        if (!rows.isEmpty()) {
            logger.info("第一条 ID: {}", rows.get(0).getValues()[0]);
            logger.info("最后一条 ID: {}", rows.get(rows.size() - 1).getValues()[0]);
        }
    }
    
    /**
     * 场景 3: 条件过滤 + 投影
     * 优化点：谓词下推、投影下推、Block 级过滤
     */
    private static void filterWithProjection(Schema schema, PathFactory pathFactory) throws IOException {
        DataTableScan scan = new DataTableScan(pathFactory, "default", "test", schema);
        DataTableScan.Plan plan = scan.plan();
        
        // 过滤：age > 30
        Predicate predicate = Predicate.greaterThan("age", 30);
        
        // 投影：只选择 id, name, age 字段
        Projection projection = Projection.of("id", "name", "age");
        
        DataTableRead read = new DataTableRead(schema)
                .withFilter(predicate)
                .withProjection(projection);
        
        List<Row> rows = read.read(plan);
        
        logger.info("过滤+投影结果: {} 条记录", rows.size());
        if (!rows.isEmpty()) {
            logger.info("第一条: {}", Arrays.toString(rows.get(0).getValues()));
            logger.info("投影后字段数: {}", rows.get(0).getValues().length);
        }
    }
    
    /**
     * 场景 4: 流式处理
     * 优化点：批量读取，减少内存占用
     */
    private static void streamProcessing(Schema schema, PathFactory pathFactory) throws IOException {
        DataTableScan scan = new DataTableScan(pathFactory, "default", "test", schema);
        DataTableScan.Plan plan = scan.plan();
        
        DataTableRead read = new DataTableRead(schema);
        
        // 使用流式处理
        final int[] count = {0};
        final double[] sumScore = {0.0};
        
        read.read(plan, row -> {
            count[0]++;
            Object[] values = row.getValues();
            if (values.length >= 4 && values[3] instanceof Number) {
                sumScore[0] += ((Number) values[3]).doubleValue();
            }
        });
        
        logger.info("流式处理结果: {} 条记录", count[0]);
        logger.info("平均分数: {}", count[0] > 0 ? sumScore[0] / count[0] : 0);
    }
}
