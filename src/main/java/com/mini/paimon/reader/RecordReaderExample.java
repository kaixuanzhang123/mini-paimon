package com.mini.paimon.reader;

import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.table.DataTableRead;
import com.mini.paimon.table.DataTableScan;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * RecordReader 使用示例
 * 演示工业级读取器的多种优化策略
 */
public class RecordReaderExample {
    private static final Logger logger = LoggerFactory.getLogger(RecordReaderExample.class);
    
    public static void main(String[] args) {
        try {
            // 准备测试数据
            Schema schema = createTestSchema();
            PathFactory pathFactory = new PathFactory("./warehouse");
            
            // 场景 1: 全表扫描
            fullTableScan(schema, pathFactory);
            
            // 场景 2: 主键范围查询
            primaryKeyRangeQuery(schema, pathFactory);
            
            // 场景 3: 条件过滤 + 投影
            filterWithProjection(schema, pathFactory);
            
            // 场景 4: 流式处理
            streamProcessing(schema, pathFactory);
            
        } catch (Exception e) {
            logger.error("执行示例时出错", e);
        }
    }
    
    /**
     * 创建测试 Schema
     */
    private static Schema createTestSchema() {
        return new Schema(
            0,
            Arrays.asList(
                new Field("id", DataType.INT, false),
                new Field("name", DataType.STRING, false),
                new Field("age", DataType.INT, true),
                new Field("score", DataType.DOUBLE, true)
            ),
            Collections.singletonList("id")
        );
    }
    
    /**
     * 场景 1: 全表扫描
     * 优化点：自动跳过无效文件、Block 级懒加载
     */
    private static void fullTableScan(Schema schema, PathFactory pathFactory) throws IOException {
        DataTableScan scan = new DataTableScan(pathFactory, "default", "test", schema);
        DataTableScan.Plan plan = scan.plan();
        
        // 修复：正确初始化DataTableRead，传递所有必需的参数
        DataTableRead read = new DataTableRead(schema, pathFactory, "default", "test");
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
        
        // 修复：正确初始化DataTableRead，传递所有必需的参数
        DataTableRead read = new DataTableRead(schema, pathFactory, "default", "test")
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
        
        // 修复：正确初始化DataTableRead，传递所有必需的参数
        DataTableRead read = new DataTableRead(schema, pathFactory, "default", "test")
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
        
        // 修复：正确初始化DataTableRead，传递所有必需的参数
        DataTableRead read = new DataTableRead(schema, pathFactory, "default", "test");
        
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