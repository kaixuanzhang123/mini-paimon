package com.mini.paimon.table;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.RowKey;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.reader.KeyValueFileReader;
import com.mini.paimon.reader.KeyValueFileReaderFactory;
import com.mini.paimon.reader.RecordReader;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Table Read
 * 负责从数据文件中读取数据,支持投影和过滤
 * 参考 Paimon 的 DataTableRead 设计
 * 
 * 优化策略:
 * 1. 文件级过滤: 通过 minKey/maxKey 和 manifest 统计信息跳过文件
 * 2. 布隆过滤器: 快速排除不存在的键
 * 3. Block Index: 定位数据块
 * 4. 谓词和投影下推到 Reader 层
 */
public class DataTableRead {
    private static final Logger logger = LoggerFactory.getLogger(DataTableRead.class);
    
    private final Schema schema;
    private final KeyValueFileReaderFactory readerFactory;
    private final PathFactory pathFactory;
    private final String database;
    private final String table;
    
    private Projection projection;
    private Predicate predicate;
    private RowKey startKey;
    private RowKey endKey;
    
    public DataTableRead(Schema schema, PathFactory pathFactory, String database, String table) {
        this.schema = schema;
        this.readerFactory = new KeyValueFileReaderFactory(schema);
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
    }
    
    /**
     * 设置投影（选择哪些字段）
     */
    public DataTableRead withProjection(Projection projection) {
        this.projection = projection;
        return this;
    }
    
    /**
     * 设置过滤条件
     */
    public DataTableRead withFilter(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }
    
    /**
     * 设置键范围（用于范围扫描优化）
     */
    public DataTableRead withKeyRange(RowKey startKey, RowKey endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
        return this;
    }
    
    public List<Row> read(DataTableScan.Plan plan) throws IOException {
        if (plan.isEmpty()) {
            return new ArrayList<>();
        }
        
        if (schema.hasPrimaryKey()) {
            return readWithMerge(plan);
        } else {
            return readWithoutMerge(plan);
        }
    }
    
    private List<Row> readWithMerge(DataTableScan.Plan plan) throws IOException {
        List<java.util.Iterator<Row>> dataSources = new ArrayList<>();
        int readFiles = 0;
        
        // 注意: merge 时不能应用投影,因为需要主键进行排序
        // 投影在 merge 后应用
        KeyValueFileReaderFactory factory = new KeyValueFileReaderFactory(schema);
        if (predicate != null) {
            factory.withFilter(predicate);
        }
        
        for (ManifestEntry entry : plan.getDataFiles()) {
            DataFileMeta fileMeta = entry.getFile();
            String relativeFilePath = fileMeta.getFileName();
            
            Path fullFilePath = pathFactory.getTablePath(database, table)
                .resolve(relativeFilePath);
            
            readFiles++;
            logger.debug("Reading file: {} (rows={}, size={})", 
                        fullFilePath, fileMeta.getRowCount(), fileMeta.getFileSize());
            
            try (KeyValueFileReader reader = factory.createReader(fullFilePath.toString())) {
                RecordReader<Row> recordReader = startKey != null || endKey != null
                    ? reader.readRange(startKey, endKey)
                    : reader.readAll();
                
                List<Row> fileRows = new ArrayList<>();
                Row row;
                while ((row = recordReader.readRecord()) != null) {
                    fileRows.add(row);
                }
                dataSources.add(fileRows.iterator());
            }
        }
        
        List<Row> result = com.mini.paimon.storage.MergeSortedReader.readAllFromIterators(schema, dataSources);
        
        // 在 merge 后应用投影
        if (projection != null && !projection.isAll()) {
            List<Row> projectedResult = new ArrayList<>(result.size());
            for (Row row : result) {
                projectedResult.add(projection.project(row, schema));
            }
            result = projectedResult;
        }
        
        logger.info("Read completed: {} rows from {} files", result.size(), readFiles);
        
        return result;
    }
    
    private List<Row> readWithoutMerge(DataTableScan.Plan plan) throws IOException {
        List<Row> result = new ArrayList<>();
        int readFiles = 0;
        
        KeyValueFileReaderFactory factory = new KeyValueFileReaderFactory(schema);
        if (predicate != null) {
            factory.withFilter(predicate);
        }
        if (projection != null) {
            factory.withProjection(projection);
        }
        
        for (ManifestEntry entry : plan.getDataFiles()) {
            DataFileMeta fileMeta = entry.getFile();
            String relativeFilePath = fileMeta.getFileName();
            
            Path fullFilePath = pathFactory.getTablePath(database, table)
                .resolve(relativeFilePath);
            
            readFiles++;
            logger.debug("Reading file: {} (rows={}, size={})", 
                        fullFilePath, fileMeta.getRowCount(), fileMeta.getFileSize());
            
            try (KeyValueFileReader reader = factory.createReader(fullFilePath.toString())) {
                RecordReader<Row> recordReader = startKey != null || endKey != null
                    ? reader.readRange(startKey, endKey)
                    : reader.readAll();
                
                Row row;
                while ((row = recordReader.readRecord()) != null) {
                    result.add(row);
                }
            }
        }
        
        logger.info("Read completed: {} rows from {} files", result.size(), readFiles);
        
        return result;
    }
    
    /**
     * 批量读取模式（支持流式处理大数据集）
     */
    public void read(DataTableScan.Plan plan, RowConsumer consumer) throws IOException {
        if (plan.isEmpty()) {
            return;
        }
        
        KeyValueFileReaderFactory factory = new KeyValueFileReaderFactory(schema);
        if (predicate != null) {
            factory.withFilter(predicate);
        }
        if (projection != null) {
            factory.withProjection(projection);
        }
        
        for (ManifestEntry entry : plan.getDataFiles()) {
            DataFileMeta fileMeta = entry.getFile();
            String relativeFilePath = fileMeta.getFileName();
            
            Path fullFilePath = pathFactory.getTablePath(database, table)
                .resolve(relativeFilePath);
            
            try (KeyValueFileReader reader = factory.createReader(fullFilePath.toString())) {
                RecordReader<Row> recordReader = startKey != null || endKey != null
                    ? reader.readRange(startKey, endKey)
                    : reader.readAll();
                
                RecordReader.RecordBatch<Row> batch;
                while ((batch = recordReader.readBatch(1000)) != null) {
                    for (int i = 0; i < batch.size(); i++) {
                        consumer.accept(batch.get(i));
                    }
                }
            }
        }
    }
    
    /**
     * 行消费者接口（用于流式处理）
     */
    public interface RowConsumer {
        void accept(Row row) throws IOException;
    }
}
