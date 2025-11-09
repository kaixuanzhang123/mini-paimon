package com.mini.paimon.table;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.reader.FileRecordReader;
import com.mini.paimon.reader.RecordReader;
import com.mini.paimon.reader.RecordReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Table Read
 * 负责从数据文件中读取数据，支持投影和过滤
 * 参考 Paimon 的 DataTableRead 设计
 * 
 * 重构后的架构：
 * 1. 使用工业级 RecordReader 抽象
 * 2. 支持 Block 级别的懒加载和过滤
 * 3. 文件级过滤：通过 minKey/maxKey 跳过不相关的文件
 * 4. Block 级过滤：只读取包含符合条件数据的 Block
 * 5. 谓词和投影下推到 Reader 层
 */
public class DataTableRead {
    private static final Logger logger = LoggerFactory.getLogger(DataTableRead.class);
    
    private final Schema schema;
    private final RecordReaderFactory readerFactory;
    
    private Projection projection;
    private Predicate predicate;
    private RowKey startKey;
    private RowKey endKey;
    
    public DataTableRead(Schema schema) {
        this.schema = schema;
        this.readerFactory = new RecordReaderFactory(schema);
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
    
    /**
     * 读取数据文件
     * 
     * 多层优化策略：
     * 1. 文件级过滤：基于 DataFileMeta 的 minKey/maxKey 跳过整个文件
     * 2. Block 级过滤：基于 Block 的统计信息跳过不相关的 Block
     * 3. 行级过滤：通过谓词下推到 Reader 层进行行级过滤
     * 4. 投影下推：只读取和返回需要的字段
     */
    public List<Row> read(DataTableScan.Plan plan) throws IOException {
        if (plan.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<Row> result = new ArrayList<>();
        int skippedFiles = 0;
        int readFiles = 0;
        int skippedBlocks = 0;
        
        // 配置 ReaderFactory
        RecordReaderFactory factory = new RecordReaderFactory(schema);
        if (predicate != null) {
            factory.withFilter(predicate);
        }
        if (projection != null) {
            factory.withProjection(projection);
        }
        if (startKey != null || endKey != null) {
            factory.withKeyRange(startKey, endKey);
        }
        
        // 遍历所有数据文件
        for (ManifestEntry entry : plan.getDataFiles()) {
            DataFileMeta fileMeta = entry.getFile();
            String filePath = fileMeta.getFileName();
            
            // 优化 1：文件级过滤 - 检查文件是否可能包含符合条件的数据
            if (canSkipFile(fileMeta)) {
                skippedFiles++;
                logger.debug("Skipped file: {} (minKey={}, maxKey={})", 
                            filePath, fileMeta.getMinKey(), fileMeta.getMaxKey());
                continue;
            }
            
            readFiles++;
            logger.debug("Reading file: {} (rows={}, size={})", 
                        filePath, fileMeta.getRowCount(), fileMeta.getFileSize());
            
            // 优化 2：使用 RecordReader 进行 Block 级别的优化读取
            // RecordReader 内部会：
            // - 懒加载 Block（只读取需要的 Block）
            // - 应用 Block 级过滤（跳过不相关的 Block）
            // - 应用谓词和投影下推
            try (FileRecordReader reader = factory.createFileReader(filePath)) {
                Row row;
                while ((row = reader.readRecord()) != null) {
                    result.add(row);
                }
            }
        }
        
        logger.info("Read completed: {} rows from {} files (skipped {} files)", 
                   result.size(), readFiles, skippedFiles);
        
        return result;
    }
    
    /**
     * 批量读取模式（支持流式处理大数据集）
     */
    public void read(DataTableScan.Plan plan, RowConsumer consumer) throws IOException {
        if (plan.isEmpty()) {
            return;
        }
        
        RecordReaderFactory factory = new RecordReaderFactory(schema);
        if (predicate != null) {
            factory.withFilter(predicate);
        }
        if (projection != null) {
            factory.withProjection(projection);
        }
        if (startKey != null || endKey != null) {
            factory.withKeyRange(startKey, endKey);
        }
        
        for (ManifestEntry entry : plan.getDataFiles()) {
            DataFileMeta fileMeta = entry.getFile();
            
            if (canSkipFile(fileMeta)) {
                continue;
            }
            
            try (FileRecordReader reader = factory.createFileReader(fileMeta.getFileName())) {
                // 批量读取以提高性能
                RecordReader.RecordBatch<Row> batch;
                while ((batch = reader.readBatch(1000)) != null) {
                    for (int i = 0; i < batch.size(); i++) {
                        consumer.accept(batch.get(i));
                    }
                }
            }
        }
    }
    
    /**
     * 检查是否可以跳过此文件
     * 
     * 优化策略：
     * 1. 基于键范围的过滤（startKey/endKey）
     * 2. 基于谓词的文件级过滤（主键字段）
     * 3. 基于文件统计信息的过滤（未来可扩展）
     */
    private boolean canSkipFile(DataFileMeta fileMeta) {
        // 如果没有统计信息，保守地不跳过
        if (fileMeta.getMinKey() == null || fileMeta.getMaxKey() == null) {
            return false;
        }
        
        // 优化 1：键范围过滤
        if (startKey != null || endKey != null) {
            if (!fileMeta.mayContainRange(startKey, endKey)) {
                return true;
            }
        }
        
        // 优化 2：谓词过滤（针对主键字段）
        if (predicate != null && schema.hasPrimaryKey()) {
            // 尝试从谓词中提取键范围
            KeyRange keyRange = extractKeyRange(predicate);
            if (keyRange != null && !fileMeta.mayContainRange(keyRange.start, keyRange.end)) {
                return true;
            }
        }
        
        // TODO: 优化 3 - 基于列统计信息的过滤
        // 例如：WHERE age > 30，可以检查文件的 maxAge 统计
        
        return false;
    }
    
    /**
     * 从谓词中提取键范围（简化实现）
     */
    private KeyRange extractKeyRange(Predicate predicate) {
        if (!(predicate instanceof Predicate.FieldPredicate)) {
            return null;
        }
        
        Predicate.FieldPredicate fieldPred = (Predicate.FieldPredicate) predicate;
        
        // 只处理主键字段
        if (!isPrimaryKeyField(fieldPred.getFieldName())) {
            return null;
        }
        
        RowKey key = createRowKey(fieldPred.getValue());
        if (key == null) {
            return null;
        }
        
        // 根据操作符确定范围
        switch (fieldPred.getOp()) {
            case EQ:
                return new KeyRange(key, key);
            case GT:
            case GE:
                return new KeyRange(key, null);
            case LT:
            case LE:
                return new KeyRange(null, key);
            default:
                return null;
        }
    }
    
    /**
     * 检查字段是否为主键字段
     */
    private boolean isPrimaryKeyField(String fieldName) {
        if (!schema.hasPrimaryKey()) {
            return false;
        }
        for (String pkField : schema.getPrimaryKeys()) {
            if (pkField.equals(fieldName)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 从值创建 RowKey
     * TODO: 完整实现应该支持复合主键
     */
    private RowKey createRowKey(Object value) {
        if (value instanceof Integer) {
            return new RowKey(java.nio.ByteBuffer.allocate(4).putInt((Integer) value).array());
        } else if (value instanceof String) {
            return new RowKey(((String) value).getBytes());
        } else if (value instanceof Long) {
            return new RowKey(java.nio.ByteBuffer.allocate(8).putLong((Long) value).array());
        }
        return null;
    }
    
    /**
     * 键范围
     */
    private static class KeyRange {
        final RowKey start;
        final RowKey end;
        
        KeyRange(RowKey start, RowKey end) {
            this.start = start;
            this.end = end;
        }
    }
    
    /**
     * 行消费者接口（用于流式处理）
     */
    public interface RowConsumer {
        void accept(Row row) throws IOException;
    }
}
