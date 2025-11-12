package com.mini.paimon.table;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.reader.FileRecordReader;
import com.mini.paimon.reader.RecordReader;
import com.mini.paimon.reader.RecordReaderFactory;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
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
    private final PathFactory pathFactory;
    private final String database;
    private final String table;
    
    private Projection projection;
    private Predicate predicate;
    private RowKey startKey;
    private RowKey endKey;
    
    public DataTableRead(Schema schema, PathFactory pathFactory, String database, String table) {
        this.schema = schema;
        this.readerFactory = new RecordReaderFactory(schema);
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
        int skippedFiles = 0;
        
        RecordReaderFactory factory = new RecordReaderFactory(schema);
        if (predicate != null) {
            factory.withFilter(predicate);
        }
        if (startKey != null || endKey != null) {
            factory.withKeyRange(startKey, endKey);
        }
        
        for (ManifestEntry entry : plan.getDataFiles()) {
            DataFileMeta fileMeta = entry.getFile();
            String relativeFilePath = fileMeta.getFileName();
            
            Path fullFilePath = pathFactory.getTablePath(database, table)
                .resolve(relativeFilePath);
            
            if (canSkipFile(fileMeta)) {
                skippedFiles++;
                logger.debug("Skipped file: {} (minKey={}, maxKey={})", 
                            relativeFilePath, fileMeta.getMinKey(), fileMeta.getMaxKey());
                continue;
            }
            
            readFiles++;
            logger.debug("Reading file: {} (rows={}, size={})", 
                        fullFilePath, fileMeta.getRowCount(), fileMeta.getFileSize());
            
            try (FileRecordReader reader = factory.createFileReader(fullFilePath.toString())) {
                List<Row> fileRows = new ArrayList<>();
                Row row;
                while ((row = reader.readRecord()) != null) {
                    fileRows.add(row);
                }
                dataSources.add(fileRows.iterator());
            }
        }
        
        List<Row> result = com.mini.paimon.storage.MergeSortedReader.readAllFromIterators(schema, dataSources);
        
        if (projection != null && !projection.isAll()) {
            List<Row> projectedResult = new ArrayList<>(result.size());
            for (Row row : result) {
                projectedResult.add(projection.project(row, schema));
            }
            result = projectedResult;
        }
        
        logger.info("Read completed: {} rows from {} files (skipped {} files)", 
                   result.size(), readFiles, skippedFiles);
        
        return result;
    }
    
    private List<Row> readWithoutMerge(DataTableScan.Plan plan) throws IOException {
        List<Row> result = new ArrayList<>();
        int skippedFiles = 0;
        int readFiles = 0;
        
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
            String relativeFilePath = fileMeta.getFileName();
            
            Path fullFilePath = pathFactory.getTablePath(database, table)
                .resolve(relativeFilePath);
            
            if (canSkipFile(fileMeta)) {
                skippedFiles++;
                logger.debug("Skipped file: {} (minKey={}, maxKey={})", 
                            relativeFilePath, fileMeta.getMinKey(), fileMeta.getMaxKey());
                continue;
            }
            
            readFiles++;
            logger.debug("Reading file: {} (rows={}, size={})", 
                        fullFilePath, fileMeta.getRowCount(), fileMeta.getFileSize());
            
            try (FileRecordReader reader = factory.createFileReader(fullFilePath.toString())) {
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
            String relativeFilePath = fileMeta.getFileName();
            
            if (canSkipFile(fileMeta)) {
                continue;
            }
            
            // 构建完整文件路径：table目录 + 相对路径
            Path fullFilePath = pathFactory.getTablePath(database, table)
                .resolve(relativeFilePath);
            
            try (FileRecordReader reader = factory.createFileReader(fullFilePath.toString())) {
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
     * 2. 基于谓词的主键字段过滤
     * 3. 基于谓词的非主键字段过滤（通过文件统计信息）
     */
    private boolean canSkipFile(DataFileMeta fileMeta) {
        // 如果没有统计信息，保守地不跳过
        if (fileMeta.getMinKey() == null || fileMeta.getMaxKey() == null) {
            return false;
        }
        
        // 优化 1：键范围过滤
        if (startKey != null || endKey != null) {
            if (!fileMeta.mayContainRange(startKey, endKey)) {
                logger.debug("Skip file by key range: [{}, {}]", startKey, endKey);
                return true;
            }
        }
        
        // 优化 2：谓词过滤（针对主键字段）
        if (predicate != null && schema.hasPrimaryKey()) {
            KeyRange keyRange = extractKeyRangeFromPredicate(predicate);
            if (keyRange != null && !fileMeta.mayContainRange(keyRange.start, keyRange.end)) {
                logger.debug("Skip file by predicate key range: [{}, {}]", keyRange.start, keyRange.end);
                return true;
            }
        }
        
        // 优化 3：基于谓词的非主键字段过滤
        if (predicate != null) {
            if (canSkipFileByPredicate(predicate, fileMeta)) {
                logger.debug("Skip file by non-key predicate");
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * 基于谓词判断是否可以跳过文件
     * 处理非主键字段的过滤条件
     */
    private boolean canSkipFileByPredicate(Predicate predicate, DataFileMeta fileMeta) {
        if (predicate instanceof Predicate.FieldPredicate) {
            return canSkipFileByFieldPredicate((Predicate.FieldPredicate) predicate, fileMeta);
        } else if (predicate instanceof Predicate.AndPredicate) {
            Predicate.AndPredicate andPred = (Predicate.AndPredicate) predicate;
            // AND条件：任一子条件可以跳过则整体可跳过
            List<Predicate> predicates = extractAndPredicates(andPred);
            for (Predicate p : predicates) {
                if (canSkipFileByPredicate(p, fileMeta)) {
                    return true;
                }
            }
            return false;
        } else if (predicate instanceof Predicate.OrPredicate) {
            Predicate.OrPredicate orPred = (Predicate.OrPredicate) predicate;
            // OR条件：所有子条件都可跳过才能整体跳过
            List<Predicate> predicates = extractOrPredicates(orPred);
            for (Predicate p : predicates) {
                if (!canSkipFileByPredicate(p, fileMeta)) {
                    return false;
                }
            }
            return !predicates.isEmpty();
        }
        return false;
    }
    
    /**
     * 基于单个字段谓词判断是否可以跳过文件
     */
    private boolean canSkipFileByFieldPredicate(Predicate.FieldPredicate fieldPred, DataFileMeta fileMeta) {
        String fieldName = fieldPred.getFieldName();
        Object predicateValue = fieldPred.getValue();
        Predicate.CompareOp op = fieldPred.getOp();
        
        // 如果是主键字段，已经在extractKeyRangeFromPredicate中处理
        if (isPrimaryKeyField(fieldName)) {
            return false;
        }
        
        // 对于非主键字段，这里可以扩展文件级统计信息过滤
        // 目前DataFileMeta只有minKey/maxKey，未来可以扩展列级统计
        // 示例：如果文件记录了每列的min/max值，可以做范围过滤
        
        return false;
    }
    
    /**
     * 提取AND谓词的所有子条件
     */
    private List<Predicate> extractAndPredicates(Predicate.AndPredicate andPred) {
        List<Predicate> predicates = new ArrayList<>();
        collectAndPredicates(andPred, predicates);
        return predicates;
    }
    
    private void collectAndPredicates(Predicate pred, List<Predicate> result) {
        if (pred instanceof Predicate.AndPredicate) {
            Predicate.AndPredicate andPred = (Predicate.AndPredicate) pred;
            // 递归提取左右子谓词
            collectAndPredicates(andPred.getLeft(), result);
            collectAndPredicates(andPred.getRight(), result);
        } else {
            result.add(pred);
        }
    }
    
    /**
     * 提取OR谓词的所有子条件
     */
    private List<Predicate> extractOrPredicates(Predicate.OrPredicate orPred) {
        List<Predicate> predicates = new ArrayList<>();
        collectOrPredicates(orPred, predicates);
        return predicates;
    }
    
    private void collectOrPredicates(Predicate pred, List<Predicate> result) {
        if (pred instanceof Predicate.OrPredicate) {
            Predicate.OrPredicate orPred = (Predicate.OrPredicate) pred;
            // 递归提取左右子谓词
            collectOrPredicates(orPred.getLeft(), result);
            collectOrPredicates(orPred.getRight(), result);
        } else {
            result.add(pred);
        }
    }
    
    /**
     * 从谓词中提取键范围
     * 支持单字段谓词和AND组合的多字段谓词
     */
    private KeyRange extractKeyRangeFromPredicate(Predicate predicate) {
        if (predicate instanceof Predicate.FieldPredicate) {
            return extractKeyRangeFromFieldPredicate((Predicate.FieldPredicate) predicate);
        } else if (predicate instanceof Predicate.AndPredicate) {
            return extractKeyRangeFromAndPredicate((Predicate.AndPredicate) predicate);
        }
        return null;
    }
    
    /**
     * 从单字段谓词提取键范围
     */
    private KeyRange extractKeyRangeFromFieldPredicate(Predicate.FieldPredicate fieldPred) {
        // 只处理主键字段
        if (!isPrimaryKeyField(fieldPred.getFieldName())) {
            return null;
        }
        
        // 检查是否为第一个主键字段（复合主键的第一个字段）
        List<String> primaryKeys = schema.getPrimaryKeys();
        if (!primaryKeys.get(0).equals(fieldPred.getFieldName())) {
            return null;
        }
        
        RowKey key = createRowKeyFromValue(fieldPred.getValue(), fieldPred.getFieldName());
        if (key == null) {
            return null;
        }
        
        // 根据操作符确定范围
        switch (fieldPred.getOp()) {
            case EQ:
                return new KeyRange(key, key);
            case GT:
                return new KeyRange(createNextKey(key), null);
            case GE:
                return new KeyRange(key, null);
            case LT:
                return new KeyRange(null, createPrevKey(key));
            case LE:
                return new KeyRange(null, key);
            case NE:
                // NE条件无法转换为单一范围
                return null;
            default:
                return null;
        }
    }
    
    /**
     * 从AND谓词提取键范围
     * 处理形如 key >= lower AND key <= upper 的范围查询
     */
    private KeyRange extractKeyRangeFromAndPredicate(Predicate.AndPredicate andPred) {
        // 简化实现：只处理主键字段的范围条件
        // 完整实现需要遍历AND树，收集所有主键条件并合并范围
        return null;
    }
    
    /**
     * 创建下一个键（用于GT操作）
     */
    private RowKey createNextKey(RowKey key) {
        byte[] bytes = key.getBytes();
        byte[] next = new byte[bytes.length];
        System.arraycopy(bytes, 0, next, 0, bytes.length);
        
        // 简单实现：在最后一个字节+1
        for (int i = next.length - 1; i >= 0; i--) {
            if (next[i] != (byte) 0xFF) {
                next[i]++;
                break;
            }
            next[i] = 0;
        }
        return new RowKey(next);
    }
    
    /**
     * 创建前一个键（用于LT操作）
     */
    private RowKey createPrevKey(RowKey key) {
        byte[] bytes = key.getBytes();
        byte[] prev = new byte[bytes.length];
        System.arraycopy(bytes, 0, prev, 0, bytes.length);
        
        // 简单实现：在最后一个字节-1
        for (int i = prev.length - 1; i >= 0; i--) {
            if (prev[i] != 0) {
                prev[i]--;
                break;
            }
            prev[i] = (byte) 0xFF;
        }
        return new RowKey(prev);
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
     * 支持复合主键的部分键构建
     */
    private RowKey createRowKeyFromValue(Object value, String fieldName) {
        List<String> primaryKeys = schema.getPrimaryKeys();
        int fieldIndex = primaryKeys.indexOf(fieldName);
        
        if (fieldIndex == -1) {
            return null;
        }
        
        // 只支持第一个主键字段的过滤
        if (fieldIndex != 0) {
            return null;
        }
        
        return serializeValue(value, schema.getField(fieldName).getType());
    }
    
    /**
     * 序列化单个值为字节数组
     */
    private RowKey serializeValue(Object value, com.mini.paimon.metadata.DataType dataType) {
        if (value == null) {
            return null;
        }
        
        try {
            switch (dataType) {
                case INT:
                    if (value instanceof Integer) {
                        return new RowKey(java.nio.ByteBuffer.allocate(4)
                            .putInt((Integer) value).array());
                    }
                    break;
                case LONG:
                    if (value instanceof Long) {
                        return new RowKey(java.nio.ByteBuffer.allocate(8)
                            .putLong((Long) value).array());
                    }
                    break;
                case STRING:
                    if (value instanceof String) {
                        return new RowKey(((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    }
                    break;
                case DOUBLE:
                    if (value instanceof Double) {
                        return new RowKey(java.nio.ByteBuffer.allocate(8)
                            .putDouble((Double) value).array());
                    }
                    break;
                case BOOLEAN:
                    if (value instanceof Boolean) {
                        return new RowKey(new byte[]{(byte) ((Boolean) value ? 1 : 0)});
                    }
                    break;
            }
        } catch (Exception e) {
            logger.warn("Failed to serialize value: {} of type {}", value, dataType, e);
        }
        
        return null;
    }
    
    /**
     * 构建复合主键的RowKey
     * 支持多个主键字段的值拼接
     */
    private RowKey buildCompositeRowKey(java.util.Map<String, Object> keyValues) {
        List<String> primaryKeys = schema.getPrimaryKeys();
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        
        try {
            for (String pkField : primaryKeys) {
                Object value = keyValues.get(pkField);
                if (value == null) {
                    break;
                }
                
                RowKey partialKey = serializeValue(value, schema.getField(pkField).getType());
                if (partialKey == null) {
                    break;
                }
                
                bos.write(partialKey.getBytes());
            }
            
            if (bos.size() > 0) {
                return new RowKey(bos.toByteArray());
            }
        } catch (java.io.IOException e) {
            logger.warn("Failed to build composite row key", e);
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
