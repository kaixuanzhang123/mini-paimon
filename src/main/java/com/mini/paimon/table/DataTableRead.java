package com.mini.paimon.table;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.storage.SSTableReader;
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
 * 优化策略：
 * 1. 文件级过滤：通过 minKey/maxKey 跳过不相关的文件
 * 2. 谓词下推：在读取数据前应用过滤条件
 * 3. 投影下推：只读取需要的字段（当前简化实现）
 */
public class DataTableRead {
    private static final Logger logger = LoggerFactory.getLogger(DataTableRead.class);
    
    private final Schema schema;
    private final SSTableReader sstReader;
    
    private Projection projection;
    private Predicate predicate;
    
    public DataTableRead(Schema schema) {
        this.schema = schema;
        this.sstReader = new SSTableReader();
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
     * 读取数据文件
     * 优化：通过文件元信息跳过不相关的文件
     */
    public List<Row> read(DataTableScan.Plan plan) throws IOException {
        if (plan.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<Row> result = new ArrayList<>();
        int skippedFiles = 0;
        int readFiles = 0;
        
        // 遍历所有数据文件
        for (ManifestEntry entry : plan.getDataFiles()) {
            DataFileMeta fileMeta = entry.getFile();
            String filePath = fileMeta.getFileName();
            
            // 优化 1：文件级过滤 - 检查过滤条件是否可能命中此文件
            if (predicate != null && canSkipFile(fileMeta, predicate)) {
                skippedFiles++;
                logger.debug("Skipped file due to filter: {} (minKey={}, maxKey={})", 
                            filePath, fileMeta.getMinKey(), fileMeta.getMaxKey());
                continue;
            }
            
            readFiles++;
            logger.debug("Reading data file: {} (rows={})", filePath, fileMeta.getRowCount());
            
            // 读取文件中的所有行
            // TODO：未来可以优化为只读取符合条件的 Block
            List<Row> rows = sstReader.scan(filePath);
            
            // 应用过滤条件
            if (predicate != null) {
                rows = applyFilter(rows, predicate);
            }
            
            // 应用投影
            if (projection != null) {
                rows = applyProjection(rows, projection);
            }
            
            result.addAll(rows);
        }
        
        logger.info("Read {} rows from {} files (skipped {} files)", 
                   result.size(), readFiles, skippedFiles);
        
        return result;
    }
    
    /**
     * 检查是否可以跳过此文件
     * 基于 minKey/maxKey 进行粗粒度过滤
     */
    private boolean canSkipFile(DataFileMeta fileMeta, Predicate predicate) {
        // 如果没有 minKey/maxKey 统计信息，不能跳过
        if (fileMeta.getMinKey() == null || fileMeta.getMaxKey() == null) {
            return false;
        }
        
        // 尝试提取谓词中的范围条件
        // 如果是主键查询，可以通过 minKey/maxKey 判断
        if (predicate instanceof Predicate.FieldPredicate) {
            Predicate.FieldPredicate fieldPred = (Predicate.FieldPredicate) predicate;
            
            // 只处理主键字段的过滤
            if (schema.hasPrimaryKey() && isPrimaryKeyField(fieldPred.getFieldName())) {
                return !fileMeta.mayContainKey(createRowKey(fieldPred.getValue()));
            }
        }
        
        // 其他情况不跳过
        return false;
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
     * 简化实现，只处理单主键情况
     */
    private RowKey createRowKey(Object value) {
        // TODO：完整实现应该支持复合主键
        if (value instanceof Integer) {
            return new RowKey(java.nio.ByteBuffer.allocate(4).putInt((Integer) value).array());
        } else if (value instanceof String) {
            return new RowKey(((String) value).getBytes());
        }
        return null;
    }
    
    /**
     * 应用过滤条件
     */
    private List<Row> applyFilter(List<Row> rows, Predicate predicate) {
        List<Row> filtered = new ArrayList<>();
        for (Row row : rows) {
            if (predicate.test(row, schema)) {
                filtered.add(row);
            }
        }
        return filtered;
    }
    
    /**
     * 应用投影
     */
    private List<Row> applyProjection(List<Row> rows, Projection projection) {
        List<Row> projected = new ArrayList<>();
        for (Row row : rows) {
            Row projectedRow = projection.project(row, schema);
            projected.add(projectedRow);
        }
        return projected;
    }
}
