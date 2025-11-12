package com.mini.paimon.index;

import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 批量索引构建器
 * 在写入 SSTable 时同步构建索引
 */
public class BatchIndexBuilder {
    
    private static final Logger logger = LoggerFactory.getLogger(BatchIndexBuilder.class);
    
    /** Schema 定义 */
    private final Schema schema;
    
    /** 字段索引映射：fieldName -> FileIndex */
    private final Map<String, List<FileIndex>> fieldIndexMap;
    
    /**
     * 构造函数
     * @param schema Schema 定义
     * @param indexConfig 索引配置（字段名 -> 索引类型列表）
     */
    public BatchIndexBuilder(Schema schema, Map<String, List<IndexType>> indexConfig) {
        this.schema = schema;
        this.fieldIndexMap = new HashMap<>();
        
        // 根据配置创建索引
        for (Map.Entry<String, List<IndexType>> entry : indexConfig.entrySet()) {
            String fieldName = entry.getKey();
            List<IndexType> indexTypes = entry.getValue();
            
            List<FileIndex> indexes = new ArrayList<>();
            for (IndexType indexType : indexTypes) {
                FileIndex index = createIndexForField(fieldName, indexType);
                if (index != null) {
                    indexes.add(index);
                }
            }
            
            if (!indexes.isEmpty()) {
                fieldIndexMap.put(fieldName, indexes);
            }
        }
        
        logger.info("Initialized BatchIndexBuilder with {} indexed fields", fieldIndexMap.size());
    }
    
    /**
     * 为所有字段创建默认索引（BloomFilter + MinMax）
     */
    public static BatchIndexBuilder createDefault(Schema schema) {
        Map<String, List<IndexType>> indexConfig = new HashMap<>();
        
        // 为所有非主键字段创建索引
        for (int i = 0; i < schema.getFields().size(); i++) {
            String fieldName = schema.getFields().get(i).getName();
            List<IndexType> types = Arrays.asList(IndexType.BLOOM_FILTER, IndexType.MIN_MAX);
            indexConfig.put(fieldName, types);
        }
        
        return new BatchIndexBuilder(schema, indexConfig);
    }
    
    /**
     * 为指定字段创建索引
     */
    private FileIndex createIndexForField(String fieldName, IndexType indexType) {
        // 检查字段是否存在
        int fieldIndex = schema.getFieldIndex(fieldName);
        if (fieldIndex < 0) {
            logger.warn("Field {} not found in schema, skipping index creation", fieldName);
            return null;
        }
        
        // 为 BloomFilter 索引估算元素数量
        if (indexType == IndexType.BLOOM_FILTER) {
            // 使用默认估算值
            return new BloomFilterIndex(fieldName, 10000);
        } else {
            return IndexFactory.createIndex(indexType, fieldName);
        }
    }
    
    /**
     * 向索引中添加一行数据
     */
    public void addRow(RowKey key, Row row) {
        if (row == null) {
            return;
        }
        
        Object[] values = row.getValues();
        
        // 遍历所有索引字段
        for (Map.Entry<String, List<FileIndex>> entry : fieldIndexMap.entrySet()) {
            String fieldName = entry.getKey();
            int fieldIndex = schema.getFieldIndex(fieldName);
            
            if (fieldIndex >= 0 && fieldIndex < values.length) {
                Object value = values[fieldIndex];
                
                // 向该字段的所有索引添加值
                for (FileIndex index : entry.getValue()) {
                    try {
                        index.add(value);
                    } catch (Exception e) {
                        logger.error("Failed to add value to index: field={}, indexType={}", 
                                   fieldName, index.getIndexType(), e);
                    }
                }
            }
        }
    }
    
    /**
     * 批量添加多行数据
     */
    public void addRows(Map<RowKey, Row> entries) {
        for (Map.Entry<RowKey, Row> entry : entries.entrySet()) {
            addRow(entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * 获取所有构建好的索引
     * @return 字段名 -> 索引列表
     */
    public Map<String, List<FileIndex>> getIndexes() {
        return Collections.unmodifiableMap(fieldIndexMap);
    }
    
    /**
     * 获取指定字段的索引
     */
    public List<FileIndex> getFieldIndexes(String fieldName) {
        return fieldIndexMap.getOrDefault(fieldName, Collections.emptyList());
    }
    
    /**
     * 获取所有索引的总内存占用
     */
    public long getTotalMemorySize() {
        long total = 0;
        for (List<FileIndex> indexes : fieldIndexMap.values()) {
            for (FileIndex index : indexes) {
                total += index.getMemorySize();
            }
        }
        return total;
    }
    
    /**
     * 获取索引统计信息
     */
    public Map<String, String> getStatistics() {
        Map<String, String> stats = new HashMap<>();
        
        for (Map.Entry<String, List<FileIndex>> entry : fieldIndexMap.entrySet()) {
            String fieldName = entry.getKey();
            
            for (FileIndex index : entry.getValue()) {
                String key = fieldName + "_" + index.getIndexType();
                
                if (index instanceof BloomFilterIndex) {
                    BloomFilterIndex bfi = (BloomFilterIndex) index;
                    stats.put(key, String.format("elements=%d, bits=%d, fpp=%.4f", 
                             bfi.getElementCount(), bfi.getBitSize(), bfi.expectedFpp()));
                } else if (index instanceof MinMaxIndex) {
                    MinMaxIndex mmi = (MinMaxIndex) index;
                    stats.put(key, String.format("min=%s, max=%s, elements=%d", 
                             mmi.getMinValue(), mmi.getMaxValue(), mmi.getElementCount()));
                }
            }
        }
        
        return stats;
    }
}
