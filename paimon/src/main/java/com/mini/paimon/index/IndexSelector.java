package com.mini.paimon.index;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.table.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 索引选择器
 * 根据查询条件选择合适的索引进行文件过滤
 */
public class IndexSelector {
    
    private static final Logger logger = LoggerFactory.getLogger(IndexSelector.class);
    
    private final IndexFileManager indexFileManager;
    private final String database;
    private final String table;
    
    public IndexSelector(IndexFileManager indexFileManager, String database, String table) {
        this.indexFileManager = indexFileManager;
        this.database = database;
        this.table = table;
    }
    
    /**
     * 使用索引过滤数据文件列表
     * @param dataFiles 待过滤的数据文件列表
     * @param predicate 查询谓词
     * @return 过滤后的数据文件列表
     */
    public List<DataFileMeta> filterWithIndex(List<DataFileMeta> dataFiles, Predicate predicate) {
        if (dataFiles == null || dataFiles.isEmpty()) {
            return dataFiles;
        }
        
        if (predicate == null) {
            return dataFiles;
        }
        
        // 提取所有 FieldPredicate
        List<Predicate.FieldPredicate> fieldPredicates = extractFieldPredicates(predicate);
        
        if (fieldPredicates.isEmpty()) {
            return dataFiles;
        }
        
        List<DataFileMeta> result = new ArrayList<>();
        int totalFiles = dataFiles.size();
        int skippedFiles = 0;
        
        for (DataFileMeta fileMeta : dataFiles) {
            if (shouldIncludeFile(fileMeta, fieldPredicates)) {
                result.add(fileMeta);
            } else {
                skippedFiles++;
            }
        }
        
        logger.info("Index filtering: total={}, skipped={}, remaining={}", 
                   totalFiles, skippedFiles, result.size());
        
        return result;
    }
    
    /**
     * 从谓词中提取所有 FieldPredicate
     */
    private List<Predicate.FieldPredicate> extractFieldPredicates(Predicate predicate) {
        List<Predicate.FieldPredicate> result = new ArrayList<>();
        
        if (predicate instanceof Predicate.FieldPredicate) {
            result.add((Predicate.FieldPredicate) predicate);
        } else if (predicate instanceof Predicate.AndPredicate) {
            Predicate.AndPredicate andPred = (Predicate.AndPredicate) predicate;
            result.addAll(extractFieldPredicates(andPred.getLeft()));
            result.addAll(extractFieldPredicates(andPred.getRight()));
        } else if (predicate instanceof Predicate.OrPredicate) {
            // OR 谓词不能安全地过滤，因为任何一个条件满足就要保留文件
            return new ArrayList<>();
        }
        
        return result;
    }
    
    /**
     * 判断文件是否应该被包含（基于索引过滤）
     */
    private boolean shouldIncludeFile(DataFileMeta fileMeta, List<Predicate.FieldPredicate> predicates) {
        // 如果没有索引，保守地包含该文件
        List<IndexMeta> indexMetas = fileMeta.getIndexMetas();
        if (indexMetas == null || indexMetas.isEmpty()) {
            return true;
        }
        
        // 构建字段名到索引的映射
        Map<String, Map<IndexType, IndexMeta>> indexMap = buildIndexMap(indexMetas);
        
        // 对每个谓词进行过滤
        for (Predicate.FieldPredicate predicate : predicates) {
            if (!evaluatePredicate(fileMeta, predicate, indexMap)) {
                // 任何一个谓词判定文件不包含数据，就可以跳过该文件
                logger.debug("File {} filtered out by predicate: {} {} {}", 
                           fileMeta.getFileName(), predicate.getFieldName(), 
                           predicate.getOp(), predicate.getValue());
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 评估谓词是否可能在文件中满足
     */
    private boolean evaluatePredicate(
            DataFileMeta fileMeta, 
            Predicate.FieldPredicate predicate, 
            Map<String, Map<IndexType, IndexMeta>> indexMap) {
        
        String fieldName = predicate.getFieldName();
        
        // 如果该字段没有索引，保守地返回 true
        if (!indexMap.containsKey(fieldName)) {
            return true;
        }
        
        Map<IndexType, IndexMeta> fieldIndexes = indexMap.get(fieldName);
        
        // 根据谓词类型选择合适的索引
        switch (predicate.getOp()) {
            case EQ:
                // 等值查询：优先使用 BloomFilter，其次使用 MinMax
                return evaluateEqualsPredicate(fieldName, predicate.getValue(), fieldIndexes);
                
            case GT:
            case GE:
            case LT:
            case LE:
                // 范围查询：使用 MinMax 索引
                return evaluateRangePredicate(predicate, fieldIndexes);
                
            default:
                return true;
        }
    }
    
    /**
     * 评估等值查询谓词
     */
    private boolean evaluateEqualsPredicate(
            String fieldName, 
            Object value, 
            Map<IndexType, IndexMeta> fieldIndexes) {
        
        // 优先使用 BloomFilter
        if (fieldIndexes.containsKey(IndexType.BLOOM_FILTER)) {
            IndexMeta bloomMeta = fieldIndexes.get(IndexType.BLOOM_FILTER);
            try {
                FileIndex index = indexFileManager.loadIndex(database, table, bloomMeta);
                boolean mightContain = index.mightContain(value);
                if (!mightContain) {
                    logger.debug("BloomFilter filtered out file for {}={}", fieldName, value);
                    return false;
                }
            } catch (IOException e) {
                logger.warn("Failed to load BloomFilter index for field {}", fieldName, e);
            }
        }
        
        // 使用 MinMax 索引作为补充
        if (fieldIndexes.containsKey(IndexType.MIN_MAX)) {
            IndexMeta minMaxMeta = fieldIndexes.get(IndexType.MIN_MAX);
            try {
                FileIndex index = indexFileManager.loadIndex(database, table, minMaxMeta);
                boolean mightContain = index.mightContain(value);
                if (!mightContain) {
                    logger.debug("MinMax filtered out file for {}={}", fieldName, value);
                    return false;
                }
            } catch (IOException e) {
                logger.warn("Failed to load MinMax index for field {}", fieldName, e);
            }
        }
        
        return true;
    }
    
    /**
     * 评估范围查询谓词
     */
    private boolean evaluateRangePredicate(
            Predicate.FieldPredicate predicate, 
            Map<IndexType, IndexMeta> fieldIndexes) {
        
        if (!fieldIndexes.containsKey(IndexType.MIN_MAX)) {
            return true;
        }
        
        IndexMeta minMaxMeta = fieldIndexes.get(IndexType.MIN_MAX);
        try {
            FileIndex index = indexFileManager.loadIndex(database, table, minMaxMeta);
            
            Object min = null;
            Object max = null;
            
            switch (predicate.getOp()) {
                case GT:
                case GE:
                    min = predicate.getValue();
                    break;
                case LT:
                case LE:
                    max = predicate.getValue();
                    break;
            }
            
            boolean mightIntersect = index.mightIntersect(min, max);
            if (!mightIntersect) {
                logger.debug("MinMax filtered out file for range query: {} {} {}", 
                           predicate.getFieldName(), predicate.getOp(), predicate.getValue());
                return false;
            }
        } catch (IOException e) {
            logger.warn("Failed to load MinMax index for field {}", predicate.getFieldName(), e);
        }
        
        return true;
    }
    
    /**
     * 构建索引映射：字段名 -> (索引类型 -> 索引元数据)
     */
    private Map<String, Map<IndexType, IndexMeta>> buildIndexMap(List<IndexMeta> indexMetas) {
        Map<String, Map<IndexType, IndexMeta>> result = new HashMap<>();
        
        for (IndexMeta meta : indexMetas) {
            result.computeIfAbsent(meta.getFieldName(), k -> new HashMap<>())
                  .put(meta.getIndexType(), meta);
        }
        
        return result;
    }
}
