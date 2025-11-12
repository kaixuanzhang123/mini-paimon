package com.mini.paimon.filter;

import com.mini.paimon.partition.PartitionSpec;
import com.mini.paimon.partition.PartitionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 分区过滤器
 * 基于谓词过滤分区，支持谓词下推优化
 * 参考 Paimon PartitionFilter 实现
 */
public class PartitionFilter {
    private static final Logger logger = LoggerFactory.getLogger(PartitionFilter.class);
    
    private final PartitionManager partitionManager;
    private final PartitionPredicate predicate;
    
    public PartitionFilter(PartitionManager partitionManager, PartitionPredicate predicate) {
        this.partitionManager = partitionManager;
        this.predicate = predicate;
    }
    
    /**
     * 执行分区过滤
     * 返回满足谓词条件的所有分区
     */
    public List<PartitionSpec> filter() throws IOException {
        // 获取所有分区
        List<PartitionSpec> allPartitions = partitionManager.listPartitions();
        
        if (allPartitions.isEmpty()) {
            logger.debug("No partitions found");
            return Collections.emptyList();
        }
        
        logger.debug("Filtering {} partitions with predicate: {}", allPartitions.size(), predicate);
        
        // 应用谓词过滤
        List<PartitionSpec> filtered = allPartitions.stream()
            .filter(predicate::test)
            .collect(Collectors.toList());
        
        logger.info("Partition filter: {} partitions matched out of {}", 
                   filtered.size(), allPartitions.size());
        
        return filtered;
    }
    
    /**
     * 检查是否可以完全跳过分区扫描
     */
    public boolean canSkipAllPartitions() throws IOException {
        // 如果谓词始终为假，可以跳过所有分区
        if (predicate instanceof PartitionPredicate.AlwaysFalsePredicate) {
            return true;
        }
        
        return false;
    }
    
    /**
     * 检查是否需要扫描所有分区
     */
    public boolean needsScanAllPartitions() {
        // 如果谓词始终为真，需要扫描所有分区
        return predicate instanceof PartitionPredicate.AlwaysTruePredicate;
    }
    
    /**
     * 静态工厂方法：从简单的分区规范创建等值过滤器
     */
    public static PartitionFilter fromPartitionSpec(PartitionManager manager, PartitionSpec spec) {
        if (spec == null || spec.isEmpty()) {
            return new PartitionFilter(manager, PartitionPredicate.alwaysTrue());
        }
        
        // 将分区规范转换为 AND 组合的等值谓词
        PartitionPredicate predicate = null;
        for (Map.Entry<String, String> entry : spec.getPartitionValues().entrySet()) {
            PartitionPredicate eqPredicate = PartitionPredicate.equal(entry.getKey(), entry.getValue());
            predicate = (predicate == null) ? eqPredicate : predicate.and(eqPredicate);
        }
        
        return new PartitionFilter(manager, predicate);
    }
}
