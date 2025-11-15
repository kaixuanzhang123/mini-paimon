package com.mini.paimon.table;

import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.partition.PartitionSpec;
import com.mini.paimon.snapshot.Snapshot;

import java.io.IOException;
import java.util.List;

/**
 * Table 扫描器
 * 用于扫描表的数据文件，生成读取计划
 * 支持分区过滤
 */
public interface TableScan {
    
    /**
     * 指定要扫描的快照
     */
    TableScan withSnapshot(long snapshotId);
    
    /**
     * 使用最新快照
     */
    TableScan withLatestSnapshot();
    
    /**
     * 过滤分区
     */
    TableScan withPartitionFilter(PartitionSpec partitionSpec);
    
    /**
     * 设置行级过滤条件
     */
    TableScan withFilter(Predicate predicate);
    
    /**
     * 执行扫描，生成读取计划
     */
    Plan plan() throws IOException;
    
    /**
     * 读取计划
     * 包含需要读取的数据文件列表
     */
    interface Plan {
        
        /**
         * 获取快照
         */
        Snapshot snapshot();
        
        /**
         * 获取需要读取的文件列表
         */
        List<ManifestEntry> files();
    }
}
