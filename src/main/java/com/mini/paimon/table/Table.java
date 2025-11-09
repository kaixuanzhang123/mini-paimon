package com.mini.paimon.table;

import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Table 接口
 * 参考 Paimon FileStoreTable 设计，提供文件 IO 操作接口
 * 通过工厂方法创建 Read、Write、Commit 等组件
 */
public interface Table {
    
    /**
     * 获取表标识符
     */
    Identifier identifier();
    
    /**
     * 获取表 Schema
     */
    Schema schema();
    
    /**
     * 创建表扫描器
     * 用于读取数据
     */
    TableScan newScan();
    
    /**
     * 创建表读取器
     * 用于读取数据文件
     */
    TableRead newRead();
    
    /**
     * 创建表写入器
     * 用于批量写入数据
     */
    TableWrite newWrite();
    
    /**
     * 创建表提交器
     * 用于提交快照
     */
    TableCommit newCommit();
    
    /**
     * 获取最新快照
     */
    Optional<Snapshot> latestSnapshot();
    
    /**
     * 获取指定快照
     */
    Optional<Snapshot> snapshot(long snapshotId);
    
    /**
     * 列出所有快照
     */
    List<Snapshot> snapshots() throws IOException;
    
    /**
     * 获取 SnapshotManager
     */
    SnapshotManager snapshotManager();
    
    /**
     * 获取 PathFactory
     */
    PathFactory pathFactory();
}
