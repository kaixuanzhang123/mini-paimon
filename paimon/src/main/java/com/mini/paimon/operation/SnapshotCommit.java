package com.mini.paimon.operation;

import com.mini.paimon.snapshot.Snapshot;

/**
 * Snapshot Commit接口
 * 参考Apache Paimon的SnapshotCommit设计
 * 定义原子提交snapshot的接口
 */
public interface SnapshotCommit {
    
    /**
     * 原子性提交snapshot
     * 
     * @param snapshot 要提交的snapshot
     * @param branch 分支名
     * @return true 如果成功提交，false 如果snapshot ID已被占用
     * @throws Exception 提交异常
     */
    boolean commit(Snapshot snapshot, String branch) throws Exception;
    
    /**
     * 过期旧的snapshots
     * 
     * @param expireMillis 保留时间（毫秒）
     * @return 删除的snapshot数量
     */
    default int expireSnapshots(long expireMillis) {
        return 0;
    }
}
