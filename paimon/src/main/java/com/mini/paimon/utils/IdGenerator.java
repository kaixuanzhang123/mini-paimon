package com.mini.paimon.utils;

import java.util.UUID;

/**
 * ID Generator
 * 生成全局唯一的ID，用于manifest文件等
 */
public class IdGenerator {
    
    /**
     * 生成manifest ID（使用UUID）
     */
    public String generateManifestId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
    
    /**
     * 生成snapshot ID（由SnapshotManager管理，这里不使用）
     */
    public long generateSnapshotId() {
        throw new UnsupportedOperationException("Snapshot ID should be managed by SnapshotManager");
    }
}
