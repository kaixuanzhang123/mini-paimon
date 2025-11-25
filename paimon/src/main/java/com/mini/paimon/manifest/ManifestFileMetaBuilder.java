package com.mini.paimon.manifest;

import com.mini.paimon.schema.RowKey;

import java.util.List;

/**
 * Manifest File Meta Builder
 * 辅助类，用于构建ManifestFileMeta
 */
public class ManifestFileMetaBuilder {
    
    /**
     * 从ManifestEntry列表构建ManifestFileMeta
     * 
     * @param fileName manifest文件名
     * @param entries manifest entries
     * @param schemaId schema ID
     * @return ManifestFileMeta对象
     */
    public static ManifestFileMeta build(String fileName, List<ManifestEntry> entries, int schemaId) {
        long fileSize = 0;
        long numAddedFiles = 0;
        long numDeletedFiles = 0;
        RowKey minKey = null;
        RowKey maxKey = null;
        
        for (ManifestEntry entry : entries) {
            fileSize += entry.getFile().getFileSize();
            
            if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                numAddedFiles++;
            } else {
                numDeletedFiles++;
            }
            
            // 更新最小/最大键
            if (entry.getMinKey() != null) {
                if (minKey == null || entry.getMinKey().compareTo(minKey) < 0) {
                    minKey = entry.getMinKey();
                }
            }
            if (entry.getMaxKey() != null) {
                if (maxKey == null || entry.getMaxKey().compareTo(maxKey) > 0) {
                    maxKey = entry.getMaxKey();
                }
            }
        }
        
        return new ManifestFileMeta(
            fileName,
            fileSize,
            numAddedFiles,
            numDeletedFiles,
            schemaId,
            minKey,
            maxKey
        );
    }
    
    /**
     * 创建空的ManifestFileMeta
     */
    public static ManifestFileMeta empty(String fileName, int schemaId) {
        return new ManifestFileMeta(fileName, 0, 0, 0, schemaId, null, null);
    }
}

