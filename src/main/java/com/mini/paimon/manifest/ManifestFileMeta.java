package com.mini.paimon.manifest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mini.paimon.metadata.RowKey;

import java.util.Objects;

/**
 * Manifest File Meta
 * 参考 Paimon 的 ManifestFileMeta 设计
 * 包含 Manifest 文件的元信息，用于在扫描时跳过不相关的 Manifest 文件
 */
public class ManifestFileMeta {
    /** Manifest 文件名 */
    private final String fileName;
    
    /** 文件大小 */
    private final long fileSize;
    
    /** 添加的文件数量 */
    private final long numAddedFiles;
    
    /** 删除的文件数量 */
    private final long numDeletedFiles;
    
    /** Schema ID */
    private final int schemaId;
    
    /** 统计信息：最小键 */
    private final RowKey minKey;
    
    /** 统计信息：最大键 */
    private final RowKey maxKey;

    @JsonCreator
    public ManifestFileMeta(
            @JsonProperty("fileName") String fileName,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("numAddedFiles") long numAddedFiles,
            @JsonProperty("numDeletedFiles") long numDeletedFiles,
            @JsonProperty("schemaId") int schemaId,
            @JsonProperty("minKey") RowKey minKey,
            @JsonProperty("maxKey") RowKey maxKey) {
        this.fileName = Objects.requireNonNull(fileName, "File name cannot be null");
        this.fileSize = fileSize;
        this.numAddedFiles = numAddedFiles;
        this.numDeletedFiles = numDeletedFiles;
        this.schemaId = schemaId;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getNumAddedFiles() {
        return numAddedFiles;
    }

    public long getNumDeletedFiles() {
        return numDeletedFiles;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public RowKey getMinKey() {
        return minKey;
    }

    public RowKey getMaxKey() {
        return maxKey;
    }
    
    /**
     * 检查给定的键范围是否与此 Manifest 有交集
     */
    public boolean mayContainKey(RowKey key) {
        if (minKey == null || maxKey == null || key == null) {
            return true;
        }
        return key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ManifestFileMeta that = (ManifestFileMeta) o;
        return fileSize == that.fileSize &&
                numAddedFiles == that.numAddedFiles &&
                numDeletedFiles == that.numDeletedFiles &&
                schemaId == that.schemaId &&
                fileName.equals(that.fileName) &&
                Objects.equals(minKey, that.minKey) &&
                Objects.equals(maxKey, that.maxKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, fileSize, numAddedFiles, numDeletedFiles, schemaId, minKey, maxKey);
    }

    @Override
    public String toString() {
        return "ManifestFileMeta{" +
                "fileName='" + fileName + '\'' +
                ", fileSize=" + fileSize +
                ", numAddedFiles=" + numAddedFiles +
                ", numDeletedFiles=" + numDeletedFiles +
                ", schemaId=" + schemaId +
                ", minKey=" + minKey +
                ", maxKey=" + maxKey +
                '}';
    }
}
