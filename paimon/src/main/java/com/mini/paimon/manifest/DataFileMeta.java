package com.mini.paimon.manifest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mini.paimon.index.IndexMeta;
import com.mini.paimon.schema.RowKey;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Data File Meta
 * 参考 Paimon 的数据文件元信息设计
 * 包含数据文件的详细元信息，用于文件级别的过滤和跳过
 */
public class DataFileMeta implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    
    /** 文件名 */
    private final String fileName;
    
    /** 文件大小 */
    private final long fileSize;
    
    /** 行数 */
    private final long rowCount;
    
    /** 最小键 */
    private final RowKey minKey;
    
    /** 最大键 */
    private final RowKey maxKey;
    
    /** Schema ID */
    private final int schemaId;
    
    /** LSM 层级 */
    private final int level;
    
    /** 创建时间 */
    private final long creationTime;
    
    /** 索引元信息列表 */
    private final List<IndexMeta> indexMetas;

    @JsonCreator
    public DataFileMeta(
            @JsonProperty("fileName") String fileName,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("rowCount") long rowCount,
            @JsonProperty("minKey") RowKey minKey,
            @JsonProperty("maxKey") RowKey maxKey,
            @JsonProperty("schemaId") int schemaId,
            @JsonProperty("level") int level,
            @JsonProperty("creationTime") long creationTime,
            @JsonProperty("indexMetas") List<IndexMeta> indexMetas) {
        this.fileName = Objects.requireNonNull(fileName, "File name cannot be null");
        this.fileSize = fileSize;
        this.rowCount = rowCount;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.schemaId = schemaId;
        this.level = level;
        this.creationTime = creationTime;
        this.indexMetas = indexMetas != null ? new ArrayList<>(indexMetas) : new ArrayList<>();
    }
    
    /**
     * 构造函数（不包含索引）
     */
    public DataFileMeta(
            String fileName,
            long fileSize,
            long rowCount,
            RowKey minKey,
            RowKey maxKey,
            int schemaId,
            int level,
            long creationTime) {
        this(fileName, fileSize, rowCount, minKey, maxKey, schemaId, level, creationTime, null);
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getRowCount() {
        return rowCount;
    }

    public RowKey getMinKey() {
        return minKey;
    }

    public RowKey getMaxKey() {
        return maxKey;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public int getLevel() {
        return level;
    }

    public long getCreationTime() {
        return creationTime;
    }
    
    public List<IndexMeta> getIndexMetas() {
        return Collections.unmodifiableList(indexMetas);
    }
    
    /**
     * 检查给定的键是否可能在此文件中
     * 用于点查询优化
     */
    public boolean mayContainKey(RowKey key) {
        if (minKey == null || maxKey == null || key == null) {
            return true;
        }
        return key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0;
    }
    
    /**
     * 检查给定的键范围是否与此文件有交集
     * 用于范围查询优化
     */
    public boolean mayContainRange(RowKey startKey, RowKey endKey) {
        if (minKey == null || maxKey == null) {
            return true;
        }
        
        if (startKey != null && startKey.compareTo(maxKey) > 0) {
            return false;
        }
        
        if (endKey != null && endKey.compareTo(minKey) < 0) {
            return false;
        }
        
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFileMeta that = (DataFileMeta) o;
        return fileSize == that.fileSize &&
                rowCount == that.rowCount &&
                schemaId == that.schemaId &&
                level == that.level &&
                creationTime == that.creationTime &&
                fileName.equals(that.fileName) &&
                Objects.equals(minKey, that.minKey) &&
                Objects.equals(maxKey, that.maxKey) &&
                Objects.equals(indexMetas, that.indexMetas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, fileSize, rowCount, minKey, maxKey, schemaId, level, creationTime, indexMetas);
    }

    @Override
    public String toString() {
        return "DataFileMeta{" +
                "fileName='" + fileName + '\'' +
                ", fileSize=" + fileSize +
                ", rowCount=" + rowCount +
                ", minKey=" + minKey +
                ", maxKey=" + maxKey +
                ", schemaId=" + schemaId +
                ", level=" + level +
                ", creationTime=" + creationTime +
                ", indexCount=" + indexMetas.size() +
                '}';
    }
}
