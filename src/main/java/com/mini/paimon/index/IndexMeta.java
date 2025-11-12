package com.mini.paimon.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * 索引元数据
 * 记录索引的基本信息，用于索引管理和查询优化
 */
public class IndexMeta {
    
    /** 索引类型 */
    private final IndexType indexType;
    
    /** 索引所属字段名 */
    private final String fieldName;
    
    /** 索引文件路径（相对于数据文件） */
    private final String indexFilePath;
    
    /** 索引文件大小 */
    private final long fileSize;
    
    /** 创建时间 */
    private final long creationTime;
    
    @JsonCreator
    public IndexMeta(
            @JsonProperty("indexType") IndexType indexType,
            @JsonProperty("fieldName") String fieldName,
            @JsonProperty("indexFilePath") String indexFilePath,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("creationTime") long creationTime) {
        this.indexType = Objects.requireNonNull(indexType, "Index type cannot be null");
        this.fieldName = Objects.requireNonNull(fieldName, "Field name cannot be null");
        this.indexFilePath = Objects.requireNonNull(indexFilePath, "Index file path cannot be null");
        this.fileSize = fileSize;
        this.creationTime = creationTime;
    }
    
    public IndexType getIndexType() {
        return indexType;
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
    public String getIndexFilePath() {
        return indexFilePath;
    }
    
    public long getFileSize() {
        return fileSize;
    }
    
    public long getCreationTime() {
        return creationTime;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexMeta indexMeta = (IndexMeta) o;
        return fileSize == indexMeta.fileSize &&
                creationTime == indexMeta.creationTime &&
                indexType == indexMeta.indexType &&
                fieldName.equals(indexMeta.fieldName) &&
                indexFilePath.equals(indexMeta.indexFilePath);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(indexType, fieldName, indexFilePath, fileSize, creationTime);
    }
    
    @Override
    public String toString() {
        return "IndexMeta{" +
                "indexType=" + indexType +
                ", fieldName='" + fieldName + '\'' +
                ", indexFilePath='" + indexFilePath + '\'' +
                ", fileSize=" + fileSize +
                ", creationTime=" + creationTime +
                '}';
    }
}
