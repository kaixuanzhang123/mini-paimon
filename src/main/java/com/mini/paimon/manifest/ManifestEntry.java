package com.mini.paimon.manifest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mini.paimon.metadata.RowKey;

import java.util.Objects;

/**
 * Manifest 条目
 * 记录数据文件的变更（添加/删除）
 */
public class ManifestEntry {
    /** 操作类型 */
    public enum FileKind {
        /** 添加文件 */
        ADD,
        /** 删除文件 */
        DELETE
    }
    
    /** 操作类型 */
    private final FileKind kind;
    
    /** 文件路径 */
    private final String file;
    
    /** 文件所在的层级 */
    private final int level;
    
    /** 文件中最小的键 */
    private final RowKey minKey;
    
    /** 文件中最大的键 */
    private final RowKey maxKey;
    
    /** 文件中的行数 */
    private final long rowCount;

    @JsonCreator
    public ManifestEntry(
            @JsonProperty("kind") FileKind kind,
            @JsonProperty("file") String file,
            @JsonProperty("level") int level,
            @JsonProperty("minKey") RowKey minKey,
            @JsonProperty("maxKey") RowKey maxKey,
            @JsonProperty("rowCount") long rowCount) {
        this.kind = Objects.requireNonNull(kind, "File kind cannot be null");
        this.file = Objects.requireNonNull(file, "File path cannot be null");
        this.level = level;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.rowCount = rowCount;
    }

    public FileKind getKind() {
        return kind;
    }

    public String getFile() {
        return file;
    }

    public int getLevel() {
        return level;
    }

    public RowKey getMinKey() {
        return minKey;
    }

    public RowKey getMaxKey() {
        return maxKey;
    }

    public long getRowCount() {
        return rowCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ManifestEntry that = (ManifestEntry) o;
        return level == that.level &&
                rowCount == that.rowCount &&
                kind == that.kind &&
                file.equals(that.file) &&
                Objects.equals(minKey, that.minKey) &&
                Objects.equals(maxKey, that.maxKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, file, level, minKey, maxKey, rowCount);
    }

    @Override
    public String toString() {
        return "ManifestEntry{" +
                "kind=" + kind +
                ", file='" + file + '\'' +
                ", level=" + level +
                ", minKey=" + minKey +
                ", maxKey=" + maxKey +
                ", rowCount=" + rowCount +
                '}';
    }
}