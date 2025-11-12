package com.mini.paimon.manifest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mini.paimon.metadata.RowKey;

import java.util.Objects;

/**
 * Manifest Entry
 * 参考 Paimon 的 ManifestEntry 设计
 * 记录数据文件的变更（添加/删除）
 */
@JsonIgnoreProperties(ignoreUnknown = true)
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
    
    /** Bucket ID */
    private final int bucket;
    
    /** 数据文件元信息 */
    private final DataFileMeta file;

    @JsonCreator
    public ManifestEntry(
            @JsonProperty("kind") FileKind kind,
            @JsonProperty("bucket") int bucket,
            @JsonProperty("file") DataFileMeta file) {
        this.kind = Objects.requireNonNull(kind, "File kind cannot be null");
        this.bucket = bucket;
        this.file = Objects.requireNonNull(file, "Data file meta cannot be null");
    }

    public FileKind getKind() {
        return kind;
    }

    public int getBucket() {
        return bucket;
    }

    public DataFileMeta getFile() {
        return file;
    }
    
    // 为了向后兼容，保留一些便捷方法
    
    public String getFileName() {
        return file.getFileName();
    }
    
    public int getLevel() {
        return file.getLevel();
    }
    
    public RowKey getMinKey() {
        return file.getMinKey();
    }
    
    public RowKey getMaxKey() {
        return file.getMaxKey();
    }
    
    public long getRowCount() {
        return file.getRowCount();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ManifestEntry that = (ManifestEntry) o;
        return bucket == that.bucket &&
                kind == that.kind &&
                file.equals(that.file);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, bucket, file);
    }

    @Override
    public String toString() {
        return "ManifestEntry{" +
                "kind=" + kind +
                ", bucket=" + bucket +
                ", file=" + file +
                '}';
    }
    
    /**
     * 创建 ADD 类型的 ManifestEntry
     * 便捷方法，简化创建过程
     */
    public static ManifestEntry addFile(
            String fileName,
            long fileSize,
            long rowCount,
            RowKey minKey,
            RowKey maxKey,
            int schemaId,
            int level) {
        DataFileMeta fileMeta = new DataFileMeta(
            fileName, fileSize, rowCount, minKey, maxKey,
            schemaId, level, System.currentTimeMillis()
        );
        return new ManifestEntry(FileKind.ADD, 0, fileMeta);
    }
    
    /**
     * 创建 DELETE 类型的 ManifestEntry
     * 用于 OVERWRITE 模式下标记旧文件为已删除
     */
    public static ManifestEntry deleteFile(
            String fileName,
            long fileSize,
            int schemaId,
            RowKey minKey,
            RowKey maxKey,
            long rowCount,
            int level) {
        DataFileMeta fileMeta = new DataFileMeta(
            fileName, fileSize, rowCount, minKey, maxKey,
            schemaId, level, System.currentTimeMillis()
        );
        return new ManifestEntry(FileKind.DELETE, 0, fileMeta);
    }
}