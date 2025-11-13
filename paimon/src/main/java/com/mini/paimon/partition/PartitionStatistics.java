package com.mini.paimon.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.utils.SerializationUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;

public class PartitionStatistics {
    
    private final PartitionSpec partitionSpec;
    private final long rowCount;
    private final long fileSize;
    private final long fileCount;
    private final Instant createTime;
    private final Instant lastModifiedTime;
    
    @JsonCreator
    public PartitionStatistics(
            @JsonProperty("partitionSpec") PartitionSpec partitionSpec,
            @JsonProperty("rowCount") long rowCount,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileCount") long fileCount,
            @JsonProperty("createTime") Instant createTime,
            @JsonProperty("lastModifiedTime") Instant lastModifiedTime) {
        this.partitionSpec = partitionSpec;
        this.rowCount = rowCount;
        this.fileSize = fileSize;
        this.fileCount = fileCount;
        this.createTime = createTime;
        this.lastModifiedTime = lastModifiedTime;
    }
    
    public PartitionSpec getPartitionSpec() {
        return partitionSpec;
    }
    
    public long getRowCount() {
        return rowCount;
    }
    
    public long getFileSize() {
        return fileSize;
    }
    
    public long getFileCount() {
        return fileCount;
    }
    
    public Instant getCreateTime() {
        return createTime;
    }
    
    public Instant getLastModifiedTime() {
        return lastModifiedTime;
    }
    
    public PartitionStatistics merge(PartitionStatistics other) {
        if (!this.partitionSpec.equals(other.partitionSpec)) {
            throw new IllegalArgumentException("Cannot merge statistics for different partitions");
        }
        
        return new PartitionStatistics(
            partitionSpec,
            this.rowCount + other.rowCount,
            this.fileSize + other.fileSize,
            this.fileCount + other.fileCount,
            this.createTime.isBefore(other.createTime) ? this.createTime : other.createTime,
            Instant.now()
        );
    }
    
    public void persist(PathFactory pathFactory, String database, String table) throws IOException {
        Path partitionDir = pathFactory.getTablePath(database, table)
            .resolve(partitionSpec.toPath());
        Files.createDirectories(partitionDir);
        
        Path statsPath = partitionDir.resolve("_statistics");
        SerializationUtils.writeToFile(statsPath, this);
    }
    
    public static PartitionStatistics load(PathFactory pathFactory, String database, String table, 
                                          PartitionSpec partitionSpec) throws IOException {
        Path statsPath = pathFactory.getTablePath(database, table)
            .resolve(partitionSpec.toPath())
            .resolve("_statistics");
        
        if (!Files.exists(statsPath)) {
            return new PartitionStatistics(partitionSpec, 0, 0, 0, Instant.now(), Instant.now());
        }
        
        return SerializationUtils.readFromFile(statsPath, PartitionStatistics.class);
    }
    
    public static Builder builder(PartitionSpec partitionSpec) {
        return new Builder(partitionSpec);
    }
    
    public static class Builder {
        private final PartitionSpec partitionSpec;
        private long rowCount = 0;
        private long fileSize = 0;
        private long fileCount = 0;
        private Instant createTime = Instant.now();
        private Instant lastModifiedTime = Instant.now();
        
        public Builder(PartitionSpec partitionSpec) {
            this.partitionSpec = partitionSpec;
        }
        
        public Builder rowCount(long rowCount) {
            this.rowCount = rowCount;
            return this;
        }
        
        public Builder fileSize(long fileSize) {
            this.fileSize = fileSize;
            return this;
        }
        
        public Builder fileCount(long fileCount) {
            this.fileCount = fileCount;
            return this;
        }
        
        public Builder createTime(Instant createTime) {
            this.createTime = createTime;
            return this;
        }
        
        public Builder lastModifiedTime(Instant lastModifiedTime) {
            this.lastModifiedTime = lastModifiedTime;
            return this;
        }
        
        public PartitionStatistics build() {
            return new PartitionStatistics(
                partitionSpec, rowCount, fileSize, fileCount, 
                createTime, lastModifiedTime
            );
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionStatistics)) return false;
        PartitionStatistics that = (PartitionStatistics) o;
        return rowCount == that.rowCount &&
               fileSize == that.fileSize &&
               fileCount == that.fileCount &&
               partitionSpec.equals(that.partitionSpec);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(partitionSpec, rowCount, fileSize, fileCount);
    }
    
    @Override
    public String toString() {
        return "PartitionStatistics{" +
               "partitionSpec=" + partitionSpec +
               ", rowCount=" + rowCount +
               ", fileSize=" + fileSize +
               ", fileCount=" + fileCount +
               ", createTime=" + createTime +
               ", lastModifiedTime=" + lastModifiedTime +
               '}';
    }
}

