package com.minipaimon.snapshot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.minipaimon.utils.PathFactory;
import com.minipaimon.utils.SerializationUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

/**
 * 快照
 * 表示表在某个时间点的状态
 */
public class Snapshot {
    /** 快照ID */
    private final long snapshotId;
    
    /** Schema ID */
    private final int schemaId;
    
    /** 提交时间 */
    private final Instant commitTime;
    
    /** Manifest List 文件路径 */
    private final String manifestList;

    @JsonCreator
    public Snapshot(
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("schemaId") int schemaId,
            @JsonProperty("commitTime") Instant commitTime,
            @JsonProperty("manifestList") String manifestList) {
        this.snapshotId = snapshotId;
        this.schemaId = schemaId;
        this.commitTime = commitTime != null ? commitTime : Instant.now();
        this.manifestList = manifestList;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public Instant getCommitTime() {
        return commitTime;
    }

    public String getManifestList() {
        return manifestList;
    }

    /**
     * 持久化快照
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @throws IOException 序列化异常
     */
    public void persist(PathFactory pathFactory, String database, String table) throws IOException {
        Path snapshotPath = pathFactory.getSnapshotPath(database, table, snapshotId);
        Files.createDirectories(snapshotPath.getParent());
        SerializationUtils.writeToFile(snapshotPath, this);
        
        // 同时更新 LATEST 指针
        updateLatestSnapshot(pathFactory, database, table);
    }

    /**
     * 更新最新快照指针
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @throws IOException IO异常
     */
    private void updateLatestSnapshot(PathFactory pathFactory, String database, String table) throws IOException {
        Path latestPath = pathFactory.getLatestSnapshotPath(database, table);
        Files.write(latestPath, String.valueOf(snapshotId).getBytes());
    }

    /**
     * 从文件加载快照
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @param snapshotId 快照ID
     * @return 快照
     * @throws IOException 反序列化异常
     */
    public static Snapshot load(PathFactory pathFactory, String database, String table, long snapshotId) throws IOException {
        Path snapshotPath = pathFactory.getSnapshotPath(database, table, snapshotId);
        if (!Files.exists(snapshotPath)) {
            throw new IOException("Snapshot file not found: " + snapshotPath);
        }
        return SerializationUtils.readFromFile(snapshotPath, Snapshot.class);
    }

    /**
     * 加载最新快照
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @return 最新快照
     * @throws IOException IO异常
     */
    public static Snapshot loadLatest(PathFactory pathFactory, String database, String table) throws IOException {
        Path latestPath = pathFactory.getLatestSnapshotPath(database, table);
        if (!Files.exists(latestPath)) {
            throw new IOException("Latest snapshot file not found: " + latestPath);
        }
        
        String latestSnapshotId = new String(Files.readAllBytes(latestPath)).trim();
        long snapshotId = Long.parseLong(latestSnapshotId);
        
        return load(pathFactory, database, table, snapshotId);
    }

    /**
     * 检查快照是否存在
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @param snapshotId 快照ID
     * @return true 如果存在，否则 false
     */
    public static boolean exists(PathFactory pathFactory, String database, String table, long snapshotId) {
        try {
            Path snapshotPath = pathFactory.getSnapshotPath(database, table, snapshotId);
            return Files.exists(snapshotPath);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 检查是否存在最新快照
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @return true 如果存在，否则 false
     */
    public static boolean hasLatestSnapshot(PathFactory pathFactory, String database, String table) {
        try {
            Path latestPath = pathFactory.getLatestSnapshotPath(database, table);
            return Files.exists(latestPath);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        Snapshot snapshot = (Snapshot) o;
        
        if (snapshotId != snapshot.snapshotId) return false;
        if (schemaId != snapshot.schemaId) return false;
        if (!commitTime.equals(snapshot.commitTime)) return false;
        return manifestList.equals(snapshot.manifestList);
    }

    @Override
    public int hashCode() {
        int result = (int) (snapshotId ^ (snapshotId >>> 32));
        result = 31 * result + schemaId;
        result = 31 * result + commitTime.hashCode();
        result = 31 * result + manifestList.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Snapshot{" +
                "snapshotId=" + snapshotId +
                ", schemaId=" + schemaId +
                ", commitTime=" + commitTime +
                ", manifestList='" + manifestList + '\'' +
                '}';
    }
}
