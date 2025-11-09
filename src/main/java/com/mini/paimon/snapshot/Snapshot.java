package com.mini.paimon.snapshot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.utils.SerializationUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;

/**
 * Snapshot
 * 参考 Apache Paimon 的 Snapshot 设计
 * 
 * 每次提交生成一个快照文件，快照版本从 1 开始且必须连续
 * Snapshot 文件为 JSON 格式，包含提交的元信息和 Manifest 引用
 * 
 * 核心字段：
 * - version: 快照文件版本
 * - id: 快照 ID（同文件名）
 * - schemaId: 对应的 Schema 版本
 * - baseManifestList: 记录从之前快照开始的所有变更
 * - deltaManifestList: 记录本次快照的新变更
 * - commitUser: 提交用户标识
 * - commitIdentifier: 事务标识符
 * - commitKind: 提交类型（APPEND/COMPACT/OVERWRITE）
 * - timeMillis: 提交时间戳
 * - totalRecordCount: 快照中所有变更的记录数
 * - deltaRecordCount: 本次快照新增变更的记录数
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Snapshot {
    /** 快照文件版本 */
    private final int version;
    
    /** 快照 ID（同文件名）*/
    private final long id;
    
    /** Schema ID */
    private final int schemaId;
    
    /** Base Manifest List 文件名（记录从之前快照开始的所有变更）*/
    private final String baseManifestList;
    
    /** Delta Manifest List 文件名（记录本次快照的新变更）*/
    private final String deltaManifestList;
    
    /** 提交用户标识（用于流式写入恢复）*/
    private final String commitUser;
    
    /** 提交标识符（事务 ID）*/
    private final long commitIdentifier;
    
    /** 提交类型 */
    private final CommitKind commitKind;
    
    /** 提交时间（毫秒时间戳）*/
    private final long timeMillis;
    
    /** 快照中所有变更的总记录数 */
    private final long totalRecordCount;
    
    /** 本次快照新增变更的记录数 */
    private final long deltaRecordCount;
    
    /**
     * 提交类型枚举
     * 参考 Paimon 的 CommitKind 设计
     */
    public enum CommitKind {
        /** 追加数据 */
        APPEND,
        
        /** 压缩 */
        COMPACT,
        
        /** 覆盖写入 */
        OVERWRITE,
        
        /** 分析/统计 */
        ANALYZE
    }

    /**
     * 完整构造函数
     */
    @JsonCreator
    public Snapshot(
            @JsonProperty("version") int version,
            @JsonProperty("id") long id,
            @JsonProperty("schemaId") int schemaId,
            @JsonProperty("baseManifestList") String baseManifestList,
            @JsonProperty("deltaManifestList") String deltaManifestList,
            @JsonProperty("commitUser") String commitUser,
            @JsonProperty("commitIdentifier") long commitIdentifier,
            @JsonProperty("commitKind") CommitKind commitKind,
            @JsonProperty("timeMillis") long timeMillis,
            @JsonProperty("totalRecordCount") long totalRecordCount,
            @JsonProperty("deltaRecordCount") long deltaRecordCount) {
        this.version = version;
        this.id = id;
        this.schemaId = schemaId;
        this.baseManifestList = baseManifestList;
        this.deltaManifestList = deltaManifestList;
        this.commitUser = commitUser;
        this.commitIdentifier = commitIdentifier;
        this.commitKind = commitKind;
        this.timeMillis = timeMillis;
        this.totalRecordCount = totalRecordCount;
        this.deltaRecordCount = deltaRecordCount;
    }
    
    /**
     * 简化构造函数（用于简单场景）
     * 
     * @deprecated 建议使用 Builder 模式
     */
    @Deprecated
    public Snapshot(long id, int schemaId, String manifestList) {
        this(3, id, schemaId, manifestList, manifestList, 
             "system", id, CommitKind.APPEND, 
             System.currentTimeMillis(), 0, 0);
    }

    public int getVersion() {
        return version;
    }
    
    public long getId() {
        return id;
    }
    
    /**
     * @deprecated 使用 getId()
     */
    @Deprecated
    public long getSnapshotId() {
        return id;
    }

    public int getSchemaId() {
        return schemaId;
    }
    
    public String getBaseManifestList() {
        return baseManifestList;
    }
    
    public String getDeltaManifestList() {
        return deltaManifestList;
    }
    
    /**
     * @deprecated 使用 getBaseManifestList() 或 getDeltaManifestList()
     */
    @Deprecated
    public String getManifestList() {
        return baseManifestList;
    }
    
    public String getCommitUser() {
        return commitUser;
    }
    
    public long getCommitIdentifier() {
        return commitIdentifier;
    }
    
    public CommitKind getCommitKind() {
        return commitKind;
    }
    
    public long getTimeMillis() {
        return timeMillis;
    }
    
    /**
     * @deprecated 使用 getTimeMillis()
     */
    @Deprecated
    public Instant getCommitTime() {
        return Instant.ofEpochMilli(timeMillis);
    }
    
    public long getTotalRecordCount() {
        return totalRecordCount;
    }
    
    public long getDeltaRecordCount() {
        return deltaRecordCount;
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
        Path snapshotPath = pathFactory.getSnapshotPath(database, table, id);
        Files.createDirectories(snapshotPath.getParent());
        SerializationUtils.writeToFile(snapshotPath, this);
        
        // 更新 LATEST 指针
        updateLatestSnapshot(pathFactory, database, table);
        
        // 更新 EARLIEST 指针（如果不存在）
        updateEarliestSnapshot(pathFactory, database, table);
    }

    /**
     * 更新最新快照指针（LATEST）
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @throws IOException IO异常
     */
    private void updateLatestSnapshot(PathFactory pathFactory, String database, String table) throws IOException {
        Path latestPath = pathFactory.getLatestSnapshotPath(database, table);
        Files.write(latestPath, String.valueOf(id).getBytes());
    }
    
    /**
     * 更新最早快照指针（EARLIEST）
     * 只在首次创建快照时更新
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @throws IOException IO异常
     */
    private void updateEarliestSnapshot(PathFactory pathFactory, String database, String table) throws IOException {
        Path earliestPath = pathFactory.getEarliestSnapshotPath(database, table);
        if (!Files.exists(earliestPath)) {
            Files.write(earliestPath, String.valueOf(id).getBytes());
        }
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
        return version == snapshot.version &&
                id == snapshot.id &&
                schemaId == snapshot.schemaId &&
                commitIdentifier == snapshot.commitIdentifier &&
                timeMillis == snapshot.timeMillis &&
                totalRecordCount == snapshot.totalRecordCount &&
                deltaRecordCount == snapshot.deltaRecordCount &&
                Objects.equals(baseManifestList, snapshot.baseManifestList) &&
                Objects.equals(deltaManifestList, snapshot.deltaManifestList) &&
                Objects.equals(commitUser, snapshot.commitUser) &&
                commitKind == snapshot.commitKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, id, schemaId, baseManifestList, deltaManifestList,
                commitUser, commitIdentifier, commitKind, timeMillis,
                totalRecordCount, deltaRecordCount);
    }

    @Override
    public String toString() {
        return "Snapshot{" +
                "version=" + version +
                ", id=" + id +
                ", schemaId=" + schemaId +
                ", baseManifestList='" + baseManifestList + '\'' +
                ", deltaManifestList='" + deltaManifestList + '\'' +
                ", commitUser='" + commitUser + '\'' +
                ", commitIdentifier=" + commitIdentifier +
                ", commitKind=" + commitKind +
                ", timeMillis=" + timeMillis +
                ", totalRecordCount=" + totalRecordCount +
                ", deltaRecordCount=" + deltaRecordCount +
                '}';
    }
    
    /**
     * Snapshot Builder
     * 用于构建 Snapshot 对象的建造者模式
     */
    public static class Builder {
        private int version = 3; // 当前版本为 3
        private long id;
        private int schemaId;
        private String baseManifestList;
        private String deltaManifestList;
        private String commitUser = "system";
        private long commitIdentifier;
        private CommitKind commitKind = CommitKind.APPEND;
        private long timeMillis = System.currentTimeMillis();
        private long totalRecordCount = 0;
        private long deltaRecordCount = 0;
        
        public Builder() {}
        
        public Builder version(int version) {
            this.version = version;
            return this;
        }
        
        public Builder id(long id) {
            this.id = id;
            return this;
        }
        
        public Builder schemaId(int schemaId) {
            this.schemaId = schemaId;
            return this;
        }
        
        public Builder baseManifestList(String baseManifestList) {
            this.baseManifestList = baseManifestList;
            return this;
        }
        
        public Builder deltaManifestList(String deltaManifestList) {
            this.deltaManifestList = deltaManifestList;
            return this;
        }
        
        public Builder commitUser(String commitUser) {
            this.commitUser = commitUser;
            return this;
        }
        
        public Builder commitIdentifier(long commitIdentifier) {
            this.commitIdentifier = commitIdentifier;
            return this;
        }
        
        public Builder commitKind(CommitKind commitKind) {
            this.commitKind = commitKind;
            return this;
        }
        
        public Builder timeMillis(long timeMillis) {
            this.timeMillis = timeMillis;
            return this;
        }
        
        public Builder totalRecordCount(long totalRecordCount) {
            this.totalRecordCount = totalRecordCount;
            return this;
        }
        
        public Builder deltaRecordCount(long deltaRecordCount) {
            this.deltaRecordCount = deltaRecordCount;
            return this;
        }
        
        public Snapshot build() {
            // 如果 baseManifestList 为空，使用 deltaManifestList
            if (baseManifestList == null && deltaManifestList != null) {
                baseManifestList = deltaManifestList;
            }
            
            return new Snapshot(
                version, id, schemaId, baseManifestList, deltaManifestList,
                commitUser, commitIdentifier, commitKind, timeMillis,
                totalRecordCount, deltaRecordCount
            );
        }
    }
}
