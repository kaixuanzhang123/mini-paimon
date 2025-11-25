package com.mini.paimon.snapshot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Snapshot - 纯数据类
 * 参考 Apache Paimon 的 Snapshot 设计
 * 每次提交生成一个快照，记录表的完整状态
 * 所有读写操作通过 SnapshotManager 进行
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Snapshot {
    
    /** 快照文件版本 */
    private final int version;
    
    /** 快照 ID */
    private final long id;
    
    /** Schema ID */
    private final int schemaId;
    
    /** Base Manifest List 文件名 */
    private final String baseManifestList;
    
    /** Delta Manifest List 文件名 */
    private final String deltaManifestList;
    
    /** 提交用户标识 */
    private final String commitUser;
    
    /** 提交标识符 */
    private final long commitIdentifier;
    
    /** 提交类型 */
    private final CommitKind commitKind;
    
    /** 提交时间（毫秒时间戳）*/
    private final long timeMillis;
    
    /** 总记录数 */
    private final long totalRecordCount;
    
    /** Delta记录数 */
    private final long deltaRecordCount;
    
    /**
     * 提交类型枚举
     */
    public enum CommitKind {
        APPEND,
        COMPACT,
        OVERWRITE,
        ANALYZE
    }

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

    public int getVersion() {
        return version;
    }
    
    public long getId() {
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
    
    public long getTotalRecordCount() {
        return totalRecordCount;
    }
    
    public long getDeltaRecordCount() {
        return deltaRecordCount;
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
}
