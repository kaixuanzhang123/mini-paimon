package com.mini.paimon.operation;

import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.snapshot.Snapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Manifest Committable
 * 参考Apache Paimon的ManifestCommittable设计
 * 包含一次提交的所有信息
 */
public class ManifestCommittable {
    
    /** 提交标识符 */
    private final long commitIdentifier;
    
    /** 提交用户 */
    private final String commitUser;
    
    /** 追加的表文件 */
    private final List<ManifestEntry> appendTableFiles;
    
    /** 压缩后的表文件 */
    private final List<ManifestEntry> compactTableFiles;
    
    /** 提交类型 */
    private final Snapshot.CommitKind commitKind;
    
    /** Watermark (可选) */
    private final Long watermark;
    
    public ManifestCommittable(long commitIdentifier, String commitUser) {
        this(commitIdentifier, commitUser, new ArrayList<>(), new ArrayList<>(),
             Snapshot.CommitKind.APPEND, null);
    }
    
    public ManifestCommittable(long commitIdentifier, String commitUser,
                              List<ManifestEntry> appendTableFiles,
                              List<ManifestEntry> compactTableFiles,
                              Snapshot.CommitKind commitKind,
                              Long watermark) {
        this.commitIdentifier = commitIdentifier;
        this.commitUser = Objects.requireNonNull(commitUser, "commitUser cannot be null");
        this.appendTableFiles = new ArrayList<>(appendTableFiles);
        this.compactTableFiles = new ArrayList<>(compactTableFiles);
        this.commitKind = Objects.requireNonNull(commitKind, "commitKind cannot be null");
        this.watermark = watermark;
    }
    
    public long getCommitIdentifier() {
        return commitIdentifier;
    }
    
    public String getCommitUser() {
        return commitUser;
    }
    
    public List<ManifestEntry> getAppendTableFiles() {
        return Collections.unmodifiableList(appendTableFiles);
    }
    
    public List<ManifestEntry> getCompactTableFiles() {
        return Collections.unmodifiableList(compactTableFiles);
    }
    
    public Snapshot.CommitKind getCommitKind() {
        return commitKind;
    }
    
    public Long getWatermark() {
        return watermark;
    }
    
    public boolean isEmpty() {
        return appendTableFiles.isEmpty() && compactTableFiles.isEmpty();
    }
    
    @Override
    public String toString() {
        return "ManifestCommittable{" +
                "commitIdentifier=" + commitIdentifier +
                ", commitUser='" + commitUser + '\'' +
                ", appendFiles=" + appendTableFiles.size() +
                ", compactFiles=" + compactTableFiles.size() +
                ", commitKind=" + commitKind +
                '}';
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private long commitIdentifier;
        private String commitUser;
        private List<ManifestEntry> appendTableFiles = new ArrayList<>();
        private List<ManifestEntry> compactTableFiles = new ArrayList<>();
        private Snapshot.CommitKind commitKind = Snapshot.CommitKind.APPEND;
        private Long watermark;
        
        public Builder commitIdentifier(long commitIdentifier) {
            this.commitIdentifier = commitIdentifier;
            return this;
        }
        
        public Builder commitUser(String commitUser) {
            this.commitUser = commitUser;
            return this;
        }
        
        public Builder appendTableFiles(List<ManifestEntry> files) {
            this.appendTableFiles = files;
            return this;
        }
        
        public Builder compactTableFiles(List<ManifestEntry> files) {
            this.compactTableFiles = files;
            return this;
        }
        
        public Builder commitKind(Snapshot.CommitKind commitKind) {
            this.commitKind = commitKind;
            return this;
        }
        
        public Builder watermark(Long watermark) {
            this.watermark = watermark;
            return this;
        }
        
        public ManifestCommittable build() {
            return new ManifestCommittable(commitIdentifier, commitUser,
                appendTableFiles, compactTableFiles, commitKind, watermark);
        }
    }
}
