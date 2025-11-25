package com.mini.paimon.operation;

/**
 * 提交配置选项
 * 参考Apache Paimon的提交配置
 */
public class CommitOptions {
    
    /** 提交超时时间（毫秒），默认10分钟 */
    private final long commitTimeout;
    
    /** 最大重试次数，默认Integer.MAX_VALUE */
    private final int commitMaxRetries;
    
    /** 最小重试等待时间（毫秒），默认100ms */
    private final long commitRetryMinWait;
    
    /** 最大重试等待时间（毫秒），默认30s */
    private final long commitRetryMaxWait;
    
    /** Manifest目标文件大小，默认8MB */
    private final long manifestTargetSize;
    
    /** Manifest合并最小文件数 */
    private final int manifestMergeMinCount;
    
    public CommitOptions() {
        this(10 * 60 * 1000L, Integer.MAX_VALUE, 100L, 30 * 1000L, 
             8 * 1024 * 1024L, 30);
    }
    
    public CommitOptions(long commitTimeout, int commitMaxRetries,
                        long commitRetryMinWait, long commitRetryMaxWait,
                        long manifestTargetSize, int manifestMergeMinCount) {
        this.commitTimeout = commitTimeout;
        this.commitMaxRetries = commitMaxRetries;
        this.commitRetryMinWait = commitRetryMinWait;
        this.commitRetryMaxWait = commitRetryMaxWait;
        this.manifestTargetSize = manifestTargetSize;
        this.manifestMergeMinCount = manifestMergeMinCount;
    }
    
    public long getCommitTimeout() {
        return commitTimeout;
    }
    
    public int getCommitMaxRetries() {
        return commitMaxRetries;
    }
    
    public long getCommitRetryMinWait() {
        return commitRetryMinWait;
    }
    
    public long getCommitRetryMaxWait() {
        return commitRetryMaxWait;
    }
    
    public long getManifestTargetSize() {
        return manifestTargetSize;
    }
    
    public int getManifestMergeMinCount() {
        return manifestMergeMinCount;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private long commitTimeout = 10 * 60 * 1000L;
        private int commitMaxRetries = Integer.MAX_VALUE;
        private long commitRetryMinWait = 100L;
        private long commitRetryMaxWait = 30 * 1000L;
        private long manifestTargetSize = 8 * 1024 * 1024L;
        private int manifestMergeMinCount = 30;
        
        public Builder commitTimeout(long commitTimeout) {
            this.commitTimeout = commitTimeout;
            return this;
        }
        
        public Builder commitMaxRetries(int commitMaxRetries) {
            this.commitMaxRetries = commitMaxRetries;
            return this;
        }
        
        public Builder commitRetryMinWait(long commitRetryMinWait) {
            this.commitRetryMinWait = commitRetryMinWait;
            return this;
        }
        
        public Builder commitRetryMaxWait(long commitRetryMaxWait) {
            this.commitRetryMaxWait = commitRetryMaxWait;
            return this;
        }
        
        public Builder manifestTargetSize(long manifestTargetSize) {
            this.manifestTargetSize = manifestTargetSize;
            return this;
        }
        
        public Builder manifestMergeMinCount(int manifestMergeMinCount) {
            this.manifestMergeMinCount = manifestMergeMinCount;
            return this;
        }
        
        public CommitOptions build() {
            return new CommitOptions(commitTimeout, commitMaxRetries, commitRetryMinWait,
                commitRetryMaxWait, manifestTargetSize, manifestMergeMinCount);
        }
    }
}

