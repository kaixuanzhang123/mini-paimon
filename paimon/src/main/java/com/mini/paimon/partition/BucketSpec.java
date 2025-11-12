package com.mini.paimon.partition;

import java.util.Objects;

/**
 * Bucket 规范
 * 参考 Paimon 的 Bucket 机制设计
 * 
 * Bucket 用于：
 * 1. 并行写入同一分区
 * 2. 支持高效的更新和删除操作
 * 3. 数据分散存储，提升读写性能
 */
public class BucketSpec {
    private final int bucket;
    private final int totalBuckets;
    
    public BucketSpec(int bucket, int totalBuckets) {
        if (bucket < 0 || bucket >= totalBuckets) {
            throw new IllegalArgumentException(
                "Invalid bucket: " + bucket + ", total buckets: " + totalBuckets);
        }
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
    }
    
    public int getBucket() {
        return bucket;
    }
    
    public int getTotalBuckets() {
        return totalBuckets;
    }
    
    /**
     * 转换为路径字符串
     * 格式: bucket-{id}
     */
    public String toPath() {
        return "bucket-" + bucket;
    }
    
    /**
     * 根据行键计算 Bucket
     */
    public static int computeBucket(byte[] keyBytes, int totalBuckets) {
        if (totalBuckets <= 0) {
            throw new IllegalArgumentException("Total buckets must be positive: " + totalBuckets);
        }
        
        // 使用 Arrays.hashCode 计算数组内容的 hash 值（而不是对象的 identityHashCode）
        // 确保相同内容的 byte[] 总是映射到相同的 bucket
        int hash = Math.abs(java.util.Arrays.hashCode(keyBytes));
        return hash % totalBuckets;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BucketSpec that = (BucketSpec) o;
        return bucket == that.bucket && totalBuckets == that.totalBuckets;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(bucket, totalBuckets);
    }
    
    @Override
    public String toString() {
        return "BucketSpec{bucket=" + bucket + ", total=" + totalBuckets + "}";
    }
}
