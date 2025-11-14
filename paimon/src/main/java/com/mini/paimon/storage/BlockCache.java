package com.mini.paimon.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Block Cache
 * 参考 Apache Paimon 和 RocksDB 的 Block Cache 设计
 * 使用 LRU 策略缓存 SSTable 的数据块,减少磁盘 I/O
 * 
 * 特性:
 * 1. LRU 淘汰策略
 * 2. 线程安全
 * 3. 内存限制
 * 4. 统计信息(命中率、驱逐次数等)
 */
public class BlockCache {
    private static final Logger logger = LoggerFactory.getLogger(BlockCache.class);
    
    /** 默认缓存大小: 256MB */
    private static final long DEFAULT_CACHE_SIZE = 256 * 1024 * 1024;
    
    /** 缓存的最大字节数 */
    private final long maxSizeInBytes;
    
    /** 当前缓存的字节数 */
    private long currentSizeInBytes = 0;
    
    /** LRU 缓存 */
    private final LinkedHashMap<BlockCacheKey, CachedBlock> cache;
    
    /** 统计信息 */
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);
    private final AtomicLong evictionCount = new AtomicLong(0);
    
    /**
     * 创建默认大小的 Block Cache
     */
    public BlockCache() {
        this(DEFAULT_CACHE_SIZE);
    }
    
    /**
     * 创建指定大小的 Block Cache
     */
    public BlockCache(long maxSizeInBytes) {
        this.maxSizeInBytes = maxSizeInBytes;
        // LinkedHashMap with access order for LRU
        this.cache = new LinkedHashMap<BlockCacheKey, CachedBlock>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<BlockCacheKey, CachedBlock> eldest) {
                if (currentSizeInBytes > maxSizeInBytes) {
                    currentSizeInBytes -= eldest.getValue().size();
                    evictionCount.incrementAndGet();
                    logger.debug("Evicted block: {}, size: {}", eldest.getKey(), eldest.getValue().size());
                    return true;
                }
                return false;
            }
        };
        
        logger.info("BlockCache initialized with max size: {} MB", maxSizeInBytes / 1024 / 1024);
    }
    
    /**
     * 获取缓存的数据块
     */
    public synchronized byte[] get(String filePath, long blockOffset) {
        BlockCacheKey key = new BlockCacheKey(filePath, blockOffset);
        CachedBlock block = cache.get(key);
        
        if (block != null) {
            hitCount.incrementAndGet();
            logger.trace("Block cache hit: {}", key);
            return block.data;
        }
        
        missCount.incrementAndGet();
        logger.trace("Block cache miss: {}", key);
        return null;
    }
    
    /**
     * 放入数据块到缓存
     */
    public synchronized void put(String filePath, long blockOffset, byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        
        BlockCacheKey key = new BlockCacheKey(filePath, blockOffset);
        
        // 如果单个 block 大于缓存大小,不缓存
        if (data.length > maxSizeInBytes) {
            logger.warn("Block size {} exceeds cache size {}, not caching", 
                       data.length, maxSizeInBytes);
            return;
        }
        
        CachedBlock block = new CachedBlock(data);
        CachedBlock old = cache.put(key, block);
        
        if (old != null) {
            // 替换旧值
            currentSizeInBytes -= old.size();
        }
        
        currentSizeInBytes += block.size();
        logger.trace("Block cached: {}, size: {}", key, block.size());
    }
    
    /**
     * 使缓存失效
     */
    public synchronized void invalidate(String filePath) {
        cache.entrySet().removeIf(entry -> {
            if (entry.getKey().filePath.equals(filePath)) {
                currentSizeInBytes -= entry.getValue().size();
                return true;
            }
            return false;
        });
        logger.debug("Invalidated cache for file: {}", filePath);
    }
    
    /**
     * 清空缓存
     */
    public synchronized void clear() {
        cache.clear();
        currentSizeInBytes = 0;
        logger.info("Block cache cleared");
    }
    
    /**
     * 获取缓存统计信息
     */
    public CacheStats getStats() {
        long hits = hitCount.get();
        long misses = missCount.get();
        long total = hits + misses;
        double hitRate = total > 0 ? (double) hits / total : 0.0;
        
        return new CacheStats(
            hits,
            misses,
            hitRate,
            evictionCount.get(),
            cache.size(),
            currentSizeInBytes,
            maxSizeInBytes
        );
    }
    
    /**
     * 缓存键
     */
    private static class BlockCacheKey {
        final String filePath;
        final long blockOffset;
        
        BlockCacheKey(String filePath, long blockOffset) {
            this.filePath = filePath;
            this.blockOffset = blockOffset;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BlockCacheKey)) return false;
            BlockCacheKey that = (BlockCacheKey) o;
            return blockOffset == that.blockOffset && filePath.equals(that.filePath);
        }
        
        @Override
        public int hashCode() {
            return 31 * filePath.hashCode() + Long.hashCode(blockOffset);
        }
        
        @Override
        public String toString() {
            return filePath + "@" + blockOffset;
        }
    }
    
    /**
     * 缓存的数据块
     */
    private static class CachedBlock {
        final byte[] data;
        final long timestamp;
        
        CachedBlock(byte[] data) {
            this.data = data;
            this.timestamp = System.currentTimeMillis();
        }
        
        int size() {
            return data.length;
        }
    }
    
    /**
     * 缓存统计信息
     */
    public static class CacheStats {
        public final long hitCount;
        public final long missCount;
        public final double hitRate;
        public final long evictionCount;
        public final int entryCount;
        public final long currentSizeInBytes;
        public final long maxSizeInBytes;
        
        CacheStats(long hitCount, long missCount, double hitRate, 
                  long evictionCount, int entryCount, 
                  long currentSizeInBytes, long maxSizeInBytes) {
            this.hitCount = hitCount;
            this.missCount = missCount;
            this.hitRate = hitRate;
            this.evictionCount = evictionCount;
            this.entryCount = entryCount;
            this.currentSizeInBytes = currentSizeInBytes;
            this.maxSizeInBytes = maxSizeInBytes;
        }
        
        @Override
        public String toString() {
            return String.format(
                "CacheStats{hits=%d, misses=%d, hitRate=%.2f%%, evictions=%d, " +
                "entries=%d, size=%d/%d MB}",
                hitCount, missCount, hitRate * 100, evictionCount, entryCount,
                currentSizeInBytes / 1024 / 1024, maxSizeInBytes / 1024 / 1024
            );
        }
    }
}


