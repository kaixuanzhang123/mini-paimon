package com.mini.paimon.manifest;

import com.mini.paimon.io.ManifestFileIO;
import com.mini.paimon.io.ManifestListIO;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manifest 缓存管理器
 * 提供 Manifest 文件的缓存和增量读取机制
 * 参考 Paimon ManifestCachingFilter 实现
 */
public class ManifestCacheManager {
    private static final Logger logger = LoggerFactory.getLogger(ManifestCacheManager.class);
    
    // Manifest文件缓存 - Key: manifestFileName, Value: ManifestFile
    private final Map<String, CachedManifestFile> manifestCache;
    
    // Manifest List缓存 - Key: snapshotId, Value: List<ManifestFileMeta>
    private final Map<Long, List<ManifestFileMeta>> manifestListCache;
    
    // 缓存策略配置
    private final int maxCacheSize;
    private final long cacheExpirationMs;
    
    // LRU淘汰策略的访问时间记录
    private final Map<String, Long> lastAccessTime;
    
    public ManifestCacheManager(int maxCacheSize, long cacheExpirationMs) {
        this.manifestCache = new ConcurrentHashMap<>();
        this.manifestListCache = new ConcurrentHashMap<>();
        this.lastAccessTime = new ConcurrentHashMap<>();
        this.maxCacheSize = maxCacheSize;
        this.cacheExpirationMs = cacheExpirationMs;
    }
    
    public ManifestCacheManager() {
        this(1000, 5 * 60 * 1000); // 默认1000个文件，5分钟过期
    }
    
    /**
     * 获取 ManifestFile，优先从缓存读取（使用 ManifestFileIO）
     */
    public List<ManifestEntry> getManifestFile(PathFactory pathFactory, String database, 
                                       String table, String manifestId) throws IOException {
        String cacheKey = buildManifestCacheKey(database, table, manifestId);
        
        // 检查缓存
        CachedManifestFile cached = manifestCache.get(cacheKey);
        if (cached != null && !cached.isExpired(cacheExpirationMs)) {
            logger.debug("Manifest cache hit: {}", manifestId);
            updateAccessTime(cacheKey);
            return cached.manifestFile.getEntries();
        }
        
        // 缓存未命中，从磁盘加载（使用 ManifestFileIO）
        logger.debug("Manifest cache miss: {}, loading from disk", manifestId);
        ManifestFileIO reader = new ManifestFileIO(pathFactory, database, table);
        List<ManifestEntry> entries = reader.readManifestById(manifestId);
        ManifestFile manifestFile = new ManifestFile(entries);
        
        // 放入缓存
        putInCache(cacheKey, manifestFile);
        
        return entries;
    }
    
    /**
     * 获取 ManifestList，优先从缓存读取（使用 ManifestListIO）
     */
    public List<ManifestFileMeta> getManifestList(PathFactory pathFactory, String database, 
                                       String table, long snapshotId) throws IOException {
        // 检查缓存
        List<ManifestFileMeta> cached = manifestListCache.get(snapshotId);
        if (cached != null) {
            logger.debug("ManifestList cache hit: snapshot {}", snapshotId);
            return cached;
        }
        
        // 缓存未命中，从磁盘加载（使用 ManifestListIO）
        logger.debug("ManifestList cache miss: snapshot {}, loading from disk", snapshotId);
        ManifestListIO reader = new ManifestListIO(pathFactory, database, table);
        List<ManifestFileMeta> manifestMetas = reader.readManifestList("manifest-list-" + snapshotId);
        
        // 放入缓存
        manifestListCache.put(snapshotId, manifestMetas);
        
        return manifestMetas;
    }
    
    /**
     * 获取 Delta ManifestList（使用 ManifestListIO）
     */
    public List<ManifestFileMeta> getDeltaManifestList(PathFactory pathFactory, String database, 
                                           String table, long snapshotId) throws IOException {
        // 使用负数来区分delta类型的缓存键
        Long cacheKey = -snapshotId;
        List<ManifestFileMeta> cached = manifestListCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        
        ManifestListIO reader = new ManifestListIO(pathFactory, database, table);
        List<ManifestFileMeta> manifestMetas = reader.readDeltaManifestList(snapshotId);
        manifestListCache.put(cacheKey, manifestMetas);
        return manifestMetas;
    }
    
    /**
     * 获取 Base ManifestList（使用 ManifestListIO）
     */
    public List<ManifestFileMeta> getBaseManifestList(PathFactory pathFactory, String database, 
                                          String table, long snapshotId) throws IOException {
        // 使用大数值偏移来区分base类型的缓存键
        Long cacheKey = snapshotId + 1000000L;
        List<ManifestFileMeta> cached = manifestListCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        
        ManifestListIO reader = new ManifestListIO(pathFactory, database, table);
        List<ManifestFileMeta> manifestMetas = reader.readBaseManifestList(snapshotId);
        manifestListCache.put(cacheKey, manifestMetas);
        return manifestMetas;
    }
    
    /**
     * 增量读取Manifest变更
     * 只读取从baseSnapshotId到targetSnapshotId之间的变更
     */
    public IncrementalManifest readIncrementalManifest(PathFactory pathFactory, 
                                                      String database, String table,
                                                      Long baseSnapshotId, long targetSnapshotId) 
            throws IOException {
        
        logger.debug("Reading incremental manifest from snapshot {} to {}", 
                    baseSnapshotId, targetSnapshotId);
        
        // 如果没有基准快照，返回全部文件
        if (baseSnapshotId == null) {
            List<ManifestFileMeta> targetManifestMetas = getDeltaManifestList(pathFactory, database, table, targetSnapshotId);
            ManifestList targetManifestList = new ManifestList(targetManifestMetas);
            List<ManifestEntry> allEntries = loadAllEntries(pathFactory, database, table, targetManifestList);
            return new IncrementalManifest(allEntries, Collections.emptyList());
        }
        
        // 读取从baseSnapshotId+1到targetSnapshotId的所有delta manifests
        List<ManifestEntry> newEntries = new ArrayList<>();
        List<ManifestEntry> deletedEntries = new ArrayList<>();
        
        for (long snapId = baseSnapshotId + 1; snapId <= targetSnapshotId; snapId++) {
            try {
                List<ManifestFileMeta> deltaMetas = getDeltaManifestList(pathFactory, database, table, snapId);
                ManifestList deltaList = new ManifestList(deltaMetas);
                List<ManifestEntry> entries = loadAllEntries(pathFactory, database, table, deltaList);
                
                for (ManifestEntry entry : entries) {
                    if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                        newEntries.add(entry);
                    } else if (entry.getKind() == ManifestEntry.FileKind.DELETE) {
                        deletedEntries.add(entry);
                    }
                }
                
                logger.debug("Loaded delta manifest from snapshot {}, {} entries", snapId, entries.size());
            } catch (IOException e) {
                logger.warn("Failed to load delta manifest for snapshot {}: {}", snapId, e.getMessage());
                // 继续处理下一个snapshot
            }
        }
        
        logger.info("Incremental manifest: {} new entries, {} deleted entries", 
                   newEntries.size(), deletedEntries.size());
        
        return new IncrementalManifest(newEntries, deletedEntries);
    }
    
    /**
     * 计算两个快照之间的增量变更
     */
    private IncrementalManifest computeIncrementalChanges(PathFactory pathFactory,
                                                          String database, String table,
                                                          ManifestList baseList, 
                                                          ManifestList targetList) 
            throws IOException {
        
        // 构建基准快照的文件集合
        Set<String> baseManifestFiles = baseList.getManifestFiles().stream()
            .map(ManifestFileMeta::getFileName)
            .collect(Collectors.toSet());
        
        // 找出新增的Manifest文件
        List<ManifestFileMeta> newManifestFiles = targetList.getManifestFiles().stream()
            .filter(meta -> !baseManifestFiles.contains(meta.getFileName()))
            .collect(Collectors.toList());
        
        logger.debug("Found {} new manifest files out of {}", 
                    newManifestFiles.size(), targetList.getManifestFiles().size());
        
        // 只读取新增的Manifest文件
        List<ManifestEntry> newEntries = new ArrayList<>();
        List<ManifestEntry> deletedEntries = new ArrayList<>();
        
        for (ManifestFileMeta meta : newManifestFiles) {
            String manifestId = meta.getFileName().substring("manifest-".length());
            List<ManifestEntry> entries = getManifestFile(pathFactory, database, table, manifestId);
            
            for (ManifestEntry entry : entries) {
                if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                    newEntries.add(entry);
                } else if (entry.getKind() == ManifestEntry.FileKind.DELETE) {
                    deletedEntries.add(entry);
                }
            }
        }
        
        logger.info("Incremental manifest: {} new entries, {} deleted entries", 
                   newEntries.size(), deletedEntries.size());
        
        return new IncrementalManifest(newEntries, deletedEntries);
    }
    
    /**
     * 加载ManifestList中的所有条目
     */
    private List<ManifestEntry> loadAllEntries(PathFactory pathFactory, String database, 
                                              String table, ManifestList manifestList) 
            throws IOException {
        List<ManifestEntry> allEntries = new ArrayList<>();
        
        for (ManifestFileMeta meta : manifestList.getManifestFiles()) {
            String manifestId = meta.getFileName().substring("manifest-".length());
            List<ManifestEntry> entries = getManifestFile(pathFactory, database, table, manifestId);
            
            for (ManifestEntry entry : entries) {
                if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                    allEntries.add(entry);
                }
            }
        }
        
        return allEntries;
    }
    
    /**
     * 将Manifest文件放入缓存，执行LRU淘汰策略
     */
    private void putInCache(String cacheKey, ManifestFile manifestFile) {
        // 检查缓存大小，执行LRU淘汰
        if (manifestCache.size() >= maxCacheSize) {
            evictLRU();
        }
        
        manifestCache.put(cacheKey, new CachedManifestFile(manifestFile, System.currentTimeMillis()));
        updateAccessTime(cacheKey);
    }
    
    /**
     * LRU淘汰策略 - 淘汰最久未访问的条目
     */
    private void evictLRU() {
        if (lastAccessTime.isEmpty()) {
            return;
        }
        
        // 找到最久未访问的key
        String lruKey = null;
        long oldestTime = Long.MAX_VALUE;
        
        for (Map.Entry<String, Long> entry : lastAccessTime.entrySet()) {
            if (entry.getValue() < oldestTime) {
                oldestTime = entry.getValue();
                lruKey = entry.getKey();
            }
        }
        
        if (lruKey != null) {
            manifestCache.remove(lruKey);
            lastAccessTime.remove(lruKey);
            logger.debug("Evicted manifest from cache: {}", lruKey);
        }
    }
    
    /**
     * 更新访问时间
     */
    private void updateAccessTime(String cacheKey) {
        lastAccessTime.put(cacheKey, System.currentTimeMillis());
    }
    
    /**
     * 构建缓存key
     */
    private String buildManifestCacheKey(String database, String table, String manifestId) {
        return database + "/" + table + "/manifest-" + manifestId;
    }
    
    /**
     * 清除过期缓存
     */
    public void clearExpiredCache() {
        long now = System.currentTimeMillis();
        List<String> expiredKeys = new ArrayList<>();
        
        for (Map.Entry<String, CachedManifestFile> entry : manifestCache.entrySet()) {
            if (entry.getValue().isExpired(cacheExpirationMs)) {
                expiredKeys.add(entry.getKey());
            }
        }
        
        for (String key : expiredKeys) {
            manifestCache.remove(key);
            lastAccessTime.remove(key);
        }
        
        if (!expiredKeys.isEmpty()) {
            logger.info("Cleared {} expired manifest cache entries", expiredKeys.size());
        }
    }
    
    /**
     * 清空所有缓存
     */
    public void clearAll() {
        manifestCache.clear();
        manifestListCache.clear();
        lastAccessTime.clear();
        logger.info("Cleared all manifest cache");
    }
    
    /**
     * 获取缓存统计信息
     */
    public CacheStats getStats() {
        return new CacheStats(manifestCache.size(), manifestListCache.size());
    }
    
    /**
     * 缓存的Manifest文件
     */
    private static class CachedManifestFile {
        final ManifestFile manifestFile;
        final long cacheTime;
        
        CachedManifestFile(ManifestFile manifestFile, long cacheTime) {
            this.manifestFile = manifestFile;
            this.cacheTime = cacheTime;
        }
        
        boolean isExpired(long expirationMs) {
            return System.currentTimeMillis() - cacheTime > expirationMs;
        }
    }
    
    /**
     * 增量Manifest结果
     */
    public static class IncrementalManifest {
        private final List<ManifestEntry> newEntries;
        private final List<ManifestEntry> deletedEntries;
        
        public IncrementalManifest(List<ManifestEntry> newEntries, List<ManifestEntry> deletedEntries) {
            this.newEntries = newEntries;
            this.deletedEntries = deletedEntries;
        }
        
        public List<ManifestEntry> getNewEntries() {
            return newEntries;
        }
        
        public List<ManifestEntry> getDeletedEntries() {
            return deletedEntries;
        }
        
        /**
         * 合并到现有文件列表
         */
        public List<ManifestEntry> mergeWith(List<ManifestEntry> baseEntries) {
            Map<String, ManifestEntry> merged = new LinkedHashMap<>();
            
            // 添加基准条目
            for (ManifestEntry entry : baseEntries) {
                merged.put(entry.getFileName(), entry);
            }
            
            // 应用新增
            for (ManifestEntry entry : newEntries) {
                merged.put(entry.getFileName(), entry);
            }
            
            // 应用删除
            for (ManifestEntry entry : deletedEntries) {
                merged.remove(entry.getFileName());
            }
            
            return new ArrayList<>(merged.values());
        }
    }
    
    /**
     * 缓存统计信息
     */
    public static class CacheStats {
        private final int manifestCacheSize;
        private final int manifestListCacheSize;
        
        public CacheStats(int manifestCacheSize, int manifestListCacheSize) {
            this.manifestCacheSize = manifestCacheSize;
            this.manifestListCacheSize = manifestListCacheSize;
        }
        
        public int getManifestCacheSize() {
            return manifestCacheSize;
        }
        
        public int getManifestListCacheSize() {
            return manifestListCacheSize;
        }
        
        @Override
        public String toString() {
            return "CacheStats{manifestCache=" + manifestCacheSize + 
                   ", manifestListCache=" + manifestListCacheSize + "}";
        }
    }
}
