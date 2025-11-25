package com.mini.paimon.io;

import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.schema.RowKey;

import java.util.*;

/**
 * File Entry
 * 参考Apache Paimon的FileEntry设计
 * 用于合并文件变更并检测冲突
 */
public class FileEntry {
    
    /**
     * 文件唯一标识
     * 用于检测同一个文件的ADD/DELETE操作
     */
    public static class Identifier {
        private final String partition;
        private final int bucket;
        private final int level;
        private final String fileName;
        
        public Identifier(String partition, int bucket, int level, String fileName) {
            this.partition = partition != null ? partition : "";
            this.bucket = bucket;
            this.level = level;
            this.fileName = Objects.requireNonNull(fileName);
        }
        
        public static Identifier fromEntry(ManifestEntry entry) {
            return new Identifier(
                entry.getFile().getFileName().contains("/") 
                    ? entry.getFile().getFileName().substring(0, entry.getFile().getFileName().lastIndexOf("/"))
                    : "",
                entry.getBucket(),
                entry.getLevel(),
                entry.getFileName()
            );
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Identifier that = (Identifier) o;
            return bucket == that.bucket &&
                   level == that.level &&
                   partition.equals(that.partition) &&
                   fileName.equals(that.fileName);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, level, fileName);
        }
        
        @Override
        public String toString() {
            return "Identifier{" +
                   "partition='" + partition + '\'' +
                   ", bucket=" + bucket +
                   ", level=" + level +
                   ", fileName='" + fileName + '\'' +
                   '}';
        }
    }
    
    /**
     * 简化的FileEntry，用于冲突检测
     */
    public static class SimpleFileEntry {
        private final ManifestEntry.FileKind kind;
        private final Identifier identifier;
        private final RowKey minKey;
        private final RowKey maxKey;
        private final int level;
        
        public SimpleFileEntry(ManifestEntry entry) {
            this.kind = entry.getKind();
            this.identifier = Identifier.fromEntry(entry);
            this.minKey = entry.getMinKey();
            this.maxKey = entry.getMaxKey();
            this.level = entry.getLevel();
        }
        
        public ManifestEntry.FileKind getKind() {
            return kind;
        }
        
        public Identifier getIdentifier() {
            return identifier;
        }
        
        public RowKey getMinKey() {
            return minKey;
        }
        
        public RowKey getMaxKey() {
            return maxKey;
        }
        
        public int getLevel() {
            return level;
        }
        
        public String getFileName() {
            return identifier.fileName;
        }
    }
    
    /**
     * 合并文件条目列表，检测冲突
     * 
     * ADD + DELETE = 无（抵消）
     * DELETE（无对应ADD）= 保留DELETE标记
     * ADD（无对应DELETE）= 保留ADD
     * 
     * @param entries 文件条目列表
     * @return 合并后的文件条目map
     * @throws IllegalStateException 如果检测到冲突
     */
    public static Map<Identifier, SimpleFileEntry> mergeEntries(
            Collection<SimpleFileEntry> entries) {
        Map<Identifier, SimpleFileEntry> result = new LinkedHashMap<>();
        
        for (SimpleFileEntry entry : entries) {
            Identifier id = entry.getIdentifier();
            
            switch (entry.getKind()) {
                case ADD:
                    if (result.containsKey(id)) {
                        throw new IllegalStateException(
                            "Trying to add file " + id + " which is already added. " +
                            "This indicates a conflict in concurrent writes.");
                    }
                    result.put(id, entry);
                    break;
                    
                case DELETE:
                    if (result.containsKey(id)) {
                        // ADD和DELETE抵消
                        result.remove(id);
                    } else {
                        // 保留DELETE标记（文件在之前的manifest中）
                        result.put(id, entry);
                    }
                    break;
                    
                default:
                    throw new IllegalArgumentException("Unknown file kind: " + entry.getKind());
            }
        }
        
        return result;
    }
    
    /**
     * 检查合并后的条目中是否有DELETE标记
     * 如果有，说明尝试删除不存在的文件（冲突）
     * 
     * @param mergedEntries 合并后的条目
     * @throws IllegalStateException 如果存在DELETE标记
     */
    public static void checkNoDeleteMarks(Map<Identifier, SimpleFileEntry> mergedEntries) {
        for (Map.Entry<Identifier, SimpleFileEntry> entry : mergedEntries.entrySet()) {
            if (entry.getValue().getKind() == ManifestEntry.FileKind.DELETE) {
                throw new IllegalStateException(
                    "Trying to delete file " + entry.getKey() + " which was not added. " +
                    "This indicates the file was already deleted by another writer.");
            }
        }
    }
    
    /**
     * 从ManifestEntry列表转换为SimpleFileEntry列表
     */
    public static List<SimpleFileEntry> fromManifestEntries(List<ManifestEntry> entries) {
        List<SimpleFileEntry> result = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            result.add(new SimpleFileEntry(entry));
        }
        return result;
    }
}
