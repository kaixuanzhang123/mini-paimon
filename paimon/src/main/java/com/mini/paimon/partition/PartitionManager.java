package com.mini.paimon.partition;

import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 分区管理器
 * 参考 Paimon PartitionManager 设计
 * 负责分区的创建、删除、查询等操作
 */
public class PartitionManager {
    private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);
    
    private final PathFactory pathFactory;
    private final String database;
    private final String table;
    private final List<String> partitionKeys;
    
    public PartitionManager(PathFactory pathFactory, String database, String table, List<String> partitionKeys) {
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.partitionKeys = new ArrayList<>(partitionKeys);
    }
    
    /**
     * 创建分区目录
     */
    public void createPartition(PartitionSpec partitionSpec) throws IOException {
        if (!isValidPartitionSpec(partitionSpec)) {
            throw new IllegalArgumentException("Invalid partition spec: " + partitionSpec);
        }
        
        Path partitionPath = getPartitionPath(partitionSpec);
        if (!Files.exists(partitionPath)) {
            Files.createDirectories(partitionPath);
            logger.info("Created partition: {}", partitionSpec);
        }
    }
    
    /**
     * 删除分区
     */
    public void dropPartition(PartitionSpec partitionSpec) throws IOException {
        // 安全检查：禁止删除空的 partitionSpec，防止删除整个表目录
        if (partitionSpec == null || partitionSpec.isEmpty()) {
            throw new IllegalArgumentException(
                "Cannot drop partition with empty PartitionSpec. " +
                "This would delete the entire table directory. " +
                "Use dropTable() to delete the table.");
        }
        
        Path partitionPath = getPartitionPath(partitionSpec);
        if (Files.exists(partitionPath)) {
            deleteDirectory(partitionPath);
            logger.info("Dropped partition: {}", partitionSpec);
        }
    }
    
    /**
     * 列出所有分区
     */
    public List<PartitionSpec> listPartitions() throws IOException {
        Path tablePath = pathFactory.getTablePath(database, table);
        
        if (!Files.exists(tablePath)) {
            return Collections.emptyList();
        }
        
        if (partitionKeys.isEmpty()) {
            return Collections.emptyList();
        }
        
        return scanPartitions(tablePath, 0);
    }
    
    /**
     * 递归扫描分区目录
     */
    private List<PartitionSpec> scanPartitions(Path currentPath, int level) throws IOException {
        if (level >= partitionKeys.size()) {
            return Collections.emptyList();
        }
        
        List<PartitionSpec> partitions = new ArrayList<>();
        String currentKey = partitionKeys.get(level);
        
        try (Stream<Path> paths = Files.list(currentPath)) {
            List<Path> subdirs = paths.filter(Files::isDirectory)
                .filter(p -> p.getFileName().toString().startsWith(currentKey + "="))
                .collect(Collectors.toList());
            
            for (Path subdir : subdirs) {
                String dirName = subdir.getFileName().toString();
                String value = dirName.substring(currentKey.length() + 1);
                
                if (level == partitionKeys.size() - 1) {
                    // 最后一层，构造完整的分区规范
                    Map<String, String> values = new LinkedHashMap<>();
                    values.put(currentKey, value);
                    partitions.add(new PartitionSpec(values));
                } else {
                    // 继续递归
                    List<PartitionSpec> subPartitions = scanPartitions(subdir, level + 1);
                    for (PartitionSpec subPartition : subPartitions) {
                        Map<String, String> values = new LinkedHashMap<>();
                        values.put(currentKey, value);
                        values.putAll(subPartition.getPartitionValues());
                        partitions.add(new PartitionSpec(values));
                    }
                }
            }
        }
        
        return partitions;
    }
    
    /**
     * 检查分区是否存在
     */
    public boolean partitionExists(PartitionSpec partitionSpec) {
        Path partitionPath = getPartitionPath(partitionSpec);
        return Files.exists(partitionPath);
    }
    
    /**
     * 获取分区路径
     */
    public Path getPartitionPath(PartitionSpec partitionSpec) {
        Path tablePath = pathFactory.getTablePath(database, table);
        if (partitionSpec == null || partitionSpec.isEmpty()) {
            throw new IllegalArgumentException(
                "PartitionSpec cannot be empty. " +
                "To get table path, use pathFactory.getTablePath() directly.");
        }
        return Paths.get(tablePath.toString(), partitionSpec.toPath());
    }
    
    /**
     * 验证分区规范是否有效
     */
    private boolean isValidPartitionSpec(PartitionSpec partitionSpec) {
        if (partitionSpec.isEmpty() && partitionKeys.isEmpty()) {
            return true;
        }
        
        Set<String> specKeys = partitionSpec.getPartitionValues().keySet();
        return specKeys.equals(new LinkedHashSet<>(partitionKeys));
    }
    
    /**
     * 递归删除目录
     */
    private void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            try (Stream<Path> walk = Files.walk(path)) {
                walk.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            logger.warn("Failed to delete: {}", p, e);
                        }
                    });
            }
        }
    }
    
    public List<String> getPartitionKeys() {
        return Collections.unmodifiableList(partitionKeys);
    }
}
