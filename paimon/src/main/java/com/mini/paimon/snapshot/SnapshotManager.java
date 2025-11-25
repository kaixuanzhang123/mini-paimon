package com.mini.paimon.snapshot;

import com.mini.paimon.branch.BranchManager;
import com.mini.paimon.io.SnapshotFile;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Snapshot Manager
 * 参考 Apache Paimon 的 SnapshotManager 设计
 * 注意：不包含创建 snapshot 的业务逻辑（由 FileStoreCommitImpl 负责）
 * 
 * @see SnapshotFile 负责 Snapshot 的 I/O 操作
 */
public class SnapshotManager {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);
    
    private final PathFactory pathFactory;
    private final String database;
    private final String table;
    private final String branch;
    
    // 新增：使用 SnapshotFile 进行 I/O 操作（对齐 Manifest 设计）
    private final SnapshotFile snapshotFile;
    
    public SnapshotManager(PathFactory pathFactory, String database, String table) {
        this(pathFactory, database, table, null);
    }
    
    public SnapshotManager(PathFactory pathFactory, String database, String table, String branch) {
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.branch = BranchManager.normalizeBranch(branch);
        
        // 初始化 SnapshotFile（对齐 Paimon 设计）
        this.snapshotFile = new SnapshotFile(pathFactory, database, table);
    }
    
    /**
     * 创建一个新的SnapshotManager，使用指定的分支
     */
    public SnapshotManager copyWithBranch(String branchName) {
        return new SnapshotManager(
            pathFactory.copyWithBranch(branchName),
            database,
            table,
            branchName
        );
    }
    
    /**
     * 获取当前分支名
     */
    public String branch() {
        return branch;
    }
    
    /**
     * 计算snapshot文件路径
     */
    public Path snapshotPath(long snapshotId) {
        return pathFactory.getSnapshotPath(database, table, snapshotId);
    }
    
    /**
     * 获取snapshot目录
     */
    public Path snapshotDirectory() {
        return pathFactory.getSnapshotDir(database, table);
    }
    
    /**
     * 读取指定 ID 的 snapshot（使用 SnapshotFile）
     * 
     * @param snapshotId snapshot ID
     * @return snapshot 对象
     * @throws IOException 读取失败
     */
    public Snapshot snapshot(long snapshotId) throws IOException {
        return snapshotFile.readSnapshot(snapshotId);
    }
    
    /**
     * 读取最新snapshot
     * 
     * @return 最新的snapshot，如果不存在返回null
     * @throws IOException 读取失败
     */
    public Snapshot latestSnapshot() throws IOException {
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return null;
        }
        return snapshot(latestId);
    }
    
    /**
     * 获取最新snapshot ID
     * 
     * @return 最新ID，如果不存在返回null
     */
    public Long latestSnapshotId() {
        try {
            Path latestPath = pathFactory.getLatestSnapshotPath(database, table);
            if (!Files.exists(latestPath)) {
                return null;
            }
            
            String content = new String(Files.readAllBytes(latestPath)).trim();
            return Long.parseLong(content);
            
        } catch (Exception e) {
            logger.warn("Failed to read latest snapshot ID", e);
            return null;
        }
    }
    
    /**
     * 获取最早snapshot ID
     * 
     * @return 最早ID，如果不存在返回null
     */
    public Long earliestSnapshotId() {
        try {
            Path earliestPath = pathFactory.getEarliestSnapshotPath(database, table);
            if (!Files.exists(earliestPath)) {
                // 如果EARLIEST不存在，扫描目录找最小ID
                return findEarliestSnapshotIdFromDirectory();
            }
            
            String content = new String(Files.readAllBytes(earliestPath)).trim();
            return Long.parseLong(content);
            
        } catch (Exception e) {
            logger.warn("Failed to read earliest snapshot ID", e);
            return null;
        }
    }
    
    /**
     * 读取最早snapshot
     * 
     * @return 最早的snapshot，如果不存在返回null
     * @throws IOException 读取失败
     */
    public Snapshot earliestSnapshot() throws IOException {
        Long earliestId = earliestSnapshotId();
        if (earliestId == null) {
            return null;
        }
        return snapshot(earliestId);
    }
    
    /**
     * 检查是否存在snapshot
     */
    public boolean hasSnapshot() {
        return latestSnapshotId() != null;
    }
    
    /**
     * 检查指定 snapshot 是否存在（使用 SnapshotFile）
     */
    public boolean snapshotExists(long snapshotId) {
        return snapshotFile.snapshotExists(snapshotId);
    }
    
    /**
     * 获取所有snapshot（按ID升序）
     */
    public List<Snapshot> listAllSnapshots() throws IOException {
        List<Snapshot> snapshots = new ArrayList<>();
        Path snapshotDir = snapshotDirectory();
        
        if (!Files.exists(snapshotDir)) {
            return snapshots;
        }
        
        try (Stream<Path> files = Files.list(snapshotDir)) {
            files.filter(path -> path.getFileName().toString().startsWith("snapshot-"))
                 .sorted(Comparator.comparing(path -> {
                     String fileName = path.getFileName().toString();
                     return Long.parseLong(fileName.substring("snapshot-".length()));
                 }))
                 .forEach(path -> {
                     try {
                         String fileName = path.getFileName().toString();
                         long snapshotId = Long.parseLong(fileName.substring("snapshot-".length()));
                         snapshots.add(snapshot(snapshotId));
                     } catch (IOException e) {
                         logger.warn("Failed to load snapshot: {}", path, e);
                     }
                 });
        }
        
        return snapshots;
    }
    
    /**
     * 获取snapshot路径列表（按ID升序）
     */
    public List<Path> listSnapshotPaths() throws IOException {
        List<Path> paths = new ArrayList<>();
        Path snapshotDir = snapshotDirectory();
        
        if (!Files.exists(snapshotDir)) {
            return paths;
        }
        
        try (Stream<Path> files = Files.list(snapshotDir)) {
            files.filter(path -> path.getFileName().toString().startsWith("snapshot-"))
                 .sorted(Comparator.comparing(path -> {
                     String fileName = path.getFileName().toString();
                     return Long.parseLong(fileName.substring("snapshot-".length()));
                 }))
                 .forEach(paths::add);
        }
        
        return paths;
    }
    
    /**
     * 更新latest hint文件
     */
    public void commitLatestHint(long snapshotId) throws IOException {
        Path latestPath = pathFactory.getLatestSnapshotPath(database, table);
        Files.createDirectories(latestPath.getParent());
        Files.write(latestPath, String.valueOf(snapshotId).getBytes());
        logger.debug("Updated latest snapshot hint to {}", snapshotId);
    }
    
    /**
     * 更新earliest hint文件（只在不存在时创建）
     */
    public void commitEarliestHint(long snapshotId) throws IOException {
        Path earliestPath = pathFactory.getEarliestSnapshotPath(database, table);
        if (!Files.exists(earliestPath)) {
            Files.createDirectories(earliestPath.getParent());
            Files.write(earliestPath, String.valueOf(snapshotId).getBytes());
            logger.debug("Created earliest snapshot hint: {}", snapshotId);
        }
    }
    
    /**
     * 删除latest hint文件
     */
    public void deleteLatestHint() throws IOException {
        Path latestPath = pathFactory.getLatestSnapshotPath(database, table);
        Files.deleteIfExists(latestPath);
    }
    
    /**
     * 删除earliest hint文件
     */
    public void deleteEarliestHint() throws IOException {
        Path earliestPath = pathFactory.getEarliestSnapshotPath(database, table);
        Files.deleteIfExists(earliestPath);
    }
    
    /**
     * 从目录中查找最早的snapshot ID
     */
    private Long findEarliestSnapshotIdFromDirectory() {
        try {
            Path snapshotDir = snapshotDirectory();
            if (!Files.exists(snapshotDir)) {
                return null;
            }
            
            try (Stream<Path> files = Files.list(snapshotDir)) {
                return files.filter(path -> path.getFileName().toString().startsWith("snapshot-"))
                    .map(path -> {
                        String fileName = path.getFileName().toString();
                        try {
                            return Long.parseLong(fileName.substring("snapshot-".length()));
                        } catch (NumberFormatException e) {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .min(Long::compareTo)
                    .orElse(null);
            }
        } catch (IOException e) {
            logger.warn("Failed to scan snapshot directory", e);
            return null;
        }
    }
}
