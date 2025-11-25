package com.mini.paimon.branch;

import com.mini.paimon.schema.SchemaManager;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * 基于文件系统的分支管理器实现
 * 参考 Apache Paimon 的 FileSystemBranchManager
 */
public class FileSystemBranchManager implements BranchManager {
    
    private static final Logger logger = LoggerFactory.getLogger(FileSystemBranchManager.class);
    
    private final PathFactory pathFactory;
    private final String database;
    private final String table;
    private final SnapshotManager snapshotManager;
    private final SchemaManager schemaManager;
    
    public FileSystemBranchManager(
            PathFactory pathFactory,
            String database,
            String table,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager) {
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
    }
    
    @Override
    public void createBranch(String branchName) {
        BranchManager.validateBranch(branchName);
        
        try {
            // 获取最新的 Schema 并复制到分支目录
            Schema latestSchema = schemaManager.getCurrentSchema();
            copySchemasToBranch(branchName, latestSchema.getSchemaId());
            
            logger.info("Created empty branch '{}' for table {}/{}", branchName, database, table);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create branch '" + branchName + "'", e);
        }
    }
    
    @Override
    public void createBranch(String branchName, String tagName) {
        if (tagName == null || tagName.trim().isEmpty()) {
            // 如果没有指定 Tag，创建空分支
            createBranch(branchName);
            return;
        }
        
        BranchManager.validateBranch(branchName);
        
        // 注意：当前实现不支持 Tag，所以从最新 Snapshot 创建分支
        // TODO: 实现 Tag 支持后，这里应该从指定的 Tag 创建分支
        logger.warn("Tag support not implemented yet, creating branch from latest snapshot");
        
        try {
            if (!snapshotManager.hasSnapshot()) {
                throw new IllegalStateException(
                    "Cannot create branch from tag: no snapshots exist for table " + database + "/" + table);
            }
            
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();
            
            // 复制 Snapshot 文件到分支目录
            Path sourceSnapshotPath = pathFactory.getSnapshotPath(database, table, latestSnapshot.getId());
            PathFactory branchPathFactory = pathFactory.copyWithBranch(branchName);
            Path targetSnapshotPath = branchPathFactory.getSnapshotPath(database, table, latestSnapshot.getId());
            
            Files.createDirectories(targetSnapshotPath.getParent());
            Files.copy(sourceSnapshotPath, targetSnapshotPath);
            
            // 复制 LATEST hint 文件
            Path sourceLatest = pathFactory.getLatestSnapshotPath(database, table);
            if (Files.exists(sourceLatest)) {
                Path targetLatest = branchPathFactory.getLatestSnapshotPath(database, table);
                Files.copy(sourceLatest, targetLatest);
            }
            
            // 复制 EARLIEST hint 文件
            Path sourceEarliest = pathFactory.getEarliestSnapshotPath(database, table);
            if (Files.exists(sourceEarliest)) {
                Path targetEarliest = branchPathFactory.getEarliestSnapshotPath(database, table);
                Files.copy(sourceEarliest, targetEarliest);
            }
            
            // 复制相关的 Schema 文件
            copySchemasToBranch(branchName, latestSnapshot.getSchemaId());
            
            logger.info("Created branch '{}' from latest snapshot {} for table {}/{}", 
                       branchName, latestSnapshot.getId(), database, table);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create branch '" + branchName + "' from tag", e);
        }
    }
    
    @Override
    public void dropBranch(String branchName) {
        if (!branchExists(branchName)) {
            throw new IllegalArgumentException("Branch '" + branchName + "' does not exist.");
        }
        
        try {
            Path branchPath = getBranchPath(branchName);
            if (Files.exists(branchPath)) {
                // 递归删除整个分支目录
                deleteDirectory(branchPath);
                logger.info("Dropped branch '{}' for table {}/{}", branchName, database, table);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to drop branch '" + branchName + "'", e);
        }
    }
    
    @Override
    public List<String> branches() {
        List<String> branchList = new ArrayList<>();
        
        try {
            Path tablePath = pathFactory.getTablePath(database, table);
            Path branchDir = tablePath.resolve("branch");
            
            if (!Files.exists(branchDir)) {
                return branchList;
            }
            
            try (Stream<Path> paths = Files.list(branchDir)) {
                paths.filter(Files::isDirectory)
                     .map(path -> path.getFileName().toString())
                     .filter(name -> name.startsWith(BRANCH_PREFIX))
                     .map(name -> name.substring(BRANCH_PREFIX.length()))
                     .forEach(branchList::add);
            }
        } catch (IOException e) {
            logger.error("Failed to list branches for table {}/{}", database, table, e);
        }
        
        return branchList;
    }
    
    /**
     * 复制 Schema 文件到分支目录
     * 
     * @param branchName 分支名
     * @param schemaId Schema ID
     */
    private void copySchemasToBranch(String branchName, int schemaId) throws IOException {
        PathFactory branchPathFactory = pathFactory.copyWithBranch(branchName);
        
        // 复制从 schema-0 到指定 schemaId 的所有 Schema 文件
        for (int i = 0; i <= schemaId; i++) {
            Path sourcePath = pathFactory.getSchemaPath(database, table, i);
            if (Files.exists(sourcePath)) {
                Path targetPath = branchPathFactory.getSchemaPath(database, table, i);
                Files.createDirectories(targetPath.getParent());
                Files.copy(sourcePath, targetPath);
                logger.debug("Copied schema-{} to branch '{}'", i, branchName);
            }
        }
    }
    
    /**
     * 获取分支路径
     */
    private Path getBranchPath(String branchName) {
        Path tablePath = pathFactory.getTablePath(database, table);
        return Paths.get(BranchManager.branchPath(tablePath.toString(), branchName));
    }
    
    /**
     * 递归删除目录
     */
    private void deleteDirectory(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        
        try (Stream<Path> paths = Files.walk(directory)) {
            paths.sorted((p1, p2) -> -p1.compareTo(p2)) // 反向排序，先删除文件再删除目录
                 .forEach(path -> {
                     try {
                         Files.delete(path);
                     } catch (IOException e) {
                         logger.warn("Failed to delete {}", path, e);
                     }
                 });
        }
    }
}

