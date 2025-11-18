package com.mini.paimon.utils;

import com.mini.paimon.branch.BranchManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 路径工厂类
 * 负责生成标准的文件路径
 * 
 * 目录结构：
 * warehouse/
 * └── {database}/
 *     └── {table}/
 *         ├── schema/
 *         ├── snapshot/
 *         ├── manifest/
 *         ├── data/
 *         └── branch/
 *             └── branch-{name}/
 *                 ├── schema/
 *                 ├── snapshot/
 *                 ├── manifest/
 *                 └── data/
 */
public class PathFactory {
    private final String warehousePath;
    private final String branch;

    public PathFactory(String warehousePath) {
        this(warehousePath, BranchManager.DEFAULT_MAIN_BRANCH);
    }
    
    public PathFactory(String warehousePath, String branch) {
        this.warehousePath = warehousePath;
        this.branch = BranchManager.normalizeBranch(branch);
    }
    
    /**
     * 创建一个新的 PathFactory，使用指定的分支
     */
    public PathFactory copyWithBranch(String branchName) {
        return new PathFactory(warehousePath, branchName);
    }
    
    /**
     * 获取当前分支名
     */
    public String getBranch() {
        return branch;
    }

    /**
     * 获取数据库路径
     */
    public Path getDatabasePath(String database) {
        return Paths.get(warehousePath, database);
    }

    /**
     * 获取表路径（不考虑分支）
     */
    public Path getTablePath(String database, String table) {
        return Paths.get(warehousePath, database, table);
    }
    
    /**
     * 获取分支路径
     */
    private String getBranchPath(String database, String table) {
        Path tablePath = getTablePath(database, table);
        return BranchManager.branchPath(tablePath.toString(), branch);
    }

    /**
     * 获取Schema目录
     */
    public Path getSchemaDir(String database, String table) {
        return Paths.get(getBranchPath(database, table), "schema");
    }

    /**
     * 获取Schema文件路径
     */
    public Path getSchemaPath(String database, String table, int schemaId) {
        return getSchemaDir(database, table).resolve("schema-" + schemaId);
    }

    /**
     * 获取Snapshot目录
     */
    public Path getSnapshotDir(String database, String table) {
        return Paths.get(getBranchPath(database, table), "snapshot");
    }

    /**
     * 获取Snapshot文件路径
     */
    public Path getSnapshotPath(String database, String table, long snapshotId) {
        return getSnapshotDir(database, table).resolve("snapshot-" + snapshotId);
    }

    /**
     * 获取最新Snapshot标记文件路径（LATEST）
     */
    public Path getLatestSnapshotPath(String database, String table) {
        return getSnapshotDir(database, table).resolve("LATEST");
    }
    
    /**
     * 获取最早Snapshot标记文件路径（EARLIEST）
     */
    public Path getEarliestSnapshotPath(String database, String table) {
        return getSnapshotDir(database, table).resolve("EARLIEST");
    }

    /**
     * 获取Manifest目录
     */
    public Path getManifestDir(String database, String table) {
        return Paths.get(getBranchPath(database, table), "manifest");
    }

    /**
     * 获取Manifest List文件路径
     */
    public Path getManifestListPath(String database, String table, long snapshotId) {
        return getManifestDir(database, table).resolve("manifest-list-" + snapshotId);
    }
    
    /**
     * 获取Delta Manifest List文件路径
     */
    public Path getDeltaManifestListPath(String database, String table, long snapshotId) {
        return getManifestDir(database, table).resolve("manifest-list-delta-" + snapshotId);
    }
    
    /**
     * 获取Base Manifest List文件路径
     */
    public Path getBaseManifestListPath(String database, String table, long snapshotId) {
        return getManifestDir(database, table).resolve("manifest-list-base-" + snapshotId);
    }

    /**
     * 获取Manifest文件路径
     */
    public Path getManifestPath(String database, String table, String manifestId) {
        return getManifestDir(database, table).resolve("manifest-" + manifestId);
    }

    /**
     * 获取Data目录
     */
    public Path getDataDir(String database, String table) {
        return Paths.get(getBranchPath(database, table), "data");
    }

    /**
     * 获取WAL目录
     */
    public Path getWalDir(String database, String table) {
        return Paths.get(getBranchPath(database, table), "wal");
    }

    /**
     * 获取WAL文件路径
     */
    public Path getWalPath(String database, String table, long sequence) {
        return getWalDir(database, table).resolve(String.format("wal-%03d.log", sequence));
    }

    /**
     * 获取SSTable文件路径
     */
    public Path getSSTPath(String database, String table, int level, long sequence) {
        return getDataDir(database, table).resolve(
                String.format("data-%d-%03d.sst", level, sequence));
    }
    
    public Path getTempDir(String database, String table) {
        return Paths.get(getBranchPath(database, table), "tmp");
    }
    
    public Path getTempFilePath(String database, String table, String tempFileName) {
        return getTempDir(database, table).resolve(tempFileName);
    }
    
    public Path getSSTPathWithBucket(String database, String table, String partitionPath, 
                                    int bucket, int level, long sequence) {
        return Paths.get(getBranchPath(database, table))
            .resolve(partitionPath)
            .resolve("bucket-" + bucket)
            .resolve(String.format("data-%d-%03d.sst", level, sequence));
    }
    
    /**
     * 获取分区 Bucket 目录
     * 
     * @param database 数据库名
     * @param table 表名
     * @param partitionPath 分区路径
     * @param bucket Bucket ID
     * @return Bucket 目录路径
     */
    public Path getBucketDir(String database, String table, String partitionPath, int bucket) {
        return Paths.get(getBranchPath(database, table))
            .resolve(partitionPath)
            .resolve("bucket-" + bucket);
    }

    /**
     * 创建表的目录结构
     */
    public void createTableDirectories(String database, String table) throws IOException {
        Files.createDirectories(getSchemaDir(database, table));
        //Files.createDirectories(getSnapshotDir(database, table));
        //Files.createDirectories(getManifestDir(database, table));
        Files.createDirectories(getDataDir(database, table));
        Files.createDirectories(getWalDir(database, table));
    }

    /**
     * 检查表是否存在
     */
    public boolean tableExists(String database, String table) {
        return Files.exists(getTablePath(database, table));
    }

    public String getWarehousePath() {
        return warehousePath;
    }
}
