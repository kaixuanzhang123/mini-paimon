package com.minipaimon.utils;

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
 *         └── data/
 */
public class PathFactory {
    private final String warehousePath;

    public PathFactory(String warehousePath) {
        this.warehousePath = warehousePath;
    }

    /**
     * 获取数据库路径
     */
    public Path getDatabasePath(String database) {
        return Paths.get(warehousePath, database);
    }

    /**
     * 获取表路径
     */
    public Path getTablePath(String database, String table) {
        return Paths.get(warehousePath, database, table);
    }

    /**
     * 获取Schema目录
     */
    public Path getSchemaDir(String database, String table) {
        return Paths.get(warehousePath, database, table, "schema");
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
        return Paths.get(warehousePath, database, table, "snapshot");
    }

    /**
     * 获取Snapshot文件路径
     */
    public Path getSnapshotPath(String database, String table, long snapshotId) {
        return getSnapshotDir(database, table).resolve("snapshot-" + snapshotId);
    }

    /**
     * 获取最新Snapshot标记文件路径
     */
    public Path getLatestSnapshotPath(String database, String table) {
        return getSnapshotDir(database, table).resolve("LATEST");
    }

    /**
     * 获取Manifest目录
     */
    public Path getManifestDir(String database, String table) {
        return Paths.get(warehousePath, database, table, "manifest");
    }

    /**
     * 获取Manifest List文件路径
     */
    public Path getManifestListPath(String database, String table, long snapshotId) {
        return getManifestDir(database, table).resolve("manifest-list-" + snapshotId);
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
        return Paths.get(warehousePath, database, table, "data");
    }

    /**
     * 获取SSTable文件路径
     */
    public Path getSSTPath(String database, String table, int level, long sequence) {
        return getDataDir(database, table).resolve(
                String.format("data-%d-%03d.sst", level, sequence));
    }

    /**
     * 创建表的目录结构
     */
    public void createTableDirectories(String database, String table) throws IOException {
        Files.createDirectories(getSchemaDir(database, table));
        Files.createDirectories(getSnapshotDir(database, table));
        Files.createDirectories(getManifestDir(database, table));
        Files.createDirectories(getDataDir(database, table));
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
