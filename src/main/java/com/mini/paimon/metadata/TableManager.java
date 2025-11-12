package com.mini.paimon.metadata;

import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 表管理器
 * 负责表的创建、删除和元数据管理
 */
public class TableManager {
    private static final Logger logger = LoggerFactory.getLogger(TableManager.class);
    
    /** 路径工厂 */
    private final PathFactory pathFactory;
    
    /** Schema 管理器缓存 */
    private final ConcurrentHashMap<String, SchemaManager> schemaManagerCache;
    
    public TableManager(PathFactory pathFactory) {
        this.pathFactory = pathFactory;
        this.schemaManagerCache = new ConcurrentHashMap<>();
    }

    /**
     * 创建新表
     * 
     * 参考 Paimon 设计：
     * - 不在表根目录下创建 metadata 文件
     * - 表的元数据通过 Schema 文件管理
     * 
     * @param database 数据库名
     * @param tableName 表名
     * @param fields 字段列表
     * @param primaryKeys 主键列表
     * @param partitionKeys 分区键列表
     * @return 表元数据
     * @throws IOException 创建失败
     */
    public synchronized TableMetadata createTable(
            String database, 
            String tableName,
            java.util.List<Field> fields,
            java.util.List<String> primaryKeys,
            java.util.List<String> partitionKeys) throws IOException {
        
        // 检查表是否已存在
        if (tableExists(database, tableName)) {
            throw new IOException("Table already exists: " + database + "." + tableName);
        }
        
        logger.info("Creating table {}/{}", database, tableName);
        
        // 创建表目录结构
        pathFactory.createTableDirectories(database, tableName);
        
        // 创建 Schema 管理器
        SchemaManager schemaManager = getSchemaManager(database, tableName);
        
        // 创建初始 Schema 版本
        Schema initialSchema = schemaManager.createNewSchemaVersion(fields, primaryKeys, partitionKeys);
        
        // 创建表元数据（仅用于内存管理，不持久化）
        TableMetadata tableMetadata = TableMetadata.newBuilder(tableName, database, initialSchema.getSchemaId())
                .build();
        
        // 关键修复：不再持久化 metadata 文件
        // Paimon 中表的元数据通过 Schema 文件管理
        // tableMetadata.persist(pathFactory);  // 删除此行
        
        logger.info("Successfully created table {}/{} with schema version {}", 
                   database, tableName, initialSchema.getSchemaId());
        
        return tableMetadata;
    }

    /**
     * 获取表元数据
     * 从 Schema 动态构建，不依赖 metadata 文件
     * 
     * @param database 数据库名
     * @param tableName 表名
     * @return 表元数据
     * @throws IOException 加载失败
     */
    public TableMetadata getTableMetadata(String database, String tableName) throws IOException {
        if (!tableExists(database, tableName)) {
            throw new IOException("Table not found: " + database + "." + tableName);
        }
        
        // 从 SchemaManager 获取当前 Schema
        SchemaManager schemaManager = getSchemaManager(database, tableName);
        Schema currentSchema = schemaManager.getCurrentSchema();
        
        // 动态构建 TableMetadata
        return new TableMetadata(tableName, database, currentSchema.getSchemaId());
    }

    /**
     * 获取 Schema 管理器
     * 
     * @param database 数据库名
     * @param tableName 表名
     * @return Schema 管理器
     */
    public SchemaManager getSchemaManager(String database, String tableName) {
        String key = database + "." + tableName;
        return schemaManagerCache.computeIfAbsent(key, k -> new SchemaManager(pathFactory, database, tableName));
    }

    /**
     * 检查表是否存在
     * 
     * 参考 Paimon 设计：
     * - 不依赖 metadata 文件
     * - 通过表目录和 schema 目录的存在性判断
     * 
     * @param database 数据库名
     * @param tableName 表名
     * @return true 如果表存在，否则 false
     */
    public boolean tableExists(String database, String tableName) {
        // 检查表目录是否存在
        java.nio.file.Path tablePath = pathFactory.getTablePath(database, tableName);
        if (!java.nio.file.Files.exists(tablePath)) {
            return false;
        }
        
        // 检查 schema 目录是否存在
        java.nio.file.Path schemaDir = pathFactory.getSchemaDir(database, tableName);
        return java.nio.file.Files.exists(schemaDir);
    }

    /**
     * 删除表
     * 
     * @param database 数据库名
     * @param tableName 表名
     * @throws IOException 删除失败
     */
    public synchronized void dropTable(String database, String tableName) throws IOException {
        if (!tableExists(database, tableName)) {
            throw new IOException("Table not found: " + database + "." + tableName);
        }
        
        // 删除表目录
        java.nio.file.Path tablePath = pathFactory.getTablePath(database, tableName);
        deleteDirectory(tablePath);
        
        // 从缓存中移除 Schema 管理器
        String key = database + "." + tableName;
        schemaManagerCache.remove(key);
        
        logger.info("Dropped table {}/{}", database, tableName);
    }

    /**
     * 递归删除目录
     */
    private void deleteDirectory(java.nio.file.Path path) throws IOException {
        if (java.nio.file.Files.exists(path)) {
            java.nio.file.Files.walk(path)
                .sorted(java.util.Comparator.reverseOrder())
                .map(java.nio.file.Path::toFile)
                .forEach(java.io.File::delete);
        }
    }

    /**
     * 列出数据库中的所有表
     * 
     * @param database 数据库名
     * @return 表名列表
     * @throws IOException 列出失败
     */
    public java.util.List<String> listTables(String database) throws IOException {
        java.nio.file.Path databasePath = pathFactory.getDatabasePath(database);
        if (!java.nio.file.Files.exists(databasePath)) {
            return new java.util.ArrayList<>();
        }
        
        java.util.List<String> tables = new java.util.ArrayList<>();
        try (java.util.stream.Stream<java.nio.file.Path> paths = java.nio.file.Files.list(databasePath)) {
            paths.filter(java.nio.file.Files::isDirectory)
                 .map(java.nio.file.Path::getFileName)
                 .map(java.nio.file.Path::toString)
                 .forEach(tables::add);
        }
        
        return tables;
    }

    /**
     * 获取路径工厂
     */
    public PathFactory getPathFactory() {
        return pathFactory;
    }
}
