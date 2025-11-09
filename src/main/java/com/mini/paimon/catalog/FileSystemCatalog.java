package com.mini.paimon.catalog;

import com.mini.paimon.exception.CatalogException;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.metadata.SchemaManager;
import com.mini.paimon.metadata.TableManager;
import com.mini.paimon.metadata.TableMetadata;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.table.Table;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 基于文件系统的 Catalog 实现
 * 参考 Apache Paimon 的 FileSystemCatalog 设计
 * 
 * 核心职责：
 * 1. 元数据管理：数据库、表、Schema 的创建、删除、查询
 * 2. Snapshot 管理：快照的提交、查询、列举
 * 3. 资源管理：Manager 的缓存和生命周期管理
 */
public class FileSystemCatalog implements Catalog {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemCatalog.class);
    
    /** Catalog 名称 */
    private final String catalogName;
    
    /** 默认数据库 */
    private final String defaultDatabase;
    
    /** 路径工厂 */
    private final PathFactory pathFactory;
    
    /** 表管理器（单例，跨所有表共享）*/
    private final TableManager tableManager;
    
    /** Schema 管理器缓存（每个表一个）*/
    private final ConcurrentHashMap<Identifier, SchemaManager> schemaManagerCache;
    
    /** Snapshot 管理器缓存（每个表一个）*/
    private final ConcurrentHashMap<Identifier, SnapshotManager> snapshotManagerCache;
    
    /** Catalog 是否已关闭 */
    private volatile boolean closed = false;
    
    /**
     * 创建文件系统 Catalog
     * 
     * @param catalogName Catalog 名称
     * @param context Catalog 上下文配置
     */
    public FileSystemCatalog(String catalogName, CatalogContext context) {
        this(catalogName, "default", context);
    }
    
    /**
     * 创建文件系统 Catalog
     * 
     * @param catalogName Catalog 名称
     * @param defaultDatabase 默认数据库
     * @param context Catalog 上下文配置
     */
    public FileSystemCatalog(String catalogName, String defaultDatabase, CatalogContext context) {
        this.catalogName = catalogName;
        this.defaultDatabase = defaultDatabase;
        this.pathFactory = new PathFactory(context.getWarehouse());
        this.tableManager = new TableManager(pathFactory);
        this.schemaManagerCache = new ConcurrentHashMap<>();
        this.snapshotManagerCache = new ConcurrentHashMap<>();
        
        logger.info("Initialized FileSystemCatalog '{}' with warehouse: {}", 
                   catalogName, context.getWarehouse());
    }
    
    // ==================== 数据库操作 ====================
    
    @Override
    public void createDatabase(String database, boolean ignoreIfExists) throws CatalogException {
        checkNotClosed();
        
        try {
            Path databasePath = pathFactory.getDatabasePath(database);
            
            if (Files.exists(databasePath)) {
                if (!ignoreIfExists) {
                    throw new CatalogException.DatabaseAlreadyExistException(database);
                }
                logger.debug("Database already exists, ignoring: {}", database);
                return;
            }
            
            Files.createDirectories(databasePath);
            logger.info("Created database: {}", database);
            
        } catch (IOException e) {
            throw new CatalogException("Failed to create database: " + database, e);
        }
    }
    
    @Override
    public void dropDatabase(String database, boolean ignoreIfNotExists, boolean cascade) 
            throws CatalogException {
        checkNotClosed();
        
        try {
            Path databasePath = pathFactory.getDatabasePath(database);
            
            if (!Files.exists(databasePath)) {
                if (!ignoreIfNotExists) {
                    throw new CatalogException.DatabaseNotExistException(database);
                }
                logger.debug("Database does not exist, ignoring: {}", database);
                return;
            }
            
            // 检查数据库是否为空
            List<String> tables = listTables(database);
            if (!tables.isEmpty() && !cascade) {
                throw new CatalogException.DatabaseNotEmptyException(database);
            }
            
            // 级联删除所有表
            if (cascade) {
                for (String table : tables) {
                    dropTable(new Identifier(database, table), true);
                }
            }
            
            // 删除数据库目录
            deleteDirectory(databasePath);
            logger.info("Dropped database: {}", database);
            
        } catch (IOException e) {
            throw new CatalogException("Failed to drop database: " + database, e);
        }
    }
    
    @Override
    public List<String> listDatabases() throws CatalogException {
        checkNotClosed();
        
        try {
            Path warehousePath = java.nio.file.Paths.get(pathFactory.getWarehousePath());
            
            if (!Files.exists(warehousePath)) {
                return new ArrayList<>();
            }
            
            try (Stream<Path> paths = Files.list(warehousePath)) {
                return paths
                    .filter(Files::isDirectory)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .collect(Collectors.toList());
            }
            
        } catch (IOException e) {
            throw new CatalogException("Failed to list databases", e);
        }
    }
    
    @Override
    public boolean databaseExists(String database) throws CatalogException {
        checkNotClosed();
        
        Path databasePath = pathFactory.getDatabasePath(database);
        return Files.exists(databasePath);
    }
    
    // ==================== 表操作 ====================
    
    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists) 
            throws CatalogException {
        createTable(identifier, schema, new ConcurrentHashMap<>(), ignoreIfExists);
    }
    
    @Override
    public void createTable(Identifier identifier, Schema schema, Map<String, String> options,
                          boolean ignoreIfExists) throws CatalogException {
        checkNotClosed();
        
        String database = identifier.getDatabase();
        String table = identifier.getTable();
        
        try {
            // 检查数据库是否存在
            if (!databaseExists(database)) {
                throw new CatalogException.DatabaseNotExistException(database);
            }
            
            // 检查表是否已存在
            if (tableExists(identifier)) {
                if (!ignoreIfExists) {
                    throw new CatalogException.TableAlreadyExistException(identifier);
                }
                logger.debug("Table already exists, ignoring: {}", identifier);
                return;
            }
            
            // 创建表
            tableManager.createTable(
                database, 
                table, 
                schema.getFields(), 
                schema.getPrimaryKeys(),
                schema.getPartitionKeys()
            );
            
            logger.info("Created table: {}", identifier);
            
        } catch (IOException e) {
            throw new CatalogException("Failed to create table: " + identifier, e);
        }
    }
    
    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists) throws CatalogException {
        checkNotClosed();
        
        String database = identifier.getDatabase();
        String table = identifier.getTable();
        
        try {
            // 检查表是否存在
            if (!tableExists(identifier)) {
                if (!ignoreIfNotExists) {
                    throw new CatalogException.TableNotExistException(identifier);
                }
                logger.debug("Table does not exist, ignoring: {}", identifier);
                return;
            }
            
            // 从缓存中移除
            schemaManagerCache.remove(identifier);
            snapshotManagerCache.remove(identifier);
            
            // 删除表
            tableManager.dropTable(database, table);
            logger.info("Dropped table: {}", identifier);
            
        } catch (IOException e) {
            throw new CatalogException("Failed to drop table: " + identifier, e);
        }
    }
    
    @Override
    public void renameTable(Identifier fromTable, Identifier toTable) throws CatalogException {
        checkNotClosed();
        
        try {
            // 检查原表是否存在
            if (!tableExists(fromTable)) {
                throw new CatalogException.TableNotExistException(fromTable);
            }
            
            // 检查新表是否已存在
            if (tableExists(toTable)) {
                throw new CatalogException.TableAlreadyExistException(toTable);
            }
            
            // 检查是否跨数据库重命名
            if (!fromTable.getDatabase().equals(toTable.getDatabase())) {
                throw new CatalogException(
                    "Cannot rename table across databases: " + fromTable + " -> " + toTable);
            }
            
            // 执行重命名（移动目录）
            Path fromPath = pathFactory.getTablePath(fromTable.getDatabase(), fromTable.getTable());
            Path toPath = pathFactory.getTablePath(toTable.getDatabase(), toTable.getTable());
            Files.move(fromPath, toPath);
            
            // 更新缓存
            SchemaManager schemaManager = schemaManagerCache.remove(fromTable);
            if (schemaManager != null) {
                schemaManagerCache.put(toTable, 
                    new SchemaManager(pathFactory, toTable.getDatabase(), toTable.getTable()));
            }
            
            SnapshotManager snapshotManager = snapshotManagerCache.remove(fromTable);
            if (snapshotManager != null) {
                snapshotManagerCache.put(toTable, 
                    new SnapshotManager(pathFactory, toTable.getDatabase(), toTable.getTable()));
            }
            
            logger.info("Renamed table from {} to {}", fromTable, toTable);
            
        } catch (IOException e) {
            throw new CatalogException("Failed to rename table: " + fromTable + " -> " + toTable, e);
        }
    }
    
    @Override
    public Schema alterTable(Identifier identifier, List<Field> newFields,
                           List<String> newPrimaryKeys, List<String> newPartitionKeys) 
            throws CatalogException {
        checkNotClosed();
        
        try {
            // 检查表是否存在
            if (!tableExists(identifier)) {
                throw new CatalogException.TableNotExistException(identifier);
            }
            
            // 获取 SchemaManager
            SchemaManager schemaManager = getSchemaManager(identifier);
            
            // 创建新的 Schema 版本
            Schema newSchema = schemaManager.createNewSchemaVersion(
                newFields, newPrimaryKeys, newPartitionKeys);
            
            logger.info("Altered table {}, new schema version: {}", 
                       identifier, newSchema.getSchemaId());
            
            return newSchema;
            
        } catch (IOException e) {
            throw new CatalogException("Failed to alter table: " + identifier, e);
        }
    }
    
    @Override
    public List<String> listTables(String database) throws CatalogException {
        checkNotClosed();
        
        try {
            // 检查数据库是否存在
            if (!databaseExists(database)) {
                throw new CatalogException.DatabaseNotExistException(database);
            }
            
            return tableManager.listTables(database);
            
        } catch (IOException e) {
            throw new CatalogException("Failed to list tables in database: " + database, e);
        }
    }
    
    @Override
    public boolean tableExists(Identifier identifier) throws CatalogException {
        checkNotClosed();
        
        return tableManager.tableExists(identifier.getDatabase(), identifier.getTable());
    }
    
    // ==================== 元数据操作 ====================
    
    @Override
    public TableMetadata getTableMetadata(Identifier identifier) throws CatalogException {
        checkNotClosed();
        
        try {
            // 检查表是否存在
            if (!tableExists(identifier)) {
                throw new CatalogException.TableNotExistException(identifier);
            }
            
            return tableManager.getTableMetadata(identifier.getDatabase(), identifier.getTable());
            
        } catch (IOException e) {
            throw new CatalogException("Failed to get table metadata: " + identifier, e);
        }
    }
    
    @Override
    public Schema getTableSchema(Identifier identifier) throws CatalogException {
        checkNotClosed();
        
        try {
            // 检查表是否存在
            if (!tableExists(identifier)) {
                throw new CatalogException.TableNotExistException(identifier);
            }
            
            SchemaManager schemaManager = getSchemaManager(identifier);
            return schemaManager.getCurrentSchema();
            
        } catch (IOException e) {
            throw new CatalogException("Failed to get table schema: " + identifier, e);
        }
    }
    
    @Override
    public Schema getTableSchema(Identifier identifier, int schemaId) throws CatalogException {
        checkNotClosed();
        
        try {
            // 检查表是否存在
            if (!tableExists(identifier)) {
                throw new CatalogException.TableNotExistException(identifier);
            }
            
            SchemaManager schemaManager = getSchemaManager(identifier);
            return schemaManager.loadSchema(schemaId);
            
        } catch (IOException e) {
            throw new CatalogException(
                "Failed to get table schema: " + identifier + ", schemaId: " + schemaId, e);
        }
    }
    
    // ==================== Snapshot 操作 ====================
    
    @Override
    public void commitSnapshot(Identifier identifier, Snapshot snapshot) throws CatalogException {
        checkNotClosed();
        
        try {
            // 检查表是否存在
            if (!tableExists(identifier)) {
                throw new CatalogException.TableNotExistException(identifier);
            }
            
            // 持久化快照
            snapshot.persist(pathFactory, identifier.getDatabase(), identifier.getTable());
            
            logger.info("Committed snapshot {} for table {}", snapshot.getId(), identifier);
            
        } catch (IOException e) {
            throw new CatalogException("Failed to commit snapshot for table: " + identifier, e);
        }
    }
    
    @Override
    public Snapshot getLatestSnapshot(Identifier identifier) throws CatalogException {
        checkNotClosed();
        
        try {
            // 检查表是否存在
            if (!tableExists(identifier)) {
                throw new CatalogException.TableNotExistException(identifier);
            }
            
            SnapshotManager snapshotManager = getSnapshotManager(identifier);
            
            if (!snapshotManager.hasSnapshot()) {
                return null;
            }
            
            return snapshotManager.getLatestSnapshot();
            
        } catch (IOException e) {
            throw new CatalogException("Failed to get latest snapshot for table: " + identifier, e);
        }
    }
    
    @Override
    public Snapshot getSnapshot(Identifier identifier, long snapshotId) throws CatalogException {
        checkNotClosed();
        
        try {
            // 检查表是否存在
            if (!tableExists(identifier)) {
                throw new CatalogException.TableNotExistException(identifier);
            }
            
            SnapshotManager snapshotManager = getSnapshotManager(identifier);
            return snapshotManager.getSnapshot(snapshotId);
            
        } catch (IOException e) {
            throw new CatalogException(
                "Failed to get snapshot for table: " + identifier + ", snapshotId: " + snapshotId, e);
        }
    }
    
    @Override
    public List<Snapshot> listSnapshots(Identifier identifier) throws CatalogException {
        checkNotClosed();
        
        try {
            // 检查表是否存在
            if (!tableExists(identifier)) {
                throw new CatalogException.TableNotExistException(identifier);
            }
            
            SnapshotManager snapshotManager = getSnapshotManager(identifier);
            return snapshotManager.getAllSnapshots();
            
        } catch (IOException e) {
            throw new CatalogException("Failed to list snapshots for table: " + identifier, e);
        }
    }
    
    // ==================== Table 操作 ====================
    
    @Override
    public Table getTable(Identifier identifier) throws CatalogException {
        checkNotClosed();
        
        if (!tableExists(identifier)) {
            throw new CatalogException.TableNotExistException(identifier);
        }
        
        Schema schema = getTableSchema(identifier);
        return new com.mini.paimon.table.FileStoreTable(this, identifier, schema, pathFactory);
    }
    
    // ==================== 生命周期管理 ====================
    
    @Override
    public String name() {
        return catalogName;
    }
    
    @Override
    public String getDefaultDatabase() {
        return defaultDatabase;
    }
    
    @Override
    public void close() throws CatalogException {
        if (closed) {
            return;
        }
        
        closed = true;
        
        // 清理缓存
        schemaManagerCache.clear();
        snapshotManagerCache.clear();
        
        logger.info("Closed catalog: {}", catalogName);
    }
    
    // ==================== 私有辅助方法 ====================
    
    /**
     * 获取 SchemaManager（带缓存）
     */
    private SchemaManager getSchemaManager(Identifier identifier) {
        return schemaManagerCache.computeIfAbsent(identifier, id -> 
            new SchemaManager(pathFactory, id.getDatabase(), id.getTable()));
    }
    
    /**
     * 获取 SnapshotManager（带缓存）
     */
    private SnapshotManager getSnapshotManager(Identifier identifier) {
        return snapshotManagerCache.computeIfAbsent(identifier, id -> 
            new SnapshotManager(pathFactory, id.getDatabase(), id.getTable()));
    }
    
    /**
     * 检查 Catalog 是否已关闭
     */
    private void checkNotClosed() throws CatalogException {
        if (closed) {
            throw new CatalogException("Catalog has been closed: " + catalogName);
        }
    }
    
    /**
     * 递归删除目录
     */
    private void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            try (Stream<Path> walk = Files.walk(path)) {
                walk.sorted((p1, p2) -> -p1.compareTo(p2))
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
}
