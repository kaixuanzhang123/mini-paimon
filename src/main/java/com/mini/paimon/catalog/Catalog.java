package com.mini.paimon.catalog;

import com.mini.paimon.exception.CatalogException;
import com.mini.paimon.index.IndexType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.metadata.TableMetadata;
import com.mini.paimon.snapshot.Snapshot;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * Catalog 接口
 * 参考 Apache Paimon 的 Catalog 设计
 * 
 * Catalog 是 Mini Paimon 的核心元数据管理接口，负责：
 * 1. 数据库的创建、删除、列举
 * 2. 表的创建、删除、列举
 * 3. Schema 的管理
 * 4. Snapshot 的提交和查询
 * 5. 表元数据的管理
 * 
 * 职责边界：
 * - Catalog：负责元数据管理和持久化
 * - LSMTree：负责具体的数据读写
 * - SnapshotManager：负责快照的内部实现细节
 */
public interface Catalog extends Closeable {
    
    // ==================== 数据库操作 ====================
    
    /**
     * 创建数据库
     * 
     * @param database 数据库名
     * @param ignoreIfExists 如果已存在是否忽略异常
     * @throws CatalogException.DatabaseAlreadyExistException 数据库已存在（当 ignoreIfExists=false 时）
     */
    void createDatabase(String database, boolean ignoreIfExists) throws CatalogException;
    
    /**
     * 删除数据库
     * 
     * @param database 数据库名
     * @param ignoreIfNotExists 如果不存在是否忽略异常
     * @param cascade 是否级联删除数据库中的所有表
     * @throws CatalogException.DatabaseNotExistException 数据库不存在（当 ignoreIfNotExists=false 时）
     * @throws CatalogException.DatabaseNotEmptyException 数据库非空（当 cascade=false 时）
     */
    void dropDatabase(String database, boolean ignoreIfNotExists, boolean cascade) 
            throws CatalogException;
    
    /**
     * 列出所有数据库
     * 
     * @return 数据库名列表
     */
    List<String> listDatabases() throws CatalogException;
    
    /**
     * 检查数据库是否存在
     * 
     * @param database 数据库名
     * @return true 如果存在，否则 false
     */
    boolean databaseExists(String database) throws CatalogException;
    
    // ==================== 表操作 ====================
    
    /**
     * 创建表
     * 
     * @param identifier 表标识符
     * @param schema Schema 定义
     * @param ignoreIfExists 如果已存在是否忽略异常
     * @throws CatalogException.TableAlreadyExistException 表已存在（当 ignoreIfExists=false 时）
     * @throws CatalogException.DatabaseNotExistException 数据库不存在
     */
    void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists) 
            throws CatalogException;
    
    /**
     * 创建表（带选项）
     * 
     * @param identifier 表标识符
     * @param schema Schema 定义
     * @param options 表选项
     * @param ignoreIfExists 如果已存在是否忽略异常
     * @throws CatalogException.TableAlreadyExistException 表已存在（当 ignoreIfExists=false 时）
     * @throws CatalogException.DatabaseNotExistException 数据库不存在
     */
    void createTable(Identifier identifier, Schema schema, Map<String, String> options, 
                    boolean ignoreIfExists) throws CatalogException;
    
    /**
     * 创建表（带索引配置）
     * 
     * @param identifier 表标识符
     * @param schema Schema 定义
     * @param indexConfig 索引配置：字段名 -> 索引类型列表
     * @param ignoreIfExists 如果已存在是否忽略异常
     * @throws CatalogException.TableAlreadyExistException 表已存在（当 ignoreIfExists=false 时）
     * @throws CatalogException.DatabaseNotExistException 数据库不存在
     */
    void createTableWithIndex(Identifier identifier, Schema schema, 
                             Map<String, List<IndexType>> indexConfig, 
                             boolean ignoreIfExists) throws CatalogException;
    
    /**
     * 删除表
     * 
     * @param identifier 表标识符
     * @param ignoreIfNotExists 如果不存在是否忽略异常
     * @throws CatalogException.TableNotExistException 表不存在（当 ignoreIfNotExists=false 时）
     */
    void dropTable(Identifier identifier, boolean ignoreIfNotExists) throws CatalogException;
    
    /**
     * 重命名表
     * 
     * @param fromTable 原表标识符
     * @param toTable 新表标识符
     * @throws CatalogException.TableNotExistException 原表不存在
     * @throws CatalogException.TableAlreadyExistException 新表已存在
     */
    void renameTable(Identifier fromTable, Identifier toTable) throws CatalogException;
    
    /**
     * 修改表（Schema 演化）
     * 
     * @param identifier 表标识符
     * @param newFields 新字段列表
     * @param newPrimaryKeys 新主键列表
     * @param newPartitionKeys 新分区键列表
     * @return 新的 Schema 版本
     * @throws CatalogException.TableNotExistException 表不存在
     */
    Schema alterTable(Identifier identifier, List<Field> newFields, 
                     List<String> newPrimaryKeys, List<String> newPartitionKeys) 
            throws CatalogException;
    
    /**
     * 列出数据库中的所有表
     * 
     * @param database 数据库名
     * @return 表名列表
     * @throws CatalogException.DatabaseNotExistException 数据库不存在
     */
    List<String> listTables(String database) throws CatalogException;
    
    /**
     * 检查表是否存在
     * 
     * @param identifier 表标识符
     * @return true 如果存在，否则 false
     */
    boolean tableExists(Identifier identifier) throws CatalogException;
    
    // ==================== 元数据操作 ====================
    
    /**
     * 获取表的元数据
     * 
     * @param identifier 表标识符
     * @return 表元数据
     * @throws CatalogException.TableNotExistException 表不存在
     */
    TableMetadata getTableMetadata(Identifier identifier) throws CatalogException;
    
    /**
     * 获取表的当前 Schema
     * 
     * @param identifier 表标识符
     * @return Schema 对象
     * @throws CatalogException.TableNotExistException 表不存在
     */
    Schema getTableSchema(Identifier identifier) throws CatalogException;
    
    /**
     * 获取表的指定 Schema 版本
     * 
     * @param identifier 表标识符
     * @param schemaId Schema 版本 ID
     * @return Schema 对象
     * @throws CatalogException.TableNotExistException 表不存在
     */
    Schema getTableSchema(Identifier identifier, int schemaId) throws CatalogException;
    
    // ==================== Snapshot 操作 ====================
    
    /**
     * 提交快照（内部使用）
     * 此方法由存储引擎调用，不对外暴露
     * 
     * @param identifier 表标识符
     * @param snapshot 快照对象
     * @throws CatalogException.TableNotExistException 表不存在
     */
    void commitSnapshot(Identifier identifier, Snapshot snapshot) throws CatalogException;
    
    /**
     * 获取表的最新快照
     * 
     * @param identifier 表标识符
     * @return 最新快照，如果不存在返回 null
     * @throws CatalogException.TableNotExistException 表不存在
     */
    Snapshot getLatestSnapshot(Identifier identifier) throws CatalogException;
    
    /**
     * 获取表的指定快照
     * 
     * @param identifier 表标识符
     * @param snapshotId 快照 ID
     * @return 快照对象
     * @throws CatalogException.TableNotExistException 表不存在
     */
    Snapshot getSnapshot(Identifier identifier, long snapshotId) throws CatalogException;
    
    /**
     * 列出表的所有快照
     * 
     * @param identifier 表标识符
     * @return 快照列表（按 ID 升序）
     * @throws CatalogException.TableNotExistException 表不存在
     */
    List<Snapshot> listSnapshots(Identifier identifier) throws CatalogException;
    
    // ==================== Table 操作 ====================
    
    /**
     * 获取 Table 实例用于数据读写
     * 
     * @param identifier 表标识符
     * @return Table 实例
     * @throws CatalogException.TableNotExistException 表不存在
     */
    com.mini.paimon.table.Table getTable(Identifier identifier) throws CatalogException;
    
    // ==================== 生命周期管理 ====================
    
    /**
     * 获取 Catalog 名称
     */
    String name();
    
    /**
     * 获取默认数据库名
     */
    String getDefaultDatabase();
    
    /**
     * 关闭 Catalog，释放资源
     */
    @Override
    void close() throws CatalogException;
}
