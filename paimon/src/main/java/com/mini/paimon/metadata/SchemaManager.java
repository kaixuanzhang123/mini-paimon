package com.mini.paimon.metadata;

import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Schema 管理器
 * 负责表结构的持久化、版本管理和缓存
 */
public class SchemaManager {
    private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);
    
    /** 路径工厂 */
    private final PathFactory pathFactory;
    
    /** 数据库名 */
    private final String database;
    
    /** 表名 */
    private final String table;
    
    /** Schema 缓存 */
    private final ConcurrentHashMap<Integer, Schema> schemaCache;
    
    /** 当前最大 Schema ID */
    private final AtomicInteger currentSchemaId;
    
    /** 当前活跃的 Schema */
    private volatile Schema currentSchema;

    public SchemaManager(PathFactory pathFactory, String database, String table) {
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.schemaCache = new ConcurrentHashMap<>();
        this.currentSchemaId = new AtomicInteger(-1);
        this.currentSchema = null;
    }

    /**
     * 创建新的 Schema 版本
     * Schema ID 由系统自动分配：获取当前最大 ID 并加 1
     * 
     * @param fields 字段列表
     * @param primaryKeys 主键列表
     * @param partitionKeys 分区键列表
     * @return 新的 Schema
     * @throws IOException 序列化异常
     */
    public synchronized Schema createNewSchemaVersion(
            java.util.List<Field> fields,
            java.util.List<String> primaryKeys,
            java.util.List<String> partitionKeys) throws IOException {
        
        // 自动生成 Schema ID：如果是第一次创建，先查找现有的最大 ID
        if (currentSchemaId.get() < 0) {
            int maxId = findMaxSchemaId();
            if (maxId >= 0) {
                currentSchemaId.set(maxId);
            }
        }
        
        int newSchemaId = currentSchemaId.incrementAndGet();
        Schema newSchema = new Schema(newSchemaId, fields, primaryKeys, partitionKeys);
        
        // 持久化到磁盘
        persistSchema(newSchema);
        
        // 更新缓存和当前 Schema
        schemaCache.put(newSchemaId, newSchema);
        this.currentSchema = newSchema;
        
        logger.info("Created new schema version {} for table {}/{}", newSchemaId, database, table);
        
        return newSchema;
    }

    /**
     * 持久化 Schema 到磁盘
     * 
     * @param schema 要持久化的 Schema
     * @throws IOException 序列化异常
     */
    private void persistSchema(Schema schema) throws IOException {
        Path schemaPath = pathFactory.getSchemaPath(database, table, schema.getSchemaId());
        
        // 确保目录存在
        Files.createDirectories(schemaPath.getParent());
        
        // 序列化 Schema 到文件
        SerializationUtils.writeToFile(schemaPath, schema);
        
        logger.debug("Persisted schema {} to {}", schema.getSchemaId(), schemaPath);
    }

    /**
     * 从磁盘加载 Schema
     * 
     * @param schemaId Schema ID
     * @return Schema 对象
     * @throws IOException 反序列化异常
     */
    public Schema loadSchema(int schemaId) throws IOException {
        // 先从缓存中查找
        Schema cachedSchema = schemaCache.get(schemaId);
        if (cachedSchema != null) {
            return cachedSchema;
        }
        
        // 从磁盘加载
        Path schemaPath = pathFactory.getSchemaPath(database, table, schemaId);
        if (!Files.exists(schemaPath)) {
            throw new IOException("Schema file not found: " + schemaPath);
        }
        
        Schema schema = SerializationUtils.readFromFile(schemaPath, Schema.class);
        
        // 放入缓存
        schemaCache.put(schemaId, schema);
        
        // 更新当前最大 Schema ID
        if (schemaId > currentSchemaId.get()) {
            currentSchemaId.set(schemaId);
            this.currentSchema = schema;
        }
        
        logger.debug("Loaded schema {} from {}", schemaId, schemaPath);
        
        return schema;
    }

    /**
     * 获取当前活跃的 Schema
     * 
     * @return 当前 Schema
     * @throws IOException 如果没有 Schema 或加载失败
     */
    public Schema getCurrentSchema() throws IOException {
        if (currentSchema != null) {
            return currentSchema;
        }
        
        // 尝试加载最新的 Schema
        return loadLatestSchema();
    }

    /**
     * 加载最新的 Schema
     * 
     * @return 最新的 Schema
     * @throws IOException 如果没有 Schema 或加载失败
     */
    public Schema loadLatestSchema() throws IOException {
        // 查找最大的 Schema ID
        int maxSchemaId = findMaxSchemaId();
        if (maxSchemaId < 0) {
            throw new IOException("No schema found for table " + database + "/" + table);
        }
        
        return loadSchema(maxSchemaId);
    }

    /**
     * 查找最大的 Schema ID
     * 
     * @return 最大的 Schema ID，如果没有找到返回 -1
     * @throws IOException IO 异常
     */
    private int findMaxSchemaId() throws IOException {
        Path schemaDir = pathFactory.getSchemaDir(database, table);
        if (!Files.exists(schemaDir)) {
            return -1;
        }
        
        int maxId = -1;
        try (java.util.stream.Stream<java.nio.file.Path> files = Files.list(schemaDir)) {
            java.util.Iterator<java.nio.file.Path> iterator = files.iterator();
            while (iterator.hasNext()) {
                Path file = iterator.next();
                String fileName = file.getFileName().toString();
                if (fileName.startsWith("schema-")) {
                    try {
                        int id = Integer.parseInt(fileName.substring(7)); // "schema-".length() = 7
                        if (id > maxId) {
                            maxId = id;
                        }
                    } catch (NumberFormatException e) {
                        // 忽略无效的文件名
                        logger.debug("Ignoring invalid schema file: {}", fileName);
                    }
                }
            }
        }
        
        // 更新当前最大 Schema ID
        if (maxId > currentSchemaId.get()) {
            currentSchemaId.set(maxId);
        }
        
        return maxId;
    }

    /**
     * 检查表是否存在 Schema
     * 
     * @return true 如果存在 Schema，否则 false
     */
    public boolean hasSchema() {
        try {
            return findMaxSchemaId() >= 0;
        } catch (IOException e) {
            logger.warn("Error checking schema existence", e);
            return false;
        }
    }

    /**
     * 获取 Schema 版本数量
     * 
     * @return Schema 版本数量
     */
    public int getSchemaVersionCount() {
        try {
            Path schemaDir = pathFactory.getSchemaDir(database, table);
            if (!Files.exists(schemaDir)) {
                return 0;
            }
            
            try (java.util.stream.Stream<java.nio.file.Path> files = Files.list(schemaDir)) {
                return (int) files.filter(file -> file.getFileName().toString().startsWith("schema-")).count();
            }
        } catch (IOException e) {
            logger.warn("Error counting schema versions", e);
            return 0;
        }
    }

    /**
     * 获取数据库名
     */
    public String getDatabase() {
        return database;
    }

    /**
     * 获取表名
     */
    public String getTable() {
        return table;
    }

    /**
     * 获取路径工厂
     */
    public PathFactory getPathFactory() {
        return pathFactory;
    }
}
