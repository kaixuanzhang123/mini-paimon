package com.mini.paimon.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mini.paimon.index.IndexType;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.utils.SerializationUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 表元数据
 * 管理表级别信息，如表名、Schema ID、创建时间等
 */
public class TableMetadata {
    /** 表名 */
    private final String tableName;
    
    /** 数据库名 */
    private final String databaseName;
    
    /** 当前 Schema ID */
    private final int currentSchemaId;
    
    /** 创建时间 */
    private final Instant createTime;
    
    /** 最后修改时间 */
    private final Instant lastModifiedTime;
    
    /** 表描述（可选） */
    private final String description;
    
    /** 表属性（可选） */
    private final java.util.Map<String, String> properties;
    
    /** 索引配置：字段名 -> 索引类型列表 */
    private final java.util.Map<String, java.util.List<IndexType>> indexConfig;

    @JsonCreator
    public TableMetadata(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("currentSchemaId") int currentSchemaId,
            @JsonProperty("createTime") Instant createTime,
            @JsonProperty("lastModifiedTime") Instant lastModifiedTime,
            @JsonProperty("description") String description,
            @JsonProperty("properties") java.util.Map<String, String> properties,
            @JsonProperty("indexConfig") java.util.Map<String, java.util.List<IndexType>> indexConfig) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.currentSchemaId = currentSchemaId;
        this.createTime = createTime != null ? createTime : Instant.now();
        this.lastModifiedTime = lastModifiedTime != null ? lastModifiedTime : Instant.now();
        this.description = description != null ? description : "";
        this.properties = properties != null ? new java.util.HashMap<>(properties) : new java.util.HashMap<>();
        this.indexConfig = indexConfig != null ? new java.util.HashMap<>(indexConfig) : new java.util.HashMap<>();
    }

    public TableMetadata(String tableName, String databaseName, int currentSchemaId) {
        this(tableName, databaseName, currentSchemaId, Instant.now(), Instant.now(), "", new java.util.HashMap<>(), new java.util.HashMap<>());
    }

    /**
     * 获取表名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 获取数据库名
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * 获取当前 Schema ID
     */
    public int getCurrentSchemaId() {
        return currentSchemaId;
    }

    /**
     * 获取创建时间
     */
    public Instant getCreateTime() {
        return createTime;
    }

    /**
     * 获取最后修改时间
     */
    public Instant getLastModifiedTime() {
        return lastModifiedTime;
    }

    /**
     * 获取表描述
     */
    public String getDescription() {
        return description;
    }

    /**
     * 获取表属性
     */
    public java.util.Map<String, String> getProperties() {
        return new java.util.HashMap<>(properties);
    }
    
    /**
     * 获取索引配置
     */
    public java.util.Map<String, java.util.List<IndexType>> getIndexConfig() {
        return new java.util.HashMap<>(indexConfig);
    }

    /**
     * 获取指定属性值
     */
    public String getProperty(String key) {
        return properties.get(key);
    }

    /**
     * 检查是否存在指定属性
     */
    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }

    /**
     * 持久化表元数据到文件
     * 
     * @param pathFactory 路径工厂
     * @throws IOException 序列化异常
     */
    public void persist(PathFactory pathFactory) throws IOException {
        Path metadataPath = pathFactory.getTablePath(databaseName, tableName).resolve("metadata");
        Files.createDirectories(metadataPath.getParent());
        SerializationUtils.writeToFile(metadataPath, this);
    }

    /**
     * 从文件加载表元数据
     * 
     * @param pathFactory 路径工厂
     * @param databaseName 数据库名
     * @param tableName 表名
     * @return 表元数据
     * @throws IOException 反序列化异常
     */
    public static TableMetadata load(PathFactory pathFactory, String databaseName, String tableName) throws IOException {
        Path metadataPath = pathFactory.getTablePath(databaseName, tableName).resolve("metadata");
        if (!Files.exists(metadataPath)) {
            throw new IOException("Table metadata file not found: " + metadataPath);
        }
        return SerializationUtils.readFromFile(metadataPath, TableMetadata.class);
    }

    /**
     * 检查表元数据文件是否存在
     * 
     * @param pathFactory 路径工厂
     * @param databaseName 数据库名
     * @param tableName 表名
     * @return true 如果存在，否则 false
     */
    public static boolean exists(PathFactory pathFactory, String databaseName, String tableName) {
        try {
            Path metadataPath = pathFactory.getTablePath(databaseName, tableName).resolve("metadata");
            return Files.exists(metadataPath);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 创建表元数据构建器
     */
    public static Builder newBuilder(String tableName, String databaseName, int currentSchemaId) {
        return new Builder(tableName, databaseName, currentSchemaId);
    }

    /**
     * 表元数据构建器
     */
    public static class Builder {
        private String tableName;
        private String databaseName;
        private int currentSchemaId;
        private Instant createTime;
        private Instant lastModifiedTime;
        private String description;
        private java.util.Map<String, String> properties;
        private java.util.Map<String, java.util.List<IndexType>> indexConfig;

        public Builder(String tableName, String databaseName, int currentSchemaId) {
            this.tableName = tableName;
            this.databaseName = databaseName;
            this.currentSchemaId = currentSchemaId;
            this.createTime = Instant.now();
            this.lastModifiedTime = Instant.now();
            this.description = "";
            this.properties = new java.util.HashMap<>();
            this.indexConfig = new java.util.HashMap<>();
        }

        public Builder createTime(Instant createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder lastModifiedTime(Instant lastModifiedTime) {
            this.lastModifiedTime = lastModifiedTime;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder property(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        public Builder properties(java.util.Map<String, String> properties) {
            this.properties.putAll(properties);
            return this;
        }
        
        public Builder indexConfig(java.util.Map<String, java.util.List<IndexType>> indexConfig) {
            this.indexConfig = indexConfig;
            return this;
        }

        public TableMetadata build() {
            return new TableMetadata(
                tableName, databaseName, currentSchemaId,
                createTime, lastModifiedTime, description, properties, indexConfig
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        TableMetadata that = (TableMetadata) o;
        
        if (currentSchemaId != that.currentSchemaId) return false;
        if (!tableName.equals(that.tableName)) return false;
        if (!databaseName.equals(that.databaseName)) return false;
        if (!createTime.equals(that.createTime)) return false;
        if (!lastModifiedTime.equals(that.lastModifiedTime)) return false;
        if (!description.equals(that.description)) return false;
        return properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        int result = tableName.hashCode();
        result = 31 * result + databaseName.hashCode();
        result = 31 * result + currentSchemaId;
        result = 31 * result + createTime.hashCode();
        result = 31 * result + lastModifiedTime.hashCode();
        result = 31 * result + description.hashCode();
        result = 31 * result + properties.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TableMetadata{" +
                "tableName='" + tableName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", currentSchemaId=" + currentSchemaId +
                ", createTime=" + createTime +
                ", lastModifiedTime=" + lastModifiedTime +
                ", description='" + description + '\'' +
                ", properties=" + properties +
                '}';
    }
}
