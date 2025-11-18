package com.mini.paimon.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Schema 类
 * 表示表的结构定义，包括字段列表和主键定义
 */
public class Schema {
    /** Schema 版本ID */
    private final int schemaId;
    
    /** 字段列表 */
    private final List<Field> fields;
    
    /** 主键字段名列表 */
    private final List<String> primaryKeys;
    
    /** 分区键字段名列表（可选） */
    private final List<String> partitionKeys;

    public Schema(int schemaId, List<Field> fields, List<String> primaryKeys) {
        this(schemaId, fields, primaryKeys, Collections.emptyList());
    }

    public Schema(int schemaId, List<Field> fields) {
        this(schemaId, fields, Collections.emptyList(), Collections.emptyList());
    }

    @JsonCreator
    public Schema(
            @JsonProperty("schemaId") int schemaId,
            @JsonProperty("fields") List<Field> fields,
            @JsonProperty("primaryKeys") List<String> primaryKeys,
            @JsonProperty("partitionKeys") List<String> partitionKeys) {
        this.schemaId = schemaId;
        this.fields = new ArrayList<>(Objects.requireNonNull(fields, "Fields cannot be null"));
        this.primaryKeys = new ArrayList<>(Objects.requireNonNull(primaryKeys, "Primary keys cannot be null"));
        this.partitionKeys = new ArrayList<>(Objects.requireNonNull(partitionKeys, "Partition keys cannot be null"));
        
        validate();
    }

    /**
     * 验证Schema的有效性
     */
    private void validate() {
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Schema must have at least one field");
        }
        
        // 验证主键字段存在（如果定义了主键）
        List<String> fieldNames = fields.stream()
                .map(Field::getName)
                .collect(Collectors.toList());
        
        for (String pkName : primaryKeys) {
            if (!fieldNames.contains(pkName)) {
                throw new IllegalArgumentException("Primary key field not found: " + pkName);
            }
        }
        
        // 验证分区键字段存在
        for (String partitionKey : partitionKeys) {
            if (!fieldNames.contains(partitionKey)) {
                throw new IllegalArgumentException("Partition key field not found: " + partitionKey);
            }
        }
    }

    public int getSchemaId() {
        return schemaId;
    }

    public List<Field> getFields() {
        return Collections.unmodifiableList(fields);
    }

    public List<String> getPrimaryKeys() {
        return Collections.unmodifiableList(primaryKeys);
    }

    public List<String> getPartitionKeys() {
        return Collections.unmodifiableList(partitionKeys);
    }

    /**
     * 根据字段名获取字段
     */
    public Field getField(String name) {
        return fields.stream()
                .filter(f -> f.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    /**
     * 获取字段索引
     */
    public int getFieldIndex(String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 获取主键字段索引列表
     */
    public List<Integer> getPrimaryKeyIndices() {
        return primaryKeys.stream()
                .map(this::getFieldIndex)
                .collect(Collectors.toList());
    }

    /**
     * 是否有主键
     */
    public boolean hasPrimaryKey() {
        return !primaryKeys.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;
        return schemaId == schema.schemaId &&
                fields.equals(schema.fields) &&
                primaryKeys.equals(schema.primaryKeys) &&
                partitionKeys.equals(schema.partitionKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaId, fields, primaryKeys, partitionKeys);
    }

    @Override
    public String toString() {
        return "Schema{" +
                "schemaId=" + schemaId +
                ", fields=" + fields +
                ", primaryKeys=" + primaryKeys +
                ", partitionKeys=" + partitionKeys +
                '}';
    }
}
