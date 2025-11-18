package com.mini.paimon.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * 字段定义类
 * 表示表中的一个列
 */
public class Field {
    /** 字段名 */
    private final String name;
    
    /** 字段类型 */
    private final DataType type;
    
    /** 是否可为空 */
    private final boolean nullable;

    @JsonCreator
    public Field(
            @JsonProperty("name") String name,
            @JsonProperty("type") DataType type,
            @JsonProperty("nullable") boolean nullable) {
        this.name = Objects.requireNonNull(name, "Field name cannot be null");
        this.type = Objects.requireNonNull(type, "Field type cannot be null");
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field field = (Field) o;
        return nullable == field.nullable &&
                name.equals(field.name) &&
                type == field.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, nullable);
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", nullable=" + nullable +
                '}';
    }
}
