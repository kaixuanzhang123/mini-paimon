package com.mini.paimon.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;

/**
 * 数据行
 * 表示一行数据，包含字段值数组
 */
public class Row {
    /** 字段值数组 */
    private final Object[] values;

    /**
     * 构造函数
     * 
     * @param values 字段值数组
     */
    @JsonCreator
    public Row(@JsonProperty("values") Object[] values) {
        this.values = values != null ? values.clone() : new Object[0];
    }

    /**
     * 获取字段值数组
     * 
     * @return 字段值数组的副本
     */
    public Object[] getValues() {
        return values != null ? values.clone() : new Object[0];
    }

    /**
     * 获取指定索引的字段值
     * 
     * @param index 字段索引
     * @return 字段值
     */
    public Object getValue(int index) {
        if (values == null || index < 0 || index >= values.length) {
            return null;
        }
        return values[index];
    }

    /**
     * 获取字段数量
     * 
     * @return 字段数量
     */
    @JsonIgnore
    public int getFieldCount() {
        return values != null ? values.length : 0;
    }

    /**
     * 验证行数据与Schema的兼容性
     * 
     * @param schema Schema定义
     * @throws IllegalArgumentException 如果数据不兼容
     */
    public void validate(Schema schema) {
        List<Field> fields = schema.getFields();
        
        // 检查字段数量
        if (values == null) {
            if (!fields.isEmpty()) {
                throw new IllegalArgumentException("Row has no values but schema has " + fields.size() + " fields");
            }
            return;
        }
        
        if (values.length != fields.size()) {
            throw new IllegalArgumentException(
                    "Row field count mismatch. Expected: " + fields.size() + ", Actual: " + values.length);
        }

        // 检查每个字段的类型兼容性
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            Object value = values[i];

            // 检查非空约束
            if (!field.isNullable() && value == null) {
                throw new IllegalArgumentException("Field '" + field.getName() + "' cannot be null");
            }

            // 检查类型匹配
            if (value != null && !isTypeCompatible(value, field.getType())) {
                throw new IllegalArgumentException(
                        "Field '" + field.getName() + "' type mismatch. Expected: " + 
                        field.getType() + ", Actual: " + value.getClass().getSimpleName());
            }
        }
    }

    /**
     * 检查值的类型是否与DataType兼容
     */
    private boolean isTypeCompatible(Object value, DataType type) {
        return type.isCompatible(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row row = (Row) o;
        return Arrays.equals(values, row.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return "Row{" + "values=" + Arrays.toString(values) + '}';
    }
}