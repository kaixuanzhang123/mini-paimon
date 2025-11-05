package com.minipaimon.metadata;

import java.util.Arrays;
import java.util.Objects;

/**
 * Row 类
 * 表示一行数据
 */
public class Row {
    /** 数据值数组，顺序与Schema中的字段顺序一致 */
    private final Object[] values;

    public Row(Object[] values) {
        this.values = Objects.requireNonNull(values, "Values cannot be null");
    }

    public Row(int fieldCount) {
        this.values = new Object[fieldCount];
    }

    /**
     * 获取字段值
     */
    public Object getValue(int index) {
        if (index < 0 || index >= values.length) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + values.length);
        }
        return values[index];
    }

    /**
     * 设置字段值
     */
    public void setValue(int index, Object value) {
        if (index < 0 || index >= values.length) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + values.length);
        }
        values[index] = value;
    }

    /**
     * 获取字段数量
     */
    public int getFieldCount() {
        return values.length;
    }

    /**
     * 获取所有值
     */
    public Object[] getValues() {
        return Arrays.copyOf(values, values.length);
    }

    /**
     * 检查指定位置的值是否为null
     */
    public boolean isNull(int index) {
        return getValue(index) == null;
    }

    /**
     * 根据Schema验证Row的有效性
     */
    public void validate(Schema schema) {
        if (values.length != schema.getFields().size()) {
            throw new IllegalArgumentException(
                    "Row field count (" + values.length + 
                    ") does not match schema (" + schema.getFields().size() + ")");
        }

        for (int i = 0; i < values.length; i++) {
            Field field = schema.getFields().get(i);
            Object value = values[i];

            // 检查非空约束
            if (value == null && !field.isNullable()) {
                throw new IllegalArgumentException(
                        "Field '" + field.getName() + "' cannot be null");
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
        switch (type) {
            case INT:
                return value instanceof Integer;
            case LONG:
                return value instanceof Long;
            case STRING:
                return value instanceof String;
            case BOOLEAN:
                return value instanceof Boolean;
            default:
                return false;
        }
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
