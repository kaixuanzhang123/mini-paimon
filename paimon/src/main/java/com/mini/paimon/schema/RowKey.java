package com.mini.paimon.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * RowKey 类
 * 表示主键的序列化形式，用于排序和比较
 */
public class RowKey implements Comparable<RowKey>, java.io.Serializable {
    private static final long serialVersionUID = 1L;
    /** 序列化后的主键字节数组 */
    private byte[] keyBytes;
    
    /** 无参构造函数，仅供 Jackson 反序列化使用 */
    public RowKey() {
        this.keyBytes = new byte[0]; // 仅供 Jackson 使用
    }

    @JsonCreator
    public RowKey(@JsonProperty("keyBytes") byte[] keyBytes) {
        this.keyBytes = Objects.requireNonNull(keyBytes, "Key bytes cannot be null");
    }
    
    /**
     * 从单个整数值创建 RowKey（用于测试）
     */
    public static RowKey of(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        return new RowKey(buffer.array());
    }
    
    /**
     * 从多个整数值创建 RowKey（用于测试）
     */
    public static RowKey of(int... values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * 4);
        for (int value : values) {
            buffer.putInt(value);
        }
        return new RowKey(buffer.array());
    }

    /**
     * 从Row中提取主键并序列化
     * 
     * @param row 数据行
     * @param schema 表结构
     * @return RowKey对象
     */
    public static RowKey fromRow(Row row, Schema schema) {
        List<Integer> pkIndices = schema.getPrimaryKeyIndices();
        
        // 计算所需的字节数
        int totalSize = calculateKeySize(row, schema, pkIndices);
        
        // 序列化主键值
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        for (int index : pkIndices) {
            Object value = row.getValue(index);
            Field field = schema.getFields().get(index);
            
            if (value == null) {
                throw new IllegalArgumentException(
                        "Primary key field '" + field.getName() + "' cannot be null");
            }
            
            serializeValue(buffer, value, field.getType());
        }
        
        return new RowKey(buffer.array());
    }

    /**
     * 从主键值数组创建 RowKey
     * 
     * @param keyValues 主键值数组
     * @param schema 表结构
     * @return RowKey对象
     */
    public static RowKey fromValues(Object[] keyValues, Schema schema) {
        if (!schema.hasPrimaryKey()) {
            throw new IllegalArgumentException("Schema has no primary key");
        }
        
        List<String> primaryKeys = schema.getPrimaryKeys();
        if (keyValues.length != primaryKeys.size()) {
            throw new IllegalArgumentException(
                String.format("Key values count mismatch: expected %d, got %d", 
                    primaryKeys.size(), keyValues.length));
        }
        
        // 计算所需的字节数
        int totalSize = 0;
        List<Field> fields = schema.getFields();
        for (int i = 0; i < primaryKeys.size(); i++) {
            String pkName = primaryKeys.get(i);
            Field field = null;
            for (Field f : fields) {
                if (f.getName().equals(pkName)) {
                    field = f;
                    break;
                }
            }
            if (field == null) {
                throw new IllegalArgumentException("Primary key field not found: " + pkName);
            }
            
            DataType type = field.getType();
            if (type instanceof DataType.IntType) {
                totalSize += 4;
            } else if (type instanceof DataType.LongType) {
                totalSize += 8;
            } else if (type instanceof DataType.BooleanType) {
                totalSize += 1;
            } else if (type instanceof DataType.StringType) {
                String str = keyValues[i] != null ? keyValues[i].toString() : "";
                totalSize += 4 + str.getBytes(StandardCharsets.UTF_8).length;
            } else {
                throw new IllegalArgumentException("Unsupported primary key type: " + type);
            }
        }
        
        // 序列化主键值
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        for (int i = 0; i < primaryKeys.size(); i++) {
            String pkName = primaryKeys.get(i);
            Field field = null;
            for (Field f : fields) {
                if (f.getName().equals(pkName)) {
                    field = f;
                    break;
                }
            }
            
            Object value = keyValues[i];
            if (value == null) {
                throw new IllegalArgumentException(
                    "Primary key field '" + pkName + "' cannot be null");
            }
            
            serializeValue(buffer, value, field.getType());
        }
        
        return new RowKey(buffer.array());
    }

    /**
     * 计算主键序列化后的大小
     */
    private static int calculateKeySize(Row row, Schema schema, List<Integer> pkIndices) {
        int size = 0;
        for (int index : pkIndices) {
            Object value = row.getValue(index);
            Field field = schema.getFields().get(index);
            DataType type = field.getType();
            
            if (type instanceof DataType.IntType) {
                size += 4;
            } else if (type instanceof DataType.LongType) {
                size += 8;
            } else if (type instanceof DataType.BooleanType) {
                size += 1;
            } else if (type instanceof DataType.StringType) {
                String str = value != null ? value.toString() : "";
                size += 4 + str.getBytes(StandardCharsets.UTF_8).length;
            } else {
                throw new IllegalArgumentException("Unsupported primary key type: " + type);
            }
        }
        return size;
    }

    /**
     * 序列化单个值到ByteBuffer
     */
    private static void serializeValue(ByteBuffer buffer, Object value, DataType type) {
        if (type instanceof DataType.IntType) {
            if (value instanceof Integer) {
                buffer.putInt((Integer) value);
            } else if (value instanceof Long) {
                buffer.putInt(((Long) value).intValue());
            } else {
                buffer.putInt(((Number) value).intValue());
            }
        } else if (type instanceof DataType.LongType) {
            if (value instanceof Long) {
                buffer.putLong((Long) value);
            } else if (value instanceof Integer) {
                buffer.putLong(((Integer) value).longValue());
            } else {
                buffer.putLong(((Number) value).longValue());
            }
        } else if (type instanceof DataType.BooleanType) {
            buffer.put((byte) (((Boolean) value) ? 1 : 0));
        } else if (type instanceof DataType.StringType) {
            String str = (String) value;
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        } else {
            throw new IllegalArgumentException("Unsupported primary key type: " + type);
        }
    }

    @JsonIgnore
    public byte[] getBytes() {
        return Arrays.copyOf(keyBytes, keyBytes.length);
    }
    
    /**
     * 获取键字节，供 Jackson 序列化使用
     * @return 键字节
     */
    @JsonProperty("keyBytes")
    public byte[] getKeyBytes() {
        return keyBytes;
    }
    
    /**
     * 设置键字节，仅供 Jackson 反序列化使用
     * @param keyBytes 键字节
     */
    public void setKeyBytes(byte[] keyBytes) {
        if (this.keyBytes == null || this.keyBytes.length == 0) {
            this.keyBytes = keyBytes;
        }
    }

    public int size() {
        return keyBytes.length;
    }

    @Override
    public int compareTo(RowKey other) {
        // 字节序比较
        int minLen = Math.min(this.keyBytes.length, other.keyBytes.length);
        for (int i = 0; i < minLen; i++) {
            int b1 = this.keyBytes[i] & 0xFF;
            int b2 = other.keyBytes[i] & 0xFF;
            if (b1 != b2) {
                return b1 - b2;
            }
        }
        return this.keyBytes.length - other.keyBytes.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowKey rowKey = (RowKey) o;
        return Arrays.equals(keyBytes, rowKey.keyBytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(keyBytes);
    }

    @Override
    public String toString() {
        return "RowKey{keyBytes=" + Arrays.toString(keyBytes) + '}';
    }
}