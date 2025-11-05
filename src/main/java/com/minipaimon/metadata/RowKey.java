package com.minipaimon.metadata;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * RowKey 类
 * 表示主键的序列化形式，用于排序和比较
 */
public class RowKey implements Comparable<RowKey> {
    /** 序列化后的主键字节数组 */
    private final byte[] keyBytes;

    public RowKey(byte[] keyBytes) {
        this.keyBytes = Objects.requireNonNull(keyBytes, "Key bytes cannot be null");
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
     * 计算主键序列化后的大小
     */
    private static int calculateKeySize(Row row, Schema schema, List<Integer> pkIndices) {
        int size = 0;
        for (int index : pkIndices) {
            Object value = row.getValue(index);
            Field field = schema.getFields().get(index);
            
            switch (field.getType()) {
                case INT:
                    size += 4;
                    break;
                case LONG:
                    size += 8;
                    break;
                case BOOLEAN:
                    size += 1;
                    break;
                case STRING:
                    String str = (String) value;
                    size += 4 + str.getBytes(StandardCharsets.UTF_8).length; // 长度 + 内容
                    break;
            }
        }
        return size;
    }

    /**
     * 序列化单个值到ByteBuffer
     */
    private static void serializeValue(ByteBuffer buffer, Object value, DataType type) {
        switch (type) {
            case INT:
                buffer.putInt((Integer) value);
                break;
            case LONG:
                buffer.putLong((Long) value);
                break;
            case BOOLEAN:
                buffer.put((byte) (((Boolean) value) ? 1 : 0));
                break;
            case STRING:
                String str = (String) value;
                byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                buffer.putInt(bytes.length);
                buffer.put(bytes);
                break;
        }
    }

    public byte[] getBytes() {
        return Arrays.copyOf(keyBytes, keyBytes.length);
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
        return "RowKey{" + "bytes=" + Arrays.toString(keyBytes) + '}';
    }
}
