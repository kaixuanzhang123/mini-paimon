package com.mini.paimon.data;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Binary Row
 * 参考Apache Paimon的BinaryRow设计
 * 
 * 用于高效的partition表示和二进制存储
 * 避免字符串的开销，提供更高效的序列化和比较
 */
public class BinaryRow implements Serializable, Comparable<BinaryRow> {
    private static final long serialVersionUID = 1L;
    
    /**
     * 二进制数据
     */
    private final byte[] data;
    
    /**
     * 字段数量
     */
    private final int fieldCount;
    
    /**
     * 从字节数组创建BinaryRow
     */
    public BinaryRow(byte[] data, int fieldCount) {
        this.data = data != null ? data : new byte[0];
        this.fieldCount = fieldCount;
    }
    
    /**
     * 从对象数组创建BinaryRow（简化版本）
     */
    public static BinaryRow fromObjects(Object[] values) {
        if (values == null || values.length == 0) {
            return new BinaryRow(new byte[0], 0);
        }
        
        // 简化实现：将所有值转换为字符串，然后序列化
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append('\0'); // 使用null字符分隔
            }
            if (values[i] != null) {
                sb.append(values[i].toString());
            }
        }
        
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        return new BinaryRow(data, values.length);
    }
    
    /**
     * 从字符串创建BinaryRow
     */
    public static BinaryRow fromString(String value) {
        if (value == null || value.isEmpty()) {
            return new BinaryRow(new byte[0], 0);
        }
        
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        return new BinaryRow(data, 1);
    }
    
    /**
     * 创建空的BinaryRow
     */
    public static BinaryRow empty() {
        return new BinaryRow(new byte[0], 0);
    }
    
    /**
     * 获取二进制数据
     */
    public byte[] getData() {
        return data;
    }
    
    /**
     * 获取字段数量
     */
    public int getFieldCount() {
        return fieldCount;
    }
    
    /**
     * 获取数据大小（字节）
     */
    public int getSizeInBytes() {
        return data.length;
    }
    
    /**
     * 是否为空
     */
    public boolean isEmpty() {
        return data.length == 0;
    }
    
    /**
     * 转换为字符串（用于调试）
     */
    public String toDebugString() {
        if (isEmpty()) {
            return "<empty>";
        }
        return new String(data, StandardCharsets.UTF_8);
    }
    
    /**
     * 获取指定字段的值（简化版本）
     */
    public String getField(int index) {
        if (index < 0 || index >= fieldCount) {
            throw new IndexOutOfBoundsException(
                "Field index " + index + " out of bounds [0, " + fieldCount + ")");
        }
        
        String str = new String(data, StandardCharsets.UTF_8);
        String[] parts = str.split("\0");
        
        if (index < parts.length) {
            return parts[index];
        }
        
        return null;
    }
    
    /**
     * 复制数据
     */
    public BinaryRow copy() {
        byte[] newData = Arrays.copyOf(data, data.length);
        return new BinaryRow(newData, fieldCount);
    }
    
    /**
     * 序列化到ByteBuffer
     */
    public void serializeTo(ByteBuffer buffer) {
        // 写入字段数量
        buffer.putInt(fieldCount);
        // 写入数据长度
        buffer.putInt(data.length);
        // 写入数据
        buffer.put(data);
    }
    
    /**
     * 从ByteBuffer反序列化
     */
    public static BinaryRow deserializeFrom(ByteBuffer buffer) {
        // 读取字段数量
        int fieldCount = buffer.getInt();
        // 读取数据长度
        int dataLength = buffer.getInt();
        // 读取数据
        byte[] data = new byte[dataLength];
        buffer.get(data);
        
        return new BinaryRow(data, fieldCount);
    }
    
    /**
     * 计算序列化后的大小
     */
    public int getSerializedSize() {
        return 4 + 4 + data.length; // fieldCount (4) + length (4) + data
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        BinaryRow that = (BinaryRow) o;
        
        if (fieldCount != that.fieldCount) return false;
        return Arrays.equals(data, that.data);
    }
    
    @Override
    public int hashCode() {
        int result = Arrays.hashCode(data);
        result = 31 * result + fieldCount;
        return result;
    }
    
    @Override
    public int compareTo(BinaryRow other) {
        if (other == null) {
            return 1;
        }
        
        // 字典序比较二进制数据
        int minLen = Math.min(this.data.length, other.data.length);
        
        for (int i = 0; i < minLen; i++) {
            int thisByte = this.data[i] & 0xFF;
            int otherByte = other.data[i] & 0xFF;
            
            if (thisByte != otherByte) {
                return thisByte - otherByte;
            }
        }
        
        // 如果前缀相同，比较长度
        return Integer.compare(this.data.length, other.data.length);
    }
    
    @Override
    public String toString() {
        return "BinaryRow{" +
               "fieldCount=" + fieldCount +
               ", sizeInBytes=" + data.length +
               ", data=" + toDebugString() +
               '}';
    }
    
    /**
     * Builder for BinaryRow
     */
    public static class Builder {
        private final int fieldCount;
        private final Object[] values;
        
        public Builder(int fieldCount) {
            this.fieldCount = fieldCount;
            this.values = new Object[fieldCount];
        }
        
        public Builder setField(int index, Object value) {
            if (index < 0 || index >= fieldCount) {
                throw new IndexOutOfBoundsException(
                    "Field index " + index + " out of bounds [0, " + fieldCount + ")");
            }
            values[index] = value;
            return this;
        }
        
        public BinaryRow build() {
            return BinaryRow.fromObjects(values);
        }
    }
    
    /**
     * 创建Builder
     */
    public static Builder builder(int fieldCount) {
        return new Builder(fieldCount);
    }
}

