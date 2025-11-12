package com.mini.paimon.index;

import java.io.*;

/**
 * Min-Max 索引
 * 存储字段的最小值和最大值，用于范围查询过滤
 * 支持所有可比较类型
 */
public class MinMaxIndex extends AbstractFileIndex {
    
    private static final long serialVersionUID = 1L;
    
    /** 最小值 */
    private Comparable minValue;
    
    /** 最大值 */
    private Comparable maxValue;
    
    /** 数据类型标识 */
    private ValueType valueType;
    
    /** 已添加的元素个数 */
    private int elementCount;
    
    /**
     * 值类型枚举
     */
    private enum ValueType {
        INTEGER,
        LONG,
        DOUBLE,
        STRING,
        BOOLEAN,
        NULL
    }
    
    /**
     * 构造函数
     */
    public MinMaxIndex(String fieldName) {
        super(IndexType.MIN_MAX, fieldName);
        this.minValue = null;
        this.maxValue = null;
        this.valueType = ValueType.NULL;
        this.elementCount = 0;
    }
    
    @Override
    public void add(Object value) {
        if (value == null) {
            return;
        }
        
        // 确定值类型
        if (valueType == ValueType.NULL) {
            valueType = detectValueType(value);
        }
        
        // 转换为 Comparable
        Comparable comparableValue = toComparable(value);
        if (comparableValue == null) {
            return;
        }
        
        // 更新最小值
        if (minValue == null || comparableValue.compareTo(minValue) < 0) {
            minValue = comparableValue;
        }
        
        // 更新最大值
        if (maxValue == null || comparableValue.compareTo(maxValue) > 0) {
            maxValue = comparableValue;
        }
        
        elementCount++;
    }
    
    @Override
    public boolean mightContain(Object value) {
        if (value == null || minValue == null || maxValue == null) {
            return true;
        }
        
        Comparable comparableValue = toComparable(value);
        if (comparableValue == null) {
            return true;
        }
        
        // 判断值是否在 [min, max] 范围内
        return comparableValue.compareTo(minValue) >= 0 && 
               comparableValue.compareTo(maxValue) <= 0;
    }
    
    @Override
    public boolean mightIntersect(Object min, Object max) {
        if (minValue == null || maxValue == null) {
            return true;
        }
        
        Comparable queryMin = min != null ? toComparable(min) : null;
        Comparable queryMax = max != null ? toComparable(max) : null;
        
        // 查询范围为 [queryMin, queryMax]
        // 索引范围为 [minValue, maxValue]
        // 判断两个范围是否有交集
        
        if (queryMin != null && queryMin.compareTo(maxValue) > 0) {
            // 查询最小值大于索引最大值，无交集
            return false;
        }
        
        if (queryMax != null && queryMax.compareTo(minValue) < 0) {
            // 查询最大值小于索引最小值，无交集
            return false;
        }
        
        return true;
    }
    
    /**
     * 检测值类型
     */
    private ValueType detectValueType(Object value) {
        if (value instanceof Integer) {
            return ValueType.INTEGER;
        } else if (value instanceof Long) {
            return ValueType.LONG;
        } else if (value instanceof Double) {
            return ValueType.DOUBLE;
        } else if (value instanceof String) {
            return ValueType.STRING;
        } else if (value instanceof Boolean) {
            return ValueType.BOOLEAN;
        }
        return ValueType.NULL;
    }
    
    /**
     * 将对象转换为 Comparable
     */
    @SuppressWarnings("unchecked")
    private Comparable toComparable(Object value) {
        if (value == null) {
            return null;
        }
        
        if (value instanceof Comparable) {
            return (Comparable) value;
        }
        
        // 如果不是 Comparable，尝试转换为字符串
        return value.toString();
    }
    
    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // 写入字段名
        dos.writeUTF(fieldName);
        
        // 写入值类型
        dos.writeInt(valueType.ordinal());
        
        // 写入元素计数
        dos.writeInt(elementCount);
        
        // 写入最小值和最大值
        writeValue(dos, minValue);
        writeValue(dos, maxValue);
        
        dos.flush();
        return baos.toByteArray();
    }
    
    @Override
    public void deserialize(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        // 读取字段名（验证）
        String deserializedFieldName = dis.readUTF();
        if (!deserializedFieldName.equals(fieldName)) {
            throw new IOException("Field name mismatch: expected " + fieldName + 
                                ", got " + deserializedFieldName);
        }
        
        // 读取值类型
        int typeOrdinal = dis.readInt();
        this.valueType = ValueType.values()[typeOrdinal];
        
        // 读取元素计数
        this.elementCount = dis.readInt();
        
        // 读取最小值和最大值
        this.minValue = readValue(dis);
        this.maxValue = readValue(dis);
    }
    
    /**
     * 写入值
     */
    private void writeValue(DataOutputStream dos, Comparable value) throws IOException {
        if (value == null) {
            dos.writeBoolean(false);
            return;
        }
        
        dos.writeBoolean(true);
        
        switch (valueType) {
            case INTEGER:
                dos.writeInt((Integer) value);
                break;
            case LONG:
                dos.writeLong((Long) value);
                break;
            case DOUBLE:
                dos.writeDouble((Double) value);
                break;
            case STRING:
                dos.writeUTF((String) value);
                break;
            case BOOLEAN:
                dos.writeBoolean((Boolean) value);
                break;
            default:
                dos.writeUTF(value.toString());
        }
    }
    
    /**
     * 读取值
     */
    @SuppressWarnings("unchecked")
    private Comparable readValue(DataInputStream dis) throws IOException {
        boolean hasValue = dis.readBoolean();
        if (!hasValue) {
            return null;
        }
        
        switch (valueType) {
            case INTEGER:
                return dis.readInt();
            case LONG:
                return dis.readLong();
            case DOUBLE:
                return dis.readDouble();
            case STRING:
                return dis.readUTF();
            case BOOLEAN:
                return dis.readBoolean();
            default:
                return null;
        }
    }
    
    @Override
    public long getMemorySize() {
        long size = 100; // 对象头和字段
        size += estimateObjectSize(minValue);
        size += estimateObjectSize(maxValue);
        return size;
    }
    
    public Comparable getMinValue() {
        return minValue;
    }
    
    public Comparable getMaxValue() {
        return maxValue;
    }
    
    public int getElementCount() {
        return elementCount;
    }
    
    @Override
    public String toString() {
        return "MinMaxIndex{" +
                "fieldName='" + fieldName + '\'' +
                ", minValue=" + minValue +
                ", maxValue=" + maxValue +
                ", valueType=" + valueType +
                ", elementCount=" + elementCount +
                '}';
    }
}
