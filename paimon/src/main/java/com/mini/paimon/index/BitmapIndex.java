package com.mini.paimon.index;

import com.mini.paimon.table.Predicate;

import java.io.*;
import java.util.*;

/**
 * Bitmap索引
 * 为每个唯一值维护一个位图，位图的每一位对应数据文件中的一行
 * 参考Paimon的BitmapIndex实现
 */
public class BitmapIndex extends AbstractFileIndex {
    
    private static final long serialVersionUID = 1L;
    
    /** 值到行号位图的映射 */
    private final Map<Object, SimpleBitmap> valueBitmaps;
    
    /** NULL值的位图 */
    private SimpleBitmap nullBitmap;
    
    /** 总行数 */
    private int rowCount;
    
    /**
     * 构造函数
     */
    public BitmapIndex(String fieldName) {
        super(IndexType.BITMAP, fieldName);
        this.valueBitmaps = new HashMap<>();
        this.nullBitmap = new SimpleBitmap();
        this.rowCount = 0;
    }
    
    @Override
    public void add(Object value) {
        // 默认实现：不记录行号，Bitmap索引必须使用addWithRowNumber
        throw new UnsupportedOperationException(
            "BitmapIndex requires addWithRowNumber(value, rowNumber) instead of add(value)");
    }
    
    @Override
    public void addWithRowNumber(Object value, int rowNumber) {
        if (value == null) {
            nullBitmap.add(rowNumber);
        } else {
            SimpleBitmap bitmap = valueBitmaps.computeIfAbsent(value, k -> new SimpleBitmap());
            bitmap.add(rowNumber);
        }
        rowCount = Math.max(rowCount, rowNumber + 1);
    }
    
    @Override
    public boolean mightContain(Object value) {
        if (value == null) {
            return !nullBitmap.isEmpty();
        }
        SimpleBitmap bitmap = valueBitmaps.get(value);
        return bitmap != null && !bitmap.isEmpty();
    }
    
    @Override
    public SimpleBitmap filter(Predicate predicate) {
        if (predicate == null) {
            return createAllRowsBitmap();
        }
        
        if (predicate instanceof Predicate.FieldPredicate) {
            Predicate.FieldPredicate fp = (Predicate.FieldPredicate) predicate;
            
            // 检查字段名是否匹配
            if (!fp.getFieldName().equals(fieldName)) {
                return createAllRowsBitmap();
            }
            
            // 根据操作符过滤
            switch (fp.getOp()) {
                case EQ:
                    return filterEqual(fp.getValue());
                case NE:
                    return filterNotEqual(fp.getValue());
                default:
                    // 其他操作符（GT, GE, LT, LE）不适合Bitmap索引
                    return createAllRowsBitmap();
            }
        } else if (predicate instanceof Predicate.AndPredicate) {
            Predicate.AndPredicate ap = (Predicate.AndPredicate) predicate;
            SimpleBitmap left = filter(ap.getLeft());
            SimpleBitmap right = filter(ap.getRight());
            return SimpleBitmap.and(left, right);
        } else if (predicate instanceof Predicate.OrPredicate) {
            Predicate.OrPredicate op = (Predicate.OrPredicate) predicate;
            SimpleBitmap left = filter(op.getLeft());
            SimpleBitmap right = filter(op.getRight());
            return SimpleBitmap.or(left, right);
        } else if (predicate instanceof InPredicate) {
            InPredicate ip = (InPredicate) predicate;
            if (!ip.getFieldName().equals(fieldName)) {
                return createAllRowsBitmap();
            }
            return filterIn(ip.getValues());
        }
        
        // 默认：返回所有行
        return createAllRowsBitmap();
    }
    
    /**
     * 过滤等值查询
     */
    private SimpleBitmap filterEqual(Object value) {
        if (value == null) {
            return nullBitmap;
        }
        SimpleBitmap bitmap = valueBitmaps.get(value);
        return bitmap != null ? bitmap : new SimpleBitmap();
    }
    
    /**
     * 过滤不等值查询
     */
    private SimpleBitmap filterNotEqual(Object value) {
        SimpleBitmap allRows = createAllRowsBitmap();
        SimpleBitmap equalRows = filterEqual(value);
        return SimpleBitmap.andNot(allRows, equalRows);
    }
    
    /**
     * 过滤IN查询
     */
    private SimpleBitmap filterIn(List<Object> values) {
        SimpleBitmap result = new SimpleBitmap();
        
        for (Object value : values) {
            SimpleBitmap bitmap = filterEqual(value);
            result = SimpleBitmap.or(result, bitmap);
        }
        
        return result;
    }
    
    /**
     * 创建包含所有行的位图
     */
    private SimpleBitmap createAllRowsBitmap() {
        SimpleBitmap allRows = new SimpleBitmap();
        for (int i = 0; i < rowCount; i++) {
            allRows.add(i);
        }
        return allRows;
    }
    
    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // 写入版本号
        dos.writeByte(2); // V2版本
        
        // 写入字段名
        dos.writeUTF(fieldName);
        
        // 写入总行数
        dos.writeInt(rowCount);
        
        // 写入NULL位图
        byte[] nullBitmapBytes = nullBitmap.serialize();
        dos.writeInt(nullBitmapBytes.length);
        dos.write(nullBitmapBytes);
        
        // 写入值位图数量
        dos.writeInt(valueBitmaps.size());
        
        // 写入每个值和对应的位图
        for (Map.Entry<Object, SimpleBitmap> entry : valueBitmaps.entrySet()) {
            Object value = entry.getKey();
            SimpleBitmap bitmap = entry.getValue();
            
            // 写入值（序列化为字符串）
            String valueStr = String.valueOf(value);
            dos.writeUTF(valueStr);
            
            // 写入位图
            byte[] bitmapBytes = bitmap.serialize();
            dos.writeInt(bitmapBytes.length);
            dos.write(bitmapBytes);
        }
        
        dos.flush();
        return baos.toByteArray();
    }
    
    @Override
    public void deserialize(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        // 读取版本号
        byte version = dis.readByte();
        if (version != 2) {
            throw new IOException("Unsupported BitmapIndex version: " + version);
        }
        
        // 读取字段名（验证）
        String deserializedFieldName = dis.readUTF();
        if (!deserializedFieldName.equals(fieldName)) {
            throw new IOException("Field name mismatch: expected " + fieldName + 
                                ", got " + deserializedFieldName);
        }
        
        // 读取总行数
        this.rowCount = dis.readInt();
        
        // 读取NULL位图
        int nullBitmapLength = dis.readInt();
        byte[] nullBitmapBytes = new byte[nullBitmapLength];
        dis.readFully(nullBitmapBytes);
        this.nullBitmap = SimpleBitmap.deserialize(nullBitmapBytes);
        
        // 读取值位图数量
        int valueBitmapsCount = dis.readInt();
        
        // 读取每个值和对应的位图
        valueBitmaps.clear();
        for (int i = 0; i < valueBitmapsCount; i++) {
            // 读取值
            String valueStr = dis.readUTF();
            
            // 读取位图
            int bitmapLength = dis.readInt();
            byte[] bitmapBytes = new byte[bitmapLength];
            dis.readFully(bitmapBytes);
            SimpleBitmap bitmap = SimpleBitmap.deserialize(bitmapBytes);
            
            valueBitmaps.put(valueStr, bitmap);
        }
    }
    
    @Override
    public long getMemorySize() {
        long size = 100; // 对象头和基本字段
        
        // NULL位图大小
        size += 64; // SimpleBitmap对象
        
        // 值位图映射大小
        for (Map.Entry<Object, SimpleBitmap> entry : valueBitmaps.entrySet()) {
            size += estimateObjectSize(entry.getKey());
            size += 64; // SimpleBitmap对象
        }
        
        return size;
    }
    
    public int getRowCount() {
        return rowCount;
    }
    
    public int getDistinctValueCount() {
        return valueBitmaps.size() + (nullBitmap.isEmpty() ? 0 : 1);
    }
    
    /**
     * IN谓词 - 用于支持IN查询
     */
    public static class InPredicate extends Predicate {
        private final String fieldName;
        private final List<Object> values;
        
        public InPredicate(String fieldName, List<Object> values) {
            this.fieldName = fieldName;
            this.values = values;
        }
        
        public String getFieldName() {
            return fieldName;
        }
        
        public List<Object> getValues() {
            return values;
        }
        
        @Override
        public boolean test(com.mini.paimon.schema.Row row, com.mini.paimon.schema.Schema schema) {
            // 查找字段索引
            int fieldIndex = -1;
            for (int i = 0; i < schema.getFields().size(); i++) {
                if (schema.getFields().get(i).getName().equals(fieldName)) {
                    fieldIndex = i;
                    break;
                }
            }
            
            if (fieldIndex == -1) {
                throw new IllegalArgumentException("Field not found: " + fieldName);
            }
            
            Object fieldValue = row.getValues()[fieldIndex];
            
            // 检查值是否在列表中
            for (Object value : values) {
                if (fieldValue == null && value == null) {
                    return true;
                }
                if (fieldValue != null && fieldValue.equals(value)) {
                    return true;
                }
            }
            
            return false;
        }
    }
}

