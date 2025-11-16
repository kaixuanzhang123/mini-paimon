package com.mini.paimon.index;

import java.io.*;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * SimpleBitmap - 简化的位图实现
 * 基于Java BitSet实现，用于Bitmap索引存储行号集合
 * 参考Paimon的RoaringBitmap32设计
 */
public class SimpleBitmap implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /** 位集合 */
    private BitSet bitSet;
    
    /** 位图中的元素数量（已设置的位数） */
    private int cardinality;
    
    public SimpleBitmap() {
        this.bitSet = new BitSet();
        this.cardinality = 0;
    }
    
    /**
     * 添加行号到位图
     */
    public void add(int rowNumber) {
        if (rowNumber < 0) {
            throw new IllegalArgumentException("Row number must be non-negative: " + rowNumber);
        }
        if (!bitSet.get(rowNumber)) {
            bitSet.set(rowNumber);
            cardinality++;
        }
    }
    
    /**
     * 检查行号是否在位图中
     */
    public boolean contains(int rowNumber) {
        if (rowNumber < 0) {
            return false;
        }
        return bitSet.get(rowNumber);
    }
    
    /**
     * 获取位图的基数（包含的行号数量）
     */
    public int getCardinality() {
        return cardinality;
    }
    
    /**
     * 检查位图是否为空
     */
    public boolean isEmpty() {
        return cardinality == 0;
    }
    
    /**
     * 与操作 - 返回两个位图的交集
     */
    public static SimpleBitmap and(SimpleBitmap bitmap1, SimpleBitmap bitmap2) {
        SimpleBitmap result = new SimpleBitmap();
        result.bitSet = (BitSet) bitmap1.bitSet.clone();
        result.bitSet.and(bitmap2.bitSet);
        result.cardinality = result.bitSet.cardinality();
        return result;
    }
    
    /**
     * 或操作 - 返回两个位图的并集
     */
    public static SimpleBitmap or(SimpleBitmap bitmap1, SimpleBitmap bitmap2) {
        SimpleBitmap result = new SimpleBitmap();
        result.bitSet = (BitSet) bitmap1.bitSet.clone();
        result.bitSet.or(bitmap2.bitSet);
        result.cardinality = result.bitSet.cardinality();
        return result;
    }
    
    /**
     * 非操作 - 返回位图的补集（在给定范围内）
     */
    public static SimpleBitmap andNot(SimpleBitmap bitmap1, SimpleBitmap bitmap2) {
        SimpleBitmap result = new SimpleBitmap();
        result.bitSet = (BitSet) bitmap1.bitSet.clone();
        result.bitSet.andNot(bitmap2.bitSet);
        result.cardinality = result.bitSet.cardinality();
        return result;
    }
    
    /**
     * 获取位图的迭代器
     */
    public Iterator<Integer> iterator() {
        return new BitmapIterator(bitSet);
    }
    
    /**
     * 序列化位图
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // 写入基数
        dos.writeInt(cardinality);
        
        // 写入BitSet
        byte[] bitSetBytes = bitSet.toByteArray();
        dos.writeInt(bitSetBytes.length);
        dos.write(bitSetBytes);
        
        dos.flush();
        return baos.toByteArray();
    }
    
    /**
     * 反序列化位图
     */
    public static SimpleBitmap deserialize(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        SimpleBitmap bitmap = new SimpleBitmap();
        
        // 读取基数
        bitmap.cardinality = dis.readInt();
        
        // 读取BitSet
        int bitSetLength = dis.readInt();
        byte[] bitSetBytes = new byte[bitSetLength];
        dis.readFully(bitSetBytes);
        bitmap.bitSet = BitSet.valueOf(bitSetBytes);
        
        return bitmap;
    }
    
    /**
     * 位图迭代器
     */
    private static class BitmapIterator implements Iterator<Integer> {
        private final BitSet bitSet;
        private int nextBit;
        
        BitmapIterator(BitSet bitSet) {
            this.bitSet = bitSet;
            this.nextBit = bitSet.nextSetBit(0);
        }
        
        @Override
        public boolean hasNext() {
            return nextBit >= 0;
        }
        
        @Override
        public Integer next() {
            if (nextBit < 0) {
                throw new NoSuchElementException();
            }
            int current = nextBit;
            nextBit = bitSet.nextSetBit(nextBit + 1);
            return current;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SimpleBitmap{cardinality=").append(cardinality).append(", values=[");
        Iterator<Integer> iter = iterator();
        int count = 0;
        while (iter.hasNext() && count < 10) {
            if (count > 0) {
                sb.append(", ");
            }
            sb.append(iter.next());
            count++;
        }
        if (cardinality > 10) {
            sb.append(", ...");
        }
        sb.append("]}");
        return sb.toString();
    }
}

