package com.mini.paimon.index;

import java.io.*;
import java.util.BitSet;

/**
 * 布隆过滤器索引
 * 使用 BitSet 实现布隆过滤器，支持快速判断值是否可能存在
 * 采用 MurmurHash 算法生成多个哈希值
 */
public class BloomFilterIndex extends AbstractFileIndex {
    
    private static final long serialVersionUID = 1L;
    
    /** 布隆过滤器误判率 */
    private static final double DEFAULT_FPP = 0.01;
    
    /** 位数组 */
    private BitSet bitSet;
    
    /** 位数组大小 */
    private int bitSize;
    
    /** 哈希函数个数 */
    private int numHashFunctions;
    
    /** 已添加的元素个数 */
    private int elementCount;
    
    /** 期望的元素个数（用于初始化） */
    private int expectedElements;
    
    /**
     * 构造函数（用于构建新索引）
     */
    public BloomFilterIndex(String fieldName) {
        this(fieldName, 10000, DEFAULT_FPP);
    }
    
    /**
     * 构造函数（指定预期元素数量）
     */
    public BloomFilterIndex(String fieldName, int expectedElements) {
        this(fieldName, expectedElements, DEFAULT_FPP);
    }
    
    /**
     * 构造函数（指定预期元素数量和误判率）
     */
    public BloomFilterIndex(String fieldName, int expectedElements, double fpp) {
        super(IndexType.BLOOM_FILTER, fieldName);
        this.expectedElements = expectedElements;
        this.elementCount = 0;
        
        // 计算最优的位数组大小和哈希函数个数
        this.bitSize = optimalBitSize(expectedElements, fpp);
        this.numHashFunctions = optimalNumHashFunctions(expectedElements, bitSize);
        this.bitSet = new BitSet(bitSize);
    }
    
    /**
     * 计算最优位数组大小
     * m = -n * ln(p) / (ln(2)^2)
     */
    private static int optimalBitSize(int n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }
    
    /**
     * 计算最优哈希函数个数
     * k = (m/n) * ln(2)
     */
    private static int optimalNumHashFunctions(int n, int m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }
    
    @Override
    public void add(Object value) {
        if (value == null) {
            return;
        }
        
        byte[] bytes = toBytes(value);
        long hash64 = murmurHash64(bytes);
        
        // 使用双重哈希技术生成 k 个哈希值
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        
        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = hash1 + i * hash2;
            int position = Math.abs(combinedHash % bitSize);
            bitSet.set(position);
        }
        
        elementCount++;
    }
    
    @Override
    public boolean mightContain(Object value) {
        if (value == null) {
            return false;
        }
        
        byte[] bytes = toBytes(value);
        long hash64 = murmurHash64(bytes);
        
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        
        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = hash1 + i * hash2;
            int position = Math.abs(combinedHash % bitSize);
            if (!bitSet.get(position)) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 将对象转换为字节数组
     */
    private byte[] toBytes(Object value) {
        if (value instanceof String) {
            return ((String) value).getBytes();
        } else if (value instanceof Integer) {
            int v = (Integer) value;
            return new byte[]{
                (byte)(v >>> 24),
                (byte)(v >>> 16),
                (byte)(v >>> 8),
                (byte)v
            };
        } else if (value instanceof Long) {
            long v = (Long) value;
            return new byte[]{
                (byte)(v >>> 56),
                (byte)(v >>> 48),
                (byte)(v >>> 40),
                (byte)(v >>> 32),
                (byte)(v >>> 24),
                (byte)(v >>> 16),
                (byte)(v >>> 8),
                (byte)v
            };
        } else if (value instanceof Double) {
            long v = Double.doubleToLongBits((Double) value);
            return new byte[]{
                (byte)(v >>> 56),
                (byte)(v >>> 48),
                (byte)(v >>> 40),
                (byte)(v >>> 32),
                (byte)(v >>> 24),
                (byte)(v >>> 16),
                (byte)(v >>> 8),
                (byte)v
            };
        } else if (value instanceof Boolean) {
            return new byte[]{(byte) (((Boolean) value) ? 1 : 0)};
        } else {
            return value.toString().getBytes();
        }
    }
    
    /**
     * MurmurHash64A 哈希算法实现
     * 高性能且分布均匀的哈希算法
     */
    private static long murmurHash64(byte[] data) {
        final long seed = 0xe17a1465;
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;
        
        long h = seed ^ (data.length * m);
        
        int length8 = data.length / 8;
        
        for (int i = 0; i < length8; i++) {
            final int i8 = i * 8;
            long k = ((long) data[i8] & 0xff)
                    + (((long) data[i8 + 1] & 0xff) << 8)
                    + (((long) data[i8 + 2] & 0xff) << 16)
                    + (((long) data[i8 + 3] & 0xff) << 24)
                    + (((long) data[i8 + 4] & 0xff) << 32)
                    + (((long) data[i8 + 5] & 0xff) << 40)
                    + (((long) data[i8 + 6] & 0xff) << 48)
                    + (((long) data[i8 + 7] & 0xff) << 56);
            
            k *= m;
            k ^= k >>> r;
            k *= m;
            
            h ^= k;
            h *= m;
        }
        
        int remaining = data.length % 8;
        if (remaining >= 7) h ^= (long) (data[(data.length & ~7) + 6] & 0xff) << 48;
        if (remaining >= 6) h ^= (long) (data[(data.length & ~7) + 5] & 0xff) << 40;
        if (remaining >= 5) h ^= (long) (data[(data.length & ~7) + 4] & 0xff) << 32;
        if (remaining >= 4) h ^= (long) (data[(data.length & ~7) + 3] & 0xff) << 24;
        if (remaining >= 3) h ^= (long) (data[(data.length & ~7) + 2] & 0xff) << 16;
        if (remaining >= 2) h ^= (long) (data[(data.length & ~7) + 1] & 0xff) << 8;
        if (remaining >= 1) {
            h ^= (long) (data[data.length & ~7] & 0xff);
            h *= m;
        }
        
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        
        return h;
    }
    
    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // 写入字段名
        dos.writeUTF(fieldName);
        
        // 写入布隆过滤器参数
        dos.writeInt(bitSize);
        dos.writeInt(numHashFunctions);
        dos.writeInt(elementCount);
        dos.writeInt(expectedElements);
        
        // 写入 BitSet
        byte[] bitSetBytes = bitSet.toByteArray();
        dos.writeInt(bitSetBytes.length);
        dos.write(bitSetBytes);
        
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
        
        // 读取布隆过滤器参数
        this.bitSize = dis.readInt();
        this.numHashFunctions = dis.readInt();
        this.elementCount = dis.readInt();
        this.expectedElements = dis.readInt();
        
        // 读取 BitSet
        int bitSetBytesLength = dis.readInt();
        byte[] bitSetBytes = new byte[bitSetBytesLength];
        dis.readFully(bitSetBytes);
        this.bitSet = BitSet.valueOf(bitSetBytes);
    }
    
    @Override
    public long getMemorySize() {
        // BitSet 内存 + 对象头和字段
        return (bitSize / 8) + 100;
    }
    
    public int getElementCount() {
        return elementCount;
    }
    
    public int getBitSize() {
        return bitSize;
    }
    
    public int getNumHashFunctions() {
        return numHashFunctions;
    }
    
    /**
     * 计算当前的误判率
     */
    public double expectedFpp() {
        if (elementCount == 0) {
            return 0.0;
        }
        return Math.pow(1 - Math.exp(-numHashFunctions * (double) elementCount / bitSize), 
                       numHashFunctions);
    }
}
