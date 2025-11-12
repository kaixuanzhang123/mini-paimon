package com.mini.paimon.index;

import java.io.IOException;

/**
 * 索引工厂
 * 负责创建不同类型的索引实例
 */
public class IndexFactory {
    
    /**
     * 创建指定类型的索引
     * @param indexType 索引类型
     * @param fieldName 字段名
     * @return 索引实例
     */
    public static FileIndex createIndex(IndexType indexType, String fieldName) {
        switch (indexType) {
            case BLOOM_FILTER:
                return new BloomFilterIndex(fieldName);
            case MIN_MAX:
                return new MinMaxIndex(fieldName);
            default:
                throw new IllegalArgumentException("Unsupported index type: " + indexType);
        }
    }
    
    /**
     * 从字节数组加载索引
     * @param indexType 索引类型
     * @param fieldName 字段名
     * @param data 序列化数据
     * @return 索引实例
     */
    public static FileIndex loadIndex(IndexType indexType, String fieldName, byte[] data) 
            throws IOException {
        FileIndex index = createIndex(indexType, fieldName);
        index.deserialize(data);
        return index;
    }
}
