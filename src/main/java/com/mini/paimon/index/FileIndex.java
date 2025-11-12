package com.mini.paimon.index;

import com.mini.paimon.metadata.Row;

import java.io.IOException;
import java.io.Serializable;

/**
 * 文件索引接口
 * 所有索引类型的基类，定义索引的基本操作
 */
public interface FileIndex extends Serializable {
    
    /**
     * 获取索引类型
     */
    IndexType getIndexType();
    
    /**
     * 获取索引所属的字段名
     */
    String getFieldName();
    
    /**
     * 向索引中添加一个值
     * 在构建索引时调用
     */
    void add(Object value);
    
    /**
     * 测试给定值是否可能存在于索引中
     * @param value 要测试的值
     * @return true 表示可能存在（或一定存在），false 表示一定不存在
     */
    boolean mightContain(Object value);
    
    /**
     * 测试给定范围是否与索引有交集
     * @param min 范围最小值（可为 null，表示负无穷）
     * @param max 范围最大值（可为 null，表示正无穷）
     * @return true 表示可能有交集，false 表示一定无交集
     */
    boolean mightIntersect(Object min, Object max);
    
    /**
     * 序列化索引到字节数组
     */
    byte[] serialize() throws IOException;
    
    /**
     * 从字节数组反序列化索引
     */
    void deserialize(byte[] data) throws IOException;
    
    /**
     * 获取索引占用的内存大小（字节）
     */
    long getMemorySize();
}
