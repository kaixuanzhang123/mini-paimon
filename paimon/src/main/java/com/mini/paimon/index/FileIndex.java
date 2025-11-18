package com.mini.paimon.index;

import com.mini.paimon.table.Predicate;

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
     * 向索引中添加一个值，同时记录行号（用于Bitmap索引）
     * @param value 值
     * @param rowNumber 行号
     */
    default void addWithRowNumber(Object value, int rowNumber) {
        // 默认实现：忽略行号，仅调用add
        add(value);
    }
    
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
     * 根据谓词过滤，返回符合条件的行号位图（仅Bitmap索引支持）
     * @param predicate 谓词条件
     * @return 符合条件的行号位图，如果不支持返回null
     */
    default SimpleBitmap filter(Predicate predicate) {
        // 默认实现：不支持
        return null;
    }
    
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
