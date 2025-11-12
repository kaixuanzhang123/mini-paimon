package com.mini.paimon.reader;

import com.mini.paimon.metadata.Row;

import java.io.Closeable;
import java.io.IOException;

/**
 * Record Reader Interface
 * 参考 Paimon 的 RecordReader 设计
 * 提供统一的记录读取接口，支持流式读取和批量读取
 * 
 * 设计理念：
 * 1. 支持多种读取模式（流式 vs 批量）
 * 2. 支持谓词下推和投影下推
 * 3. 支持资源自动清理
 */
public interface RecordReader<T> extends Closeable {
    
    /**
     * 读取下一条记录
     * 
     * @return 下一条记录，如果没有更多记录返回 null
     * @throws IOException 读取异常
     */
    T readRecord() throws IOException;
    
    /**
     * 批量读取记录
     * 
     * @param maxRecords 最大读取记录数
     * @return 读取到的记录批次，如果没有更多记录返回 null
     * @throws IOException 读取异常
     */
    RecordBatch<T> readBatch(int maxRecords) throws IOException;
    
    /**
     * 关闭读取器，释放资源
     */
    @Override
    void close() throws IOException;
    
    /**
     * 记录批次
     */
    interface RecordBatch<T> {
        /**
         * 获取批次中的记录数量
         */
        int size();
        
        /**
         * 获取指定索引的记录
         */
        T get(int index);
        
        /**
         * 检查批次是否为空
         */
        default boolean isEmpty() {
            return size() == 0;
        }
    }
}
