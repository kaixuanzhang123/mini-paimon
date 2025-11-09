package com.mini.paimon.reader;

import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;

import java.io.IOException;

/**
 * File Record Reader
 * 文件级记录读取器接口
 * 参考 Paimon 的 FileRecordReader 设计
 * 
 * 职责：
 * 1. 从单个文件中读取记录
 * 2. 支持基于键的范围扫描
 * 3. 支持谓词和投影下推到文件读取层
 */
public interface FileRecordReader extends RecordReader<Row> {
    
    /**
     * 获取当前文件路径
     */
    String getFilePath();
    
    /**
     * 定位到指定的键位置
     * 用于范围扫描的优化
     * 
     * @param key 目标键
     * @throws IOException 定位失败
     */
    void seekToKey(RowKey key) throws IOException;
    
    /**
     * 检查是否还有更多记录
     */
    boolean hasNext() throws IOException;
}
