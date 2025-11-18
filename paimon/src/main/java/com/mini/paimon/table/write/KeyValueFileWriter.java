package com.mini.paimon.table.write;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.schema.Row;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * KeyValue文件写入器接口
 * 参考 Paimon KeyValueFileWriter 设计
 * 
 * 用于写入不同格式的数据文件（CSV、SST等）
 */
public interface KeyValueFileWriter extends Closeable, Flushable {
    
    /**
     * 写入一行数据
     * 
     * @param row 数据行
     * @throws IOException 写入异常
     */
    void write(Row row) throws IOException;
    
    /**
     * 刷写缓冲区
     * 
     * @throws IOException 刷写异常
     */
    @Override
    void flush() throws IOException;
    
    /**
     * 获取已写入的行数
     * 
     * @return 行数
     */
    long getRowCount();
    
    /**
     * 获取文件大小（字节）
     * 
     * @return 文件大小
     */
    long getFileSize() throws IOException;
    
    /**
     * 获取文件元信息
     * 必须在close()之前或之后调用
     * 
     * @return 数据文件元信息
     * @throws IOException IO异常
     */
    DataFileMeta getFileMeta() throws IOException;
    
    /**
     * 关闭写入器
     * 
     * @throws IOException IO异常
     */
    @Override
    void close() throws IOException;
}

