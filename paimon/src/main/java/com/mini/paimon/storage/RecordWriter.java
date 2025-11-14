package com.mini.paimon.storage;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.metadata.Row;

import java.io.IOException;
import java.util.List;

/**
 * RecordWriter 接口
 * 参考 Paimon RecordWriter 设计
 * 
 * 定义数据写入的通用接口,支持两种实现:
 * 1. MergeTreeWriter - 主键表,使用 LSM Tree
 * 2. AppendOnlyWriter - 仅追加表,直接写文件
 */
public interface RecordWriter extends AutoCloseable {
    
    /**
     * 写入单行数据
     */
    void write(Row row) throws IOException;
    
    /**
     * 准备提交
     * 刷写所有缓冲数据到磁盘,返回新生成的文件元信息
     */
    List<DataFileMeta> prepareCommit() throws IOException;
    
    /**
     * 关闭写入器
     */
    @Override
    void close() throws IOException;
}

