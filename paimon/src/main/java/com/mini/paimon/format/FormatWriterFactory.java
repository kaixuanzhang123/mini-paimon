package com.mini.paimon.format;

import com.mini.paimon.table.write.KeyValueFileWriter;

import java.io.IOException;
import java.nio.file.Path;

/**
 * 格式写入器工厂接口
 * 参考 Paimon FormatWriterFactory 设计
 * 
 * 负责根据上下文创建具体的文件写入器
 */
public interface FormatWriterFactory {
    
    /**
     * 创建文件写入器
     * 
     * @param context 写入器上下文
     * @return KeyValue文件写入器
     * @throws IOException IO异常
     */
    KeyValueFileWriter createWriter(WriterContext context) throws IOException;
    
    /**
     * 写入器上下文
     * 封装创建写入器所需的所有信息
     */
    interface WriterContext {
        /**
         * 获取文件路径
         */
        Path filePath();
        
        /**
         * 获取压缩格式（可选）
         * 
         * @return 压缩格式，如"gzip"、"snappy"等，null表示不压缩
         */
        String compression();
    }
}

