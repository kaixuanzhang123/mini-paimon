package com.mini.paimon.format;

import com.mini.paimon.reader.KeyValueFileReader;

import java.io.IOException;

/**
 * 格式读取器工厂接口
 * 参考 Paimon FormatReaderFactory 设计
 * 
 * 负责根据上下文创建具体的文件读取器
 */
public interface FormatReaderFactory {
    
    /**
     * 创建文件读取器
     * 
     * @param context 读取器上下文
     * @return KeyValue文件读取器
     * @throws IOException IO异常
     */
    KeyValueFileReader createReader(ReaderContext context) throws IOException;
    
    /**
     * 读取器上下文
     * 封装创建读取器所需的所有信息
     */
    interface ReaderContext {
        /**
         * 获取文件路径
         */
        String filePath();
    }
}

