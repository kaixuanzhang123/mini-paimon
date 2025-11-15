package com.mini.paimon.format;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据文件格式工厂
 * 参考 Paimon FileFormatFactory 设计
 * 
 * 支持的格式：
 * - csv: CSV格式（用于非主键表）
 * - sst: SSTable格式（用于主键表）
 */
public class FileFormatFactory {
    
    private static final Map<String, FileFormat> FORMATS = new ConcurrentHashMap<>();
    
    static {
        // 注册默认支持的数据文件格式
        register(new CsvFileFormat());
        register(new SSTFileFormat());
    }
    
    /**
     * 注册文件格式
     */
    public static void register(FileFormat format) {
        FORMATS.put(format.identifier().toLowerCase(), format);
    }
    
    /**
     * 根据标识符获取文件格式
     */
    public static FileFormat getFormat(String identifier) {
        FileFormat format = FORMATS.get(identifier.toLowerCase());
        if (format == null) {
            throw new IllegalArgumentException("Unsupported file format: " + identifier);
        }
        return format;
    }
    
    /**
     * 根据文件扩展名获取文件格式
     * 
     * @param fileName 文件名
     * @return 文件格式
     */
    public static FileFormat getFormatByFileName(String fileName) {
        if (fileName.endsWith(".csv")) {
            return getFormat("csv");
        } else if (fileName.endsWith(".sst")) {
            return getFormat("sst");
        } else {
            // 默认使用SST格式（向后兼容）
            return getFormat("sst");
        }
    }
    
    /**
     * 检查格式是否支持
     */
    public static boolean isSupported(String identifier) {
        return FORMATS.containsKey(identifier.toLowerCase());
    }
}

