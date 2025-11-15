package com.mini.paimon.format;

import com.mini.paimon.metadata.Schema;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;

import javax.annotation.Nullable;
import java.util.List;

/**
 * 数据文件格式抽象类
 * 参考 Paimon FileFormat 设计
 * 
 * 用于创建不同格式的数据文件读写器工厂:
 * - CSV格式 (用于非主键表)
 * - SST格式 (用于主键表)
 */
public abstract class FileFormat {
    
    /** 格式标识符 */
    private final String identifier;
    
    protected FileFormat(String identifier) {
        this.identifier = identifier;
    }
    
    /**
     * 获取格式标识符
     * 
     * @return 格式名称 (例如: "csv", "sst")
     */
    public String identifier() {
        return identifier;
    }
    
    /**
     * 创建读取器工厂
     * 
     * @param schema 数据Schema
     * @param projection 投影（选择的列）
     * @param filters 过滤条件
     * @return 读取器工厂
     */
    public abstract FormatReaderFactory createReaderFactory(
            Schema schema,
            @Nullable Projection projection,
            @Nullable List<Predicate> filters);
    
    /**
     * 创建写入器工厂
     * 
     * @param schema 数据Schema
     * @return 写入器工厂
     */
    public abstract FormatWriterFactory createWriterFactory(Schema schema);
}

