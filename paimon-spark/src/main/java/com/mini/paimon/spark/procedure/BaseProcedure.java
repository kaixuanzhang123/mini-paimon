package com.mini.paimon.spark.procedure;

import com.mini.paimon.catalog.Catalog;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Spark Procedure 基类
 * 
 * 提供通用的 Catalog 访问能力和参数验证
 * 
 * 注意：Spark 3.3.x 没有标准的 Procedure 接口，这是一个自定义接口
 * 在实际使用中，可以通过 Spark SQL 的自定义函数或者扩展机制来调用
 */
public abstract class BaseProcedure {
    
    protected final Catalog catalog;
    
    public BaseProcedure(Catalog catalog) {
        this.catalog = catalog;
    }
    
    /**
     * 获取 Catalog 实例
     */
    protected Catalog catalog() {
        return catalog;
    }
    
    /**
     * 获取 Procedure 名称
     */
    public abstract String name();
    
    /**
     * 获取输出 Schema
     */
    public abstract StructType outputType();
    
    /**
     * 执行 Procedure
     * 
     * @param args 参数数组
     * @return 执行结果
     */
    public abstract InternalRow[] call(InternalRow args);
    
    /**
     * 验证参数是否为空
     */
    protected void checkNotNull(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }
    
    /**
     * 验证字符串参数是否为空或空白
     */
    protected void checkNotBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " cannot be null or blank");
        }
    }
    
    /**
     * 创建简单的输出 Schema（单个字符串字段）
     */
    protected StructType singleStringOutput() {
        return new StructType(new StructField[]{
            new StructField("result", DataTypes.StringType, false, Metadata.empty())
        });
    }
    
    /**
     * 创建表格输出 Schema（branch_name 字段）
     */
    protected StructType branchListOutput() {
        return new StructType(new StructField[]{
            new StructField("branch_name", DataTypes.StringType, false, Metadata.empty())
        });
    }
    
    /**
     * 创建成功消息的输出行
     */
    protected InternalRow[] successRow(String message) {
        return new InternalRow[]{
            new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
                new Object[]{org.apache.spark.unsafe.types.UTF8String.fromString(message)}
            )
        };
    }
}

