package com.mini.paimon.catalog;

import java.io.Serializable;
import java.util.Objects;

/**
 * 表标识符
 * 参考 Apache Paimon 的 Identifier 设计
 * 
 * 用于唯一标识一个表（database.table）
 */
public class Identifier implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /** 数据库名 */
    private final String database;
    
    /** 表名 */
    private final String table;
    
    /**
     * 创建表标识符
     * 
     * @param database 数据库名
     * @param table 表名
     */
    public Identifier(String database, String table) {
        this.database = Objects.requireNonNull(database, "Database name cannot be null");
        this.table = Objects.requireNonNull(table, "Table name cannot be null");
        
        validateName(database, "database");
        validateName(table, "table");
    }
    
    /**
     * 从字符串创建标识符（格式：database.table）
     * 
     * @param fullName 完整表名
     * @return 表标识符
     */
    public static Identifier fromString(String fullName) {
        Objects.requireNonNull(fullName, "Full name cannot be null");
        
        String[] parts = fullName.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                "Invalid table identifier: " + fullName + ". Expected format: database.table");
        }
        
        return new Identifier(parts[0], parts[1]);
    }
    
    /**
     * 验证名称的有效性
     */
    private void validateName(String name, String type) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException(type + " name cannot be empty");
        }
        
        // 检查是否包含非法字符
        if (!name.matches("^[a-zA-Z0-9_]+$")) {
            throw new IllegalArgumentException(
                type + " name can only contain letters, numbers and underscores: " + name);
        }
    }
    
    public String getDatabase() {
        return database;
    }
    
    public String getTable() {
        return table;
    }
    
    /**
     * 获取完整表名（database.table）
     */
    public String getFullName() {
        return database + "." + table;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Identifier that = (Identifier) o;
        return database.equals(that.database) && table.equals(that.table);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(database, table);
    }
    
    @Override
    public String toString() {
        return getFullName();
    }
}
