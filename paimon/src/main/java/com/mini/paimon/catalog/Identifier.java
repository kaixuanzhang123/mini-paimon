package com.mini.paimon.catalog;

import java.io.Serializable;
import java.util.Objects;

/**
 * 表标识符
 * 参考 Apache Paimon 的 Identifier 设计
 * 
 * 用于唯一标识一个表（database.table）
 * 支持分支和系统表：database.table$branch_xxx$systemTable
 */
public class Identifier implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /** 系统表分隔符 */
    public static final String SYSTEM_TABLE_SPLITTER = "$";
    
    /** 分支前缀 */
    public static final String SYSTEM_BRANCH_PREFIX = "branch_";
    
    /** 默认主分支 */
    public static final String DEFAULT_MAIN_BRANCH = "main";
    
    /** 数据库名 */
    private final String database;
    
    /** 完整对象名（可能包含分支和系统表） */
    private final String object;
    
    /** 表名（不包含分支和系统表） */
    private transient String table;
    
    /** 分支名 */
    private transient String branch;
    
    /** 系统表名 */
    private transient String systemTable;
    
    /**
     * 创建表标识符
     * 
     * @param database 数据库名
     * @param object 对象名（可能包含分支和系统表）
     */
    public Identifier(String database, String object) {
        this.database = Objects.requireNonNull(database, "Database name cannot be null");
        this.object = Objects.requireNonNull(object, "Object name cannot be null");
        
        validateName(database, "database");
        splitObjectName();
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
     * 解析对象名，提取表名、分支名和系统表名
     */
    private void splitObjectName() {
        String[] splits = object.split("\\" + SYSTEM_TABLE_SPLITTER, -1);
        
        if (splits.length == 1) {
            // 格式：table
            this.table = object;
            this.branch = null;
            this.systemTable = null;
        } else if (splits.length == 2) {
            // 格式：table$branch_xxx 或 table$systemTable
            this.table = splits[0];
            if (splits[1].startsWith(SYSTEM_BRANCH_PREFIX)) {
                this.branch = splits[1].substring(SYSTEM_BRANCH_PREFIX.length());
                this.systemTable = null;
            } else {
                this.branch = null;
                this.systemTable = splits[1];
            }
        } else if (splits.length == 3) {
            // 格式：table$branch_xxx$systemTable
            this.table = splits[0];
            if (!splits[1].startsWith(SYSTEM_BRANCH_PREFIX)) {
                throw new IllegalArgumentException(
                    "System table can only contain one '" + SYSTEM_TABLE_SPLITTER + "' separator, but this is: " + object);
            }
            this.branch = splits[1].substring(SYSTEM_BRANCH_PREFIX.length());
            this.systemTable = splits[2];
        } else {
            throw new IllegalArgumentException("Invalid object name: " + object);
        }
        
        validateName(table, "table");
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
     * 获取分支名（可能为 null）
     */
    public String getBranch() {
        return branch;
    }
    
    /**
     * 获取分支名，如果为 null 则返回默认主分支
     */
    public String getBranchNameOrDefault() {
        return branch == null ? DEFAULT_MAIN_BRANCH : branch;
    }
    
    /**
     * 获取系统表名（可能为 null）
     */
    public String getSystemTableName() {
        return systemTable;
    }
    
    /**
     * 获取完整表名（database.table）
     */
    public String getFullName() {
        return database + "." + table;
    }
    
    /**
     * 获取完整对象名（可能包含分支和系统表）
     */
    public String getObjectName() {
        return object;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Identifier that = (Identifier) o;
        return database.equals(that.database) && object.equals(that.object);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(database, object);
    }
    
    @Override
    public String toString() {
        return database + "." + object;
    }
}
