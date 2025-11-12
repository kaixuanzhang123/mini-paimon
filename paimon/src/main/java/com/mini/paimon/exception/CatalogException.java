package com.mini.paimon.exception;

import com.mini.paimon.catalog.Identifier;

/**
 * Catalog 异常类
 * 参考 Apache Paimon 的异常设计
 */
public class CatalogException extends MiniPaimonException {
    private static final long serialVersionUID = 1L;
    
    public CatalogException(String message) {
        super(message);
    }
    
    public CatalogException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public CatalogException(Throwable cause) {
        super(cause);
    }
    
    /**
     * 表已存在异常
     */
    public static class TableAlreadyExistException extends CatalogException {
        public TableAlreadyExistException(Identifier identifier) {
            super("Table already exists: " + identifier.getFullName());
        }
        
        public TableAlreadyExistException(Identifier identifier, Throwable cause) {
            super("Table already exists: " + identifier.getFullName(), cause);
        }
    }
    
    /**
     * 表不存在异常
     */
    public static class TableNotExistException extends CatalogException {
        public TableNotExistException(Identifier identifier) {
            super("Table does not exist: " + identifier.getFullName());
        }
        
        public TableNotExistException(Identifier identifier, Throwable cause) {
            super("Table does not exist: " + identifier.getFullName(), cause);
        }
    }
    
    /**
     * 数据库已存在异常
     */
    public static class DatabaseAlreadyExistException extends CatalogException {
        public DatabaseAlreadyExistException(String database) {
            super("Database already exists: " + database);
        }
        
        public DatabaseAlreadyExistException(String database, Throwable cause) {
            super("Database already exists: " + database, cause);
        }
    }
    
    /**
     * 数据库不存在异常
     */
    public static class DatabaseNotExistException extends CatalogException {
        public DatabaseNotExistException(String database) {
            super("Database does not exist: " + database);
        }
        
        public DatabaseNotExistException(String database, Throwable cause) {
            super("Database does not exist: " + database, cause);
        }
    }
    
    /**
     * 数据库非空异常（删除时）
     */
    public static class DatabaseNotEmptyException extends CatalogException {
        public DatabaseNotEmptyException(String database) {
            super("Database is not empty: " + database);
        }
    }
}
