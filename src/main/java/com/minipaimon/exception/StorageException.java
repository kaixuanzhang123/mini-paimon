package com.minipaimon.exception;

/**
 * 存储相关异常
 */
public class StorageException extends MiniPaimonException {
    
    public StorageException(String message) {
        super(message);
    }
    
    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
