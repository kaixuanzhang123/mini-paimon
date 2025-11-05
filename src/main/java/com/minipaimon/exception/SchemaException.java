package com.minipaimon.exception;

/**
 * Schema 相关异常
 */
public class SchemaException extends MiniPaimonException {
    
    public SchemaException(String message) {
        super(message);
    }
    
    public SchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
