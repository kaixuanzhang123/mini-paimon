package com.mini.paimon.exception;

/**
 * Mini Paimon 基础异常类
 */
public class MiniPaimonException extends RuntimeException {
    
    public MiniPaimonException(String message) {
        super(message);
    }
    
    public MiniPaimonException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public MiniPaimonException(Throwable cause) {
        super(cause);
    }
}
