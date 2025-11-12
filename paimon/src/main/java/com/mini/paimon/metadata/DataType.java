package com.mini.paimon.metadata;

/**
 * 数据类型枚举
 * 支持的基本数据类型
 */
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum DataType {
    /** 32位整数 */
    INT(4),
    
    /** 64位长整数 */
    LONG(8),
    
    /** 字符串类型（变长） */
    STRING(-1),
    
    /** 布尔类型 */
    BOOLEAN(1),
    
    /** 双精度浮点数 */
    DOUBLE(8);

    /** 固定长度类型的字节数，-1表示变长 */
    private final int fixedSize;

    DataType(int fixedSize) {
        this.fixedSize = fixedSize;
    }

    public int getFixedSize() {
        return fixedSize;
    }

    public boolean isFixedLength() {
        return fixedSize > 0;
    }
}