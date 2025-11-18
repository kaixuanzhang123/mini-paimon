package com.mini.paimon.schema;

public enum TableType {
    APPEND_ONLY,
    PRIMARY_KEY;
    
    public static TableType fromSchema(Schema schema) {
        return schema.hasPrimaryKey() ? PRIMARY_KEY : APPEND_ONLY;
    }
    
    public boolean isPrimaryKey() {
        return this == PRIMARY_KEY;
    }
    
    public boolean isAppendOnly() {
        return this == APPEND_ONLY;
    }
}

