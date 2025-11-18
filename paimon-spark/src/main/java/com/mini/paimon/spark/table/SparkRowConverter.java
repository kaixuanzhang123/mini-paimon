package com.mini.paimon.spark.table;

import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkRowConverter {

    public static InternalRow toInternalRow(Row paimonRow, Schema schema) {
        Object[] values = new Object[paimonRow.getFieldCount()];
        
        for (int i = 0; i < paimonRow.getFieldCount(); i++) {
            Object value = paimonRow.getValue(i);
            Field field = schema.getFields().get(i);
            values[i] = convertToSparkValue(value, field.getType());
        }
        
        return new GenericInternalRow(values);
    }

    public static Row toPaimonRow(InternalRow sparkRow, StructType sparkSchema, Schema paimonSchema) {
        Object[] values = new Object[sparkSchema.fields().length];
        
        for (int i = 0; i < sparkSchema.fields().length; i++) {
            Field field = paimonSchema.getFields().get(i);
            Object sparkValue = getSparkValue(sparkRow, i, field.getType());
            values[i] = convertToPaimonValue(sparkValue, field.getType());
        }
        
        return new Row(values);
    }

    private static Object convertToSparkValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        
        String typeName = dataType.typeName();
        if ("STRING".equals(typeName)) {
            return UTF8String.fromString((String) value);
        } else if ("INT".equals(typeName)) {
            // 确保返回 Integer 类型
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return value;
        } else if ("BIGINT".equals(typeName) || "LONG".equals(typeName)) {
            // 确保返回 Long 类型 - 修复 JSON 反序列化时可能返回 Integer 的问题
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            return value;
        } else if ("DOUBLE".equals(typeName)) {
            // 确保返回 Double 类型
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            return value;
        } else if ("BOOLEAN".equals(typeName)) {
            return value;
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static Object getSparkValue(InternalRow row, int ordinal, DataType dataType) {
        if (row.isNullAt(ordinal)) {
            return null;
        }
        
        String typeName = dataType.typeName();
        if ("INT".equals(typeName)) {
            return row.getInt(ordinal);
        } else if ("BIGINT".equals(typeName) || "LONG".equals(typeName)) {
            return row.getLong(ordinal);
        } else if ("STRING".equals(typeName)) {
            return row.getUTF8String(ordinal);
        } else if ("BOOLEAN".equals(typeName)) {
            return row.getBoolean(ordinal);
        } else if ("DOUBLE".equals(typeName)) {
            return row.getDouble(ordinal);
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static Object convertToPaimonValue(Object sparkValue, DataType dataType) {
        if (sparkValue == null) {
            return null;
        }
        
        String typeName = dataType.typeName();
        if ("STRING".equals(typeName)) {
            if (sparkValue instanceof UTF8String) {
                return ((UTF8String) sparkValue).toString();
            }
            return sparkValue.toString();
        } else if ("INT".equals(typeName) || "BIGINT".equals(typeName) || "LONG".equals(typeName) || 
                   "BOOLEAN".equals(typeName) || "DOUBLE".equals(typeName)) {
            return sparkValue;
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }
}

