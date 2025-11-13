package com.mini.paimon.spark.table;

import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
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
        
        switch (dataType) {
            case STRING:
                return UTF8String.fromString((String) value);
            case INT:
            case LONG:
            case BOOLEAN:
            case DOUBLE:
                return value;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static Object getSparkValue(InternalRow row, int ordinal, DataType dataType) {
        if (row.isNullAt(ordinal)) {
            return null;
        }
        
        switch (dataType) {
            case INT:
                return row.getInt(ordinal);
            case LONG:
                return row.getLong(ordinal);
            case STRING:
                return row.getUTF8String(ordinal);
            case BOOLEAN:
                return row.getBoolean(ordinal);
            case DOUBLE:
                return row.getDouble(ordinal);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static Object convertToPaimonValue(Object sparkValue, DataType dataType) {
        if (sparkValue == null) {
            return null;
        }
        
        switch (dataType) {
            case STRING:
                if (sparkValue instanceof UTF8String) {
                    return ((UTF8String) sparkValue).toString();
                }
                return sparkValue.toString();
            case INT:
            case LONG:
            case BOOLEAN:
            case DOUBLE:
                return sparkValue;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }
}

