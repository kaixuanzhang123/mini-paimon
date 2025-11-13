package com.mini.paimon.flink.table;

import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.util.ArrayList;
import java.util.List;

public class FlinkRowConverter {

    public static Row toRow(RowData rowData, Schema schema) {
        Object[] values = new Object[schema.getFields().size()];
        
        for (int i = 0; i < schema.getFields().size(); i++) {
            Field field = schema.getFields().get(i);
            DataType dataType = field.getType();
            
            Object value = extractValue(rowData, i, dataType);
            values[i] = value;
        }
        
        return new Row(values);
    }
    
    public static RowData toRowData(Row row, Schema schema) {
        GenericRowData rowData = new GenericRowData(schema.getFields().size());
        
        for (int i = 0; i < schema.getFields().size(); i++) {
            Field field = schema.getFields().get(i);
            Object value = row.getValue(i);
            
            Object convertedValue = convertToFlinkValue(value, field.getType());
            rowData.setField(i, convertedValue);
        }
        
        return rowData;
    }
    
    private static Object extractValue(RowData rowData, int pos, DataType dataType) {
        if (rowData.isNullAt(pos)) {
            return null;
        }
        
        switch (dataType) {
            case INT:
                return rowData.getInt(pos);
            case LONG:
                return rowData.getLong(pos);
            case STRING:
                return rowData.getString(pos).toString();
            case BOOLEAN:
                return rowData.getBoolean(pos);
            case DOUBLE:
                return rowData.getDouble(pos);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }
    
    private static Object convertToFlinkValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        
        switch (dataType) {
            case INT:
                if (value instanceof Integer) {
                    return value;
                } else if (value instanceof Long) {
                    return ((Long) value).intValue();
                } else if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return value;
            case LONG:
                if (value instanceof Long) {
                    return value;
                } else if (value instanceof Integer) {
                    return ((Integer) value).longValue();
                } else if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return value;
            case DOUBLE:
                if (value instanceof Double) {
                    return value;
                } else if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return value;
            case STRING:
                return StringData.fromString(value.toString());
            case BOOLEAN:
                return value;
            default:
                return value;
        }
    }
}
