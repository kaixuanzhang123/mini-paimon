package com.mini.paimon.flink.table;

import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.Schema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

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
        
        String typeName = dataType.typeName();
        if ("INT".equals(typeName)) {
            return rowData.getInt(pos);
        } else if ("BIGINT".equals(typeName) || "LONG".equals(typeName)) {
            return rowData.getLong(pos);
        } else if ("STRING".equals(typeName)) {
            return rowData.getString(pos).toString();
        } else if ("BOOLEAN".equals(typeName)) {
            return rowData.getBoolean(pos);
        } else if ("DOUBLE".equals(typeName)) {
            return rowData.getDouble(pos);
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + typeName);
        }
    }
    
    private static Object convertToFlinkValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        
        String typeName = dataType.typeName();
        if ("INT".equals(typeName)) {
            if (value instanceof Integer) {
                return value;
            } else if (value instanceof Long) {
                return ((Long) value).intValue();
            } else if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return value;
        } else if ("BIGINT".equals(typeName) || "LONG".equals(typeName)) {
            if (value instanceof Long) {
                return value;
            } else if (value instanceof Integer) {
                return ((Integer) value).longValue();
            } else if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            return value;
        } else if ("DOUBLE".equals(typeName)) {
            if (value instanceof Double) {
                return value;
            } else if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            return value;
        } else if ("STRING".equals(typeName)) {
            return StringData.fromString(value.toString());
        } else if ("BOOLEAN".equals(typeName)) {
            return value;
        } else {
            return value;
        }
    }
}
