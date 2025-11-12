package com.mini.paimon.flink.catalog;

import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.types.logical.*;

import java.util.ArrayList;
import java.util.List;

public class FlinkSchemaConverter {

    public static Schema toSchema(CatalogBaseTable flinkTable) {
        if (!(flinkTable instanceof CatalogTable)) {
            throw new UnsupportedOperationException("Only CatalogTable is supported");
        }
        
        CatalogTable catalogTable = (CatalogTable) flinkTable;
        TableSchema tableSchema = catalogTable.getSchema();
        
        List<String> primaryKeys = new ArrayList<>();
        if (tableSchema.getPrimaryKey().isPresent()) {
            primaryKeys = tableSchema.getPrimaryKey().get().getColumns();
        }
        
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            String fieldName = tableSchema.getFieldNames()[i];
            LogicalType logicalType = tableSchema.getFieldDataTypes()[i].getLogicalType();
            DataType dataType = toDataType(logicalType);
            
            boolean nullable = !primaryKeys.contains(fieldName);
            fields.add(new Field(fieldName, dataType, nullable));
        }
        
        List<String> partitionKeys = catalogTable.getPartitionKeys();
        
        return new Schema(1, fields, primaryKeys, partitionKeys);
    }
    
    public static DataType toDataType(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case INTEGER:
                return DataType.INT;
            case BIGINT:
                return DataType.LONG;
            case VARCHAR:
            case CHAR:
                return DataType.STRING;
            case BOOLEAN:
                return DataType.BOOLEAN;
            case DOUBLE:
                return DataType.DOUBLE;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported Flink type: " + logicalType.getTypeRoot());
        }
    }
    
    public static org.apache.flink.table.api.DataTypes.Field toFlinkField(Field field) {
        return org.apache.flink.table.api.DataTypes.FIELD(
            field.getName(), 
            toFlinkType(field.getType())
        );
    }
    
    public static org.apache.flink.table.types.DataType toFlinkType(DataType dataType) {
        switch (dataType) {
            case INT:
                return org.apache.flink.table.api.DataTypes.INT();
            case LONG:
                return org.apache.flink.table.api.DataTypes.BIGINT();
            case STRING:
                return org.apache.flink.table.api.DataTypes.STRING();
            case BOOLEAN:
                return org.apache.flink.table.api.DataTypes.BOOLEAN();
            case DOUBLE:
                return org.apache.flink.table.api.DataTypes.DOUBLE();
            default:
                throw new UnsupportedOperationException(
                    "Unsupported Paimon type: " + dataType);
        }
    }
}
