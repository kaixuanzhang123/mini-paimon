package com.mini.paimon.flink.catalog;

import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.table.Table;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkTableFactory {

    public static CatalogBaseTable createFlinkTable(Table paimonTable, String warehouse) {
        Schema schema = paimonTable.schema();
        
        org.apache.flink.table.api.Schema.Builder schemaBuilder = 
            org.apache.flink.table.api.Schema.newBuilder();
        
        for (Field field : schema.getFields()) {
            org.apache.flink.table.types.DataType flinkType = FlinkSchemaConverter.toFlinkType(field.getType());
            if (field.isNullable()) {
                schemaBuilder.column(field.getName(), flinkType);
            } else {
                schemaBuilder.column(field.getName(), flinkType.notNull());
            }
        }
        
        if (!schema.getPrimaryKeys().isEmpty()) {
            schemaBuilder.primaryKey(schema.getPrimaryKeys());
        }
        
        List<String> partitionKeys = schema.getPartitionKeys();
        
        Map<String, String> options = new HashMap<>();
        options.put("connector", "mini-paimon");
        if (warehouse != null && !warehouse.isEmpty()) {
            options.put("warehouse", warehouse);
        }
        
        return CatalogTable.of(
            schemaBuilder.build(),
            null,
            partitionKeys,
            options
        );
    }
}
