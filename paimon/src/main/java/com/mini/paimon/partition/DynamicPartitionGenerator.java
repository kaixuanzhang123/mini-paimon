package com.mini.paimon.partition;

import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DynamicPartitionGenerator {
    
    private final Schema schema;
    private final List<String> partitionKeys;
    
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("HH");
    
    public DynamicPartitionGenerator(Schema schema) {
        this.schema = schema;
        this.partitionKeys = schema.getPartitionKeys();
    }
    
    public PartitionSpec generatePartitionSpec(Row row) {
        if (partitionKeys.isEmpty()) {
            return new PartitionSpec(new LinkedHashMap<>());
        }
        
        Map<String, String> partitionValues = new LinkedHashMap<>();
        
        for (String partitionKey : partitionKeys) {
            int fieldIndex = schema.getFieldIndex(partitionKey);
            if (fieldIndex < 0) {
                throw new IllegalArgumentException("Partition key not found in schema: " + partitionKey);
            }
            
            Object value = row.getValue(fieldIndex);
            String partitionValue = formatPartitionValue(partitionKey, value, schema.getField(partitionKey));
            partitionValues.put(partitionKey, partitionValue);
        }
        
        return new PartitionSpec(partitionValues);
    }
    
    private String formatPartitionValue(String fieldName, Object value, Field field) {
        if (value == null) {
            return "null";
        }
        
        if (value instanceof LocalDateTime) {
            LocalDateTime dateTime = (LocalDateTime) value;
            if (fieldName.equalsIgnoreCase("dt") || fieldName.equalsIgnoreCase("date")) {
                return dateTime.format(DATE_FORMATTER);
            } else if (fieldName.equalsIgnoreCase("hour")) {
                return dateTime.format(HOUR_FORMATTER);
            }
        }
        
        return value.toString();
    }
    
    public static PartitionSpec generateTimePartition(LocalDateTime dateTime) {
        Map<String, String> partitionValues = new LinkedHashMap<>();
        partitionValues.put("dt", dateTime.format(DATE_FORMATTER));
        return new PartitionSpec(partitionValues);
    }
    
    public static PartitionSpec generateHourlyPartition(LocalDateTime dateTime) {
        Map<String, String> partitionValues = new LinkedHashMap<>();
        partitionValues.put("dt", dateTime.format(DATE_FORMATTER));
        partitionValues.put("hour", dateTime.format(HOUR_FORMATTER));
        return new PartitionSpec(partitionValues);
    }
}

