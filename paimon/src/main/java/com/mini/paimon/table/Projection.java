package com.mini.paimon.table;

import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Projection
 * 字段投影，用于 SELECT 指定列
 */
public class Projection {
    private final List<String> selectedFields;
    
    private Projection(List<String> selectedFields) {
        this.selectedFields = selectedFields;
    }
    
    /**
     * 创建全字段投影（SELECT *）
     */
    public static Projection all() {
        return new Projection(null);
    }
    
    /**
     * 创建指定字段投影
     */
    public static Projection of(List<String> fields) {
        return new Projection(new ArrayList<>(fields));
    }
    
    /**
     * 创建指定字段投影
     */
    public static Projection of(String... fields) {
        List<String> fieldList = new ArrayList<>();
        for (String field : fields) {
            fieldList.add(field);
        }
        return new Projection(fieldList);
    }
    
    /**
     * 是否是全字段投影
     */
    public boolean isAll() {
        return selectedFields == null;
    }
    
    /**
     * 获取选中的字段列表
     */
    public List<String> getSelectedFields() {
        return selectedFields;
    }
    
    /**
     * 对行进行投影
     */
    public Row project(Row row, Schema schema) {
        if (isAll()) {
            return row;
        }
        
        Object[] originalValues = row.getValues();
        Object[] projectedValues = new Object[selectedFields.size()];
        
        List<Field> schemaFields = schema.getFields();
        
        for (int i = 0; i < selectedFields.size(); i++) {
            String fieldName = selectedFields.get(i);
            
            // 查找字段在 schema 中的索引
            int fieldIndex = -1;
            for (int j = 0; j < schemaFields.size(); j++) {
                if (schemaFields.get(j).getName().equals(fieldName)) {
                    fieldIndex = j;
                    break;
                }
            }
            
            if (fieldIndex == -1) {
                throw new IllegalArgumentException("Field not found: " + fieldName);
            }
            
            projectedValues[i] = originalValues[fieldIndex];
        }
        
        return new Row(projectedValues);
    }
    
    /**
     * 获取投影后的 Schema
     */
    public Schema projectSchema(Schema schema) {
        if (isAll()) {
            return schema;
        }
        
        List<Field> originalFields = schema.getFields();
        List<Field> projectedFields = new ArrayList<>();
        
        for (String fieldName : selectedFields) {
            for (Field field : originalFields) {
                if (field.getName().equals(fieldName)) {
                    projectedFields.add(field);
                    break;
                }
            }
        }
        
        // 投影后的表没有主键
        return new Schema(schema.getSchemaId(), projectedFields, new ArrayList<>(), new ArrayList<>());
    }
}
