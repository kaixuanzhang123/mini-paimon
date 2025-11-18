package com.mini.paimon.spark.catalog;

import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Schema;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkSchemaConverter {

    public static Schema toSchema(StructType sparkSchema, Transform[] partitions, Map<String, String> properties) {
        List<Field> fields = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        List<String> partitionKeys = new ArrayList<>();
        
        for (StructField sparkField : sparkSchema.fields()) {
            String fieldName = sparkField.name();
            DataType dataType = toDataType(sparkField.dataType());
            boolean nullable = sparkField.nullable();
            
            fields.add(new Field(fieldName, dataType, nullable));
        }
        
        if (properties != null && properties.containsKey("primary-key")) {
            String pkStr = properties.get("primary-key");
            String[] pks = pkStr.split(",");
            for (String pk : pks) {
                primaryKeys.add(pk.trim());
            }
        }
        
        if (partitions != null) {
            for (Transform transform : partitions) {
                if (transform.name().equals("identity") && transform.references().length > 0) {
                    NamedReference ref = (NamedReference) transform.references()[0];
                    partitionKeys.add(ref.fieldNames()[0]);
                }
            }
        }
        
        return new Schema(1, fields, primaryKeys, partitionKeys);
    }

    public static DataType toDataType(org.apache.spark.sql.types.DataType sparkType) {
        if (sparkType instanceof IntegerType) {
            return DataType.INT();
        } else if (sparkType instanceof LongType) {
            return DataType.LONG();
        } else if (sparkType instanceof StringType) {
            return DataType.STRING();
        } else if (sparkType instanceof BooleanType) {
            return DataType.BOOLEAN();
        } else if (sparkType instanceof DoubleType) {
            return DataType.DOUBLE();
        } else {
            throw new UnsupportedOperationException("Unsupported Spark type: " + sparkType);
        }
    }

    public static StructType toSparkSchema(Schema schema) {
        List<StructField> sparkFields = new ArrayList<>();
        
        for (Field field : schema.getFields()) {
            org.apache.spark.sql.types.DataType sparkType = toSparkType(field.getType());
            StructField sparkField = new StructField(
                field.getName(),
                sparkType,
                field.isNullable(),
                Metadata.empty()
            );
            sparkFields.add(sparkField);
        }
        
        return new StructType(sparkFields.toArray(new StructField[0]));
    }

    public static org.apache.spark.sql.types.DataType toSparkType(DataType dataType) {
        String typeName = dataType.typeName();
        if ("INT".equals(typeName)) {
            return DataTypes.IntegerType;
        } else if ("BIGINT".equals(typeName) || "LONG".equals(typeName)) {
            return DataTypes.LongType;
        } else if ("STRING".equals(typeName)) {
            return DataTypes.StringType;
        } else if ("BOOLEAN".equals(typeName)) {
            return DataTypes.BooleanType;
        } else if ("DOUBLE".equals(typeName)) {
            return DataTypes.DoubleType;
        } else {
            throw new UnsupportedOperationException("Unsupported Paimon type: " + dataType);
        }
    }
}

