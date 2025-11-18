package com.mini.paimon.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = DataType.IntType.class, name = "INT"),
    @JsonSubTypes.Type(value = DataType.LongType.class, name = "BIGINT"),
    @JsonSubTypes.Type(value = DataType.StringType.class, name = "STRING"),
    @JsonSubTypes.Type(value = DataType.BooleanType.class, name = "BOOLEAN"),
    @JsonSubTypes.Type(value = DataType.DoubleType.class, name = "DOUBLE"),
    @JsonSubTypes.Type(value = DataType.TimestampType.class, name = "TIMESTAMP"),
    @JsonSubTypes.Type(value = DataType.DecimalType.class, name = "DECIMAL"),
    @JsonSubTypes.Type(value = DataType.ArrayType.class, name = "ARRAY"),
    @JsonSubTypes.Type(value = DataType.MapType.class, name = "MAP")
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class DataType {
    
    @com.fasterxml.jackson.annotation.JsonIgnore
    public abstract String typeName();
    
    @com.fasterxml.jackson.annotation.JsonIgnore
    public abstract int getFixedSize();
    
    @com.fasterxml.jackson.annotation.JsonIgnore
    public boolean isFixedLength() {
        return getFixedSize() > 0;
    }
    
    @com.fasterxml.jackson.annotation.JsonIgnore
    public abstract boolean isCompatible(Object value);
    
    public static DataType INT() {
        return IntType.INSTANCE;
    }
    
    public static DataType LONG() {
        return LongType.INSTANCE;
    }
    
    public static DataType STRING() {
        return StringType.INSTANCE;
    }
    
    public static DataType BOOLEAN() {
        return BooleanType.INSTANCE;
    }
    
    public static DataType DOUBLE() {
        return DoubleType.INSTANCE;
    }
    
    public static DataType TIMESTAMP() {
        return TimestampType.INSTANCE;
    }
    
    public static DataType DECIMAL(int precision, int scale) {
        return new DecimalType(precision, scale);
    }
    
    public static DataType ARRAY(DataType elementType) {
        return new ArrayType(elementType);
    }
    
    public static DataType MAP(DataType keyType, DataType valueType) {
        return new MapType(keyType, valueType);
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class IntType extends DataType {
        public static final IntType INSTANCE = new IntType();
        
        private IntType() {}
        
        @Override
        public String typeName() {
            return "INT";
        }
        
        @Override
        public int getFixedSize() {
            return 4;
        }
        
        @Override
        public boolean isCompatible(Object value) {
            return value instanceof Integer || 
                   (value instanceof Number && isInIntRange((Number) value));
        }
        
        private boolean isInIntRange(Number number) {
            long longValue = number.longValue();
            return longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE;
        }
        
        @Override
        public String toString() {
            return "INT";
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LongType extends DataType {
        public static final LongType INSTANCE = new LongType();
        
        private LongType() {}
        
        @Override
        public String typeName() {
            return "BIGINT";
        }
        
        @Override
        public int getFixedSize() {
            return 8;
        }
        
        @Override
        public boolean isCompatible(Object value) {
            return value instanceof Long || value instanceof Integer || value instanceof Number;
        }
        
        @Override
        public String toString() {
            return "LONG";
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StringType extends DataType {
        public static final StringType INSTANCE = new StringType();
        
        private StringType() {}
        
        @Override
        public String typeName() {
            return "STRING";
        }
        
        @Override
        public int getFixedSize() {
            return -1;
        }
        
        @Override
        public boolean isCompatible(Object value) {
            return value instanceof String;
        }
        
        @Override
        public String toString() {
            return "STRING";
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BooleanType extends DataType {
        public static final BooleanType INSTANCE = new BooleanType();
        
        private BooleanType() {}
        
        @Override
        public String typeName() {
            return "BOOLEAN";
        }
        
        @Override
        public int getFixedSize() {
            return 1;
        }
        
        @Override
        public boolean isCompatible(Object value) {
            return value instanceof Boolean;
        }
        
        @Override
        public String toString() {
            return "BOOLEAN";
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DoubleType extends DataType {
        public static final DoubleType INSTANCE = new DoubleType();
        
        private DoubleType() {}
        
        @Override
        public String typeName() {
            return "DOUBLE";
        }
        
        @Override
        public int getFixedSize() {
            return 8;
        }
        
        @Override
        public boolean isCompatible(Object value) {
            return value instanceof Double || value instanceof Float || value instanceof Number;
        }
        
        @Override
        public String toString() {
            return "DOUBLE";
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TimestampType extends DataType {
        public static final TimestampType INSTANCE = new TimestampType();
        
        private TimestampType() {}
        
        @Override
        public String typeName() {
            return "TIMESTAMP";
        }
        
        @Override
        public int getFixedSize() {
            return 8;
        }
        
        @Override
        public boolean isCompatible(Object value) {
            return value instanceof LocalDateTime || value instanceof Long || 
                   value instanceof java.sql.Timestamp || value instanceof java.util.Date;
        }
        
        @Override
        public String toString() {
            return "TIMESTAMP";
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DecimalType extends DataType {
        private final int precision;
        private final int scale;
        
        @JsonCreator
        public DecimalType(
                @JsonProperty("precision") int precision, 
                @JsonProperty("scale") int scale) {
            this.precision = precision;
            this.scale = scale;
        }
        
        @Override
        public String typeName() {
            return "DECIMAL";
        }
        
        @Override
        public int getFixedSize() {
            return -1;
        }
        
        @Override
        public boolean isCompatible(Object value) {
            return value instanceof BigDecimal || value instanceof Number;
        }
        
        public int getPrecision() {
            return precision;
        }
        
        public int getScale() {
            return scale;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DecimalType)) return false;
            DecimalType that = (DecimalType) o;
            return precision == that.precision && scale == that.scale;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(precision, scale);
        }
        
        @Override
        public String toString() {
            return "DECIMAL(" + precision + "," + scale + ")";
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ArrayType extends DataType {
        private final DataType elementType;
        
        @JsonCreator
        public ArrayType(@JsonProperty("elementType") DataType elementType) {
            this.elementType = Objects.requireNonNull(elementType);
        }
        
        @Override
        public String typeName() {
            return "ARRAY";
        }
        
        @Override
    public int getFixedSize() {
            return -1;
        }
        
        @Override
        public boolean isCompatible(Object value) {
            if (!(value instanceof List)) {
                return false;
            }
            List<?> list = (List<?>) value;
            for (Object element : list) {
                if (element != null && !elementType.isCompatible(element)) {
                    return false;
                }
            }
            return true;
        }
        
        public DataType getElementType() {
            return elementType;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ArrayType)) return false;
            ArrayType arrayType = (ArrayType) o;
            return elementType.equals(arrayType.elementType);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(elementType);
        }
        
        @Override
        public String toString() {
            return "ARRAY<" + elementType + ">";
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MapType extends DataType {
        private final DataType keyType;
        private final DataType valueType;
        
        @JsonCreator
        public MapType(
                @JsonProperty("keyType") DataType keyType, 
                @JsonProperty("valueType") DataType valueType) {
            this.keyType = Objects.requireNonNull(keyType);
            this.valueType = Objects.requireNonNull(valueType);
        }
        
        @Override
        public String typeName() {
            return "MAP";
        }
        
        @Override
        public int getFixedSize() {
            return -1;
        }
        
        @Override
        public boolean isCompatible(Object value) {
            if (!(value instanceof Map)) {
                return false;
            }
            Map<?, ?> map = (Map<?, ?>) value;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() != null && !keyType.isCompatible(entry.getKey())) {
                    return false;
                }
                if (entry.getValue() != null && !valueType.isCompatible(entry.getValue())) {
                    return false;
                }
            }
            return true;
        }
        
        public DataType getKeyType() {
            return keyType;
        }
        
        public DataType getValueType() {
            return valueType;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MapType)) return false;
            MapType mapType = (MapType) o;
            return keyType.equals(mapType.keyType) && valueType.equals(mapType.valueType);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(keyType, valueType);
        }
        
        @Override
        public String toString() {
            return "MAP<" + keyType + "," + valueType + ">";
        }
    }
}