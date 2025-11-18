package com.mini.paimon.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Column Statistics
 * 参考 Apache Paimon 的列统计信息设计
 * 用于查询优化和谓词下推
 * 
 * 统计信息包括:
 * 1. 最小值/最大值 (用于范围过滤)
 * 2. 空值数量 (用于 IS NULL 优化)
 * 3. 不同值数量 (用于基数估算)
 * 4. 总行数
 */
public class ColumnStatistics {
    
    private final String columnName;
    private final DataType dataType;
    
    /** 最小值 */
    private final Object minValue;
    
    /** 最大值 */
    private final Object maxValue;
    
    /** 空值数量 */
    private final long nullCount;
    
    /** 不同值数量(基数) */
    private final long distinctCount;
    
    /** 总行数 */
    private final long rowCount;
    
    @JsonCreator
    public ColumnStatistics(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("dataType") DataType dataType,
            @JsonProperty("minValue") Object minValue,
            @JsonProperty("maxValue") Object maxValue,
            @JsonProperty("nullCount") long nullCount,
            @JsonProperty("distinctCount") long distinctCount,
            @JsonProperty("rowCount") long rowCount) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.nullCount = nullCount;
        this.distinctCount = distinctCount;
        this.rowCount = rowCount;
    }
    
    public String getColumnName() {
        return columnName;
    }
    
    public DataType getDataType() {
        return dataType;
    }
    
    public Object getMinValue() {
        return minValue;
    }
    
    public Object getMaxValue() {
        return maxValue;
    }
    
    public long getNullCount() {
        return nullCount;
    }
    
    public long getDistinctCount() {
        return distinctCount;
    }
    
    public long getRowCount() {
        return rowCount;
    }
    
    /**
     * 检查值是否可能在范围内
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean mightContainValue(Object value) {
        if (value == null) {
            return nullCount > 0;
        }
        
        if (minValue == null || maxValue == null) {
            return true; // 没有统计信息,保守返回 true
        }
        
        if (!(value instanceof Comparable)) {
            return true;
        }
        
        try {
            Comparable comparableValue = (Comparable) value;
            Comparable comparableMin = (Comparable) minValue;
            Comparable comparableMax = (Comparable) maxValue;
            
            return comparableValue.compareTo(comparableMin) >= 0 &&
                   comparableValue.compareTo(comparableMax) <= 0;
        } catch (ClassCastException e) {
            return true; // 类型不匹配,保守返回 true
        }
    }
    
    /**
     * 检查范围是否可能重叠
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean mightOverlapRange(Object rangeMin, Object rangeMax) {
        if (minValue == null || maxValue == null) {
            return true;
        }
        
        if (rangeMin == null && rangeMax == null) {
            return true;
        }
        
        try {
            if (rangeMin != null && maxValue instanceof Comparable) {
                Comparable comparableRangeMin = (Comparable) rangeMin;
                Comparable comparableMax = (Comparable) maxValue;
                if (comparableRangeMin.compareTo(comparableMax) > 0) {
                    return false; // rangeMin > maxValue
                }
            }
            
            if (rangeMax != null && minValue instanceof Comparable) {
                Comparable comparableRangeMax = (Comparable) rangeMax;
                Comparable comparableMin = (Comparable) minValue;
                if (comparableRangeMax.compareTo(comparableMin) < 0) {
                    return false; // rangeMax < minValue
                }
            }
            
            return true;
        } catch (ClassCastException e) {
            return true;
        }
    }
    
    /**
     * 合并两个统计信息
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ColumnStatistics merge(ColumnStatistics other) {
        if (!this.columnName.equals(other.columnName)) {
            throw new IllegalArgumentException("Cannot merge statistics for different columns");
        }
        
        Object newMin = this.minValue;
        Object newMax = this.maxValue;
        
        if (this.minValue != null && other.minValue != null && 
            this.minValue instanceof Comparable && other.minValue instanceof Comparable) {
            Comparable thisMin = (Comparable) this.minValue;
            Comparable otherMin = (Comparable) other.minValue;
            newMin = thisMin.compareTo(otherMin) < 0 ? this.minValue : other.minValue;
        } else if (other.minValue != null) {
            newMin = other.minValue;
        }
        
        if (this.maxValue != null && other.maxValue != null &&
            this.maxValue instanceof Comparable && other.maxValue instanceof Comparable) {
            Comparable thisMax = (Comparable) this.maxValue;
            Comparable otherMax = (Comparable) other.maxValue;
            newMax = thisMax.compareTo(otherMax) > 0 ? this.maxValue : other.maxValue;
        } else if (other.maxValue != null) {
            newMax = other.maxValue;
        }
        
        return new ColumnStatistics(
            columnName,
            dataType,
            newMin,
            newMax,
            this.nullCount + other.nullCount,
            Math.max(this.distinctCount, other.distinctCount), // 近似值
            this.rowCount + other.rowCount
        );
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnStatistics)) return false;
        ColumnStatistics that = (ColumnStatistics) o;
        return nullCount == that.nullCount &&
               distinctCount == that.distinctCount &&
               rowCount == that.rowCount &&
               columnName.equals(that.columnName) &&
               Objects.equals(minValue, that.minValue) &&
               Objects.equals(maxValue, that.maxValue);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(columnName, minValue, maxValue, nullCount, distinctCount, rowCount);
    }
    
    @Override
    public String toString() {
        return String.format(
            "ColumnStatistics{column=%s, min=%s, max=%s, nulls=%d, distinct=%d, rows=%d}",
            columnName, minValue, maxValue, nullCount, distinctCount, rowCount
        );
    }
    
    /**
     * 构建器
     */
    public static class Builder {
        private final String columnName;
        private final DataType dataType;
        private Object minValue;
        private Object maxValue;
        private long nullCount = 0;
        private long distinctCount = 0;
        private long rowCount = 0;
        
        public Builder(String columnName, DataType dataType) {
            this.columnName = columnName;
            this.dataType = dataType;
        }
        
        public Builder minValue(Object minValue) {
            this.minValue = minValue;
            return this;
        }
        
        public Builder maxValue(Object maxValue) {
            this.maxValue = maxValue;
            return this;
        }
        
        public Builder nullCount(long nullCount) {
            this.nullCount = nullCount;
            return this;
        }
        
        public Builder distinctCount(long distinctCount) {
            this.distinctCount = distinctCount;
            return this;
        }
        
        public Builder rowCount(long rowCount) {
            this.rowCount = rowCount;
            return this;
        }
        
        public ColumnStatistics build() {
            return new ColumnStatistics(
                columnName, dataType, minValue, maxValue,
                nullCount, distinctCount, rowCount
            );
        }
    }
}


