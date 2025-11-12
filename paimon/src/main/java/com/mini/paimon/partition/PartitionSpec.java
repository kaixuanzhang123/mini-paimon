package com.mini.paimon.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

/**
 * 分区规范
 * 参考 Paimon PartitionSpec 设计
 * 表示一个具体的分区值
 */
public class PartitionSpec {
    
    private final Map<String, String> partitionValues;
    
    @JsonCreator
    public PartitionSpec(@JsonProperty("partitionValues") Map<String, String> partitionValues) {
        this.partitionValues = new LinkedHashMap<>(Objects.requireNonNull(partitionValues));
    }
    
    public static PartitionSpec of(Map<String, String> partitionValues) {
        return new PartitionSpec(partitionValues);
    }
    
    public static PartitionSpec of(String key, String value) {
        Map<String, String> map = new LinkedHashMap<>();
        map.put(key, value);
        return new PartitionSpec(map);
    }
    
    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }
    
    public String get(String key) {
        return partitionValues.get(key);
    }
    
    public boolean isEmpty() {
        return partitionValues.isEmpty();
    }
    
    /**
     * 转换为分区路径字符串
     * 例如: dt=2024-01-01/hour=10
     */
    public String toPath() {
        if (partitionValues.isEmpty()) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            if (!first) {
                sb.append("/");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
            first = false;
        }
        return sb.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionSpec that = (PartitionSpec) o;
        return partitionValues.equals(that.partitionValues);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(partitionValues);
    }
    
    @Override
    public String toString() {
        return toPath();
    }
}
