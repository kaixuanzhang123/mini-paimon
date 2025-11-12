package com.mini.paimon.filter;

import com.mini.paimon.partition.PartitionSpec;

import java.util.*;

/**
 * 分区谓词
 * 支持复杂的分区表达式解析和过滤
 * 参考 Paimon PartitionPredicate 实现
 */
public abstract class PartitionPredicate {
    
    /**
     * 测试分区是否满足条件
     */
    public abstract boolean test(PartitionSpec partition);
    
    /**
     * AND 组合
     */
    public PartitionPredicate and(PartitionPredicate other) {
        return new AndPartitionPredicate(this, other);
    }
    
    /**
     * OR 组合
     */
    public PartitionPredicate or(PartitionPredicate other) {
        return new OrPartitionPredicate(this, other);
    }
    
    /**
     * NOT 条件
     */
    public PartitionPredicate not() {
        return new NotPartitionPredicate(this);
    }
    
    /**
     * 单个分区字段的等值谓词
     */
    public static class EqualPredicate extends PartitionPredicate {
        private final String partitionKey;
        private final String value;
        
        public EqualPredicate(String partitionKey, String value) {
            this.partitionKey = partitionKey;
            this.value = value;
        }
        
        @Override
        public boolean test(PartitionSpec partition) {
            String partitionValue = partition.get(partitionKey);
            return value.equals(partitionValue);
        }
        
        @Override
        public String toString() {
            return partitionKey + "='" + value + "'";
        }
    }
    
    /**
     * IN 谓词 - 分区值在给定集合中
     */
    public static class InPredicate extends PartitionPredicate {
        private final String partitionKey;
        private final Set<String> values;
        
        public InPredicate(String partitionKey, Collection<String> values) {
            this.partitionKey = partitionKey;
            this.values = new HashSet<>(values);
        }
        
        @Override
        public boolean test(PartitionSpec partition) {
            String partitionValue = partition.get(partitionKey);
            return values.contains(partitionValue);
        }
        
        @Override
        public String toString() {
            return partitionKey + " IN (" + String.join(",", values) + ")";
        }
    }
    
    /**
     * 范围谓词 - 支持字符串范围比较
     */
    public static class RangePredicate extends PartitionPredicate {
        private final String partitionKey;
        private final String lowerBound;  // 包含
        private final String upperBound;  // 包含
        private final boolean lowerInclusive;
        private final boolean upperInclusive;
        
        public RangePredicate(String partitionKey, String lowerBound, String upperBound,
                            boolean lowerInclusive, boolean upperInclusive) {
            this.partitionKey = partitionKey;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.lowerInclusive = lowerInclusive;
            this.upperInclusive = upperInclusive;
        }
        
        @Override
        public boolean test(PartitionSpec partition) {
            String value = partition.get(partitionKey);
            if (value == null) {
                return false;
            }
            
            if (lowerBound != null) {
                int cmp = value.compareTo(lowerBound);
                if (lowerInclusive ? cmp < 0 : cmp <= 0) {
                    return false;
                }
            }
            
            if (upperBound != null) {
                int cmp = value.compareTo(upperBound);
                if (upperInclusive ? cmp > 0 : cmp >= 0) {
                    return false;
                }
            }
            
            return true;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (lowerBound != null) {
                sb.append(partitionKey).append(lowerInclusive ? " >= " : " > ").append("'").append(lowerBound).append("'");
            }
            if (upperBound != null) {
                if (lowerBound != null) sb.append(" AND ");
                sb.append(partitionKey).append(upperInclusive ? " <= " : " < ").append("'").append(upperBound).append("'");
            }
            return sb.toString();
        }
    }
    
    /**
     * 前缀匹配谓词 - 支持 LIKE 'prefix%'
     */
    public static class PrefixPredicate extends PartitionPredicate {
        private final String partitionKey;
        private final String prefix;
        
        public PrefixPredicate(String partitionKey, String prefix) {
            this.partitionKey = partitionKey;
            this.prefix = prefix;
        }
        
        @Override
        public boolean test(PartitionSpec partition) {
            String value = partition.get(partitionKey);
            return value != null && value.startsWith(prefix);
        }
        
        @Override
        public String toString() {
            return partitionKey + " LIKE '" + prefix + "%'";
        }
    }
    
    /**
     * AND 谓词
     */
    public static class AndPartitionPredicate extends PartitionPredicate {
        private final PartitionPredicate left;
        private final PartitionPredicate right;
        
        public AndPartitionPredicate(PartitionPredicate left, PartitionPredicate right) {
            this.left = left;
            this.right = right;
        }
        
        @Override
        public boolean test(PartitionSpec partition) {
            return left.test(partition) && right.test(partition);
        }
        
        @Override
        public String toString() {
            return "(" + left + " AND " + right + ")";
        }
    }
    
    /**
     * OR 谓词
     */
    public static class OrPartitionPredicate extends PartitionPredicate {
        private final PartitionPredicate left;
        private final PartitionPredicate right;
        
        public OrPartitionPredicate(PartitionPredicate left, PartitionPredicate right) {
            this.left = left;
            this.right = right;
        }
        
        @Override
        public boolean test(PartitionSpec partition) {
            return left.test(partition) || right.test(partition);
        }
        
        @Override
        public String toString() {
            return "(" + left + " OR " + right + ")";
        }
    }
    
    /**
     * NOT 谓词
     */
    public static class NotPartitionPredicate extends PartitionPredicate {
        private final PartitionPredicate predicate;
        
        public NotPartitionPredicate(PartitionPredicate predicate) {
            this.predicate = predicate;
        }
        
        @Override
        public boolean test(PartitionSpec partition) {
            return !predicate.test(partition);
        }
        
        @Override
        public String toString() {
            return "NOT " + predicate;
        }
    }
    
    /**
     * 始终为真的谓词
     */
    public static class AlwaysTruePredicate extends PartitionPredicate {
        @Override
        public boolean test(PartitionSpec partition) {
            return true;
        }
        
        @Override
        public String toString() {
            return "TRUE";
        }
    }
    
    /**
     * 始终为假的谓词
     */
    public static class AlwaysFalsePredicate extends PartitionPredicate {
        @Override
        public boolean test(PartitionSpec partition) {
            return false;
        }
        
        @Override
        public String toString() {
            return "FALSE";
        }
    }
    
    // 静态工厂方法
    
    public static PartitionPredicate equal(String key, String value) {
        return new EqualPredicate(key, value);
    }
    
    public static PartitionPredicate in(String key, String... values) {
        return new InPredicate(key, Arrays.asList(values));
    }
    
    public static PartitionPredicate in(String key, Collection<String> values) {
        return new InPredicate(key, values);
    }
    
    public static PartitionPredicate greaterThan(String key, String value) {
        return new RangePredicate(key, value, null, false, false);
    }
    
    public static PartitionPredicate greaterThanOrEqual(String key, String value) {
        return new RangePredicate(key, value, null, true, false);
    }
    
    public static PartitionPredicate lessThan(String key, String value) {
        return new RangePredicate(key, null, value, false, false);
    }
    
    public static PartitionPredicate lessThanOrEqual(String key, String value) {
        return new RangePredicate(key, null, value, false, true);
    }
    
    public static PartitionPredicate between(String key, String lower, String upper) {
        return new RangePredicate(key, lower, upper, true, true);
    }
    
    public static PartitionPredicate prefix(String key, String prefix) {
        return new PrefixPredicate(key, prefix);
    }
    
    public static PartitionPredicate alwaysTrue() {
        return new AlwaysTruePredicate();
    }
    
    public static PartitionPredicate alwaysFalse() {
        return new AlwaysFalsePredicate();
    }
}
